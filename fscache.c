/*
 * BackFS Filesystem Cache
 * Copyright (c) 2010-2011 William R. Fraser
 */

#include "fscache.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <dirent.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>

#include <pthread.h>
static pthread_mutex_t lock;

#include "fsll.h"

char *cache_dir;
uint64_t cache_size;
uint64_t cache_used_size;
bool use_whole_device;
uint64_t bucket_max_size;

uint64_t get_cache_used_size(const char *root)
{
    fprintf(stderr, "BackFS CACHE: taking inventory of cache directory\n");
    uint64_t total = 0;
    struct dirent *e = malloc(offsetof(struct dirent, d_name) + PATH_MAX + 1);
    struct dirent *result = e;
    struct stat s;
    char buf[PATH_MAX];
    DIR *dir = opendir(root);
    while (readdir_r(dir, e, &result) == 0 && result != NULL) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        snprintf(buf, PATH_MAX, "%s/%s/data", root, e->d_name);
        s.st_size = 0;
        if (stat(buf, &s) == -1 && errno != ENOENT) {
            perror("BackFS CACHE ERROR: stat in get_cache_used_size");
            fprintf(stderr, "\tcaused by stat(%s)\n", buf);
            abort();
        }
        fprintf(stderr, "BackFS CACHE: bucket %s: %llu bytes\n",
                e->d_name, (unsigned long long) s.st_size);
        total += s.st_size;
    }
    if (result != NULL) {
        perror("BackFS CACHE ERROR: readdir in get_cache_used_size");
        abort();
    }

    closedir(dir);
    free(e);

    fprintf(stderr, "BackFS CACHE: %llu bytes used initially\n",
            (unsigned long long) total);

    return total;
}

/*
 * Initialize the cache.
 */
void cache_init(const char *a_cache_dir, uint64_t a_cache_size, uint64_t a_bucket_max_size)
{
    cache_dir = (char*)malloc(strlen(a_cache_dir)+1);
    strcpy(cache_dir, a_cache_dir);
    cache_size = a_cache_size;
    use_whole_device = (cache_size == 0);

    char bucket_dir[PATH_MAX];
    snprintf(bucket_dir, PATH_MAX, "%s/buckets", cache_dir);
    cache_used_size = get_cache_used_size(bucket_dir);

    bucket_max_size = a_bucket_max_size;
}

const char * bucketname(const char *path)
{
    return fsll_basename(path);
}

void dump_queues()
{
    fprintf(stderr, "BackFS Used Bucket Queue:\n");
    fsll_dump(cache_dir, "buckets/head", "buckets/tail");
    fprintf(stderr, "BackFS Free Bucket Queue:\n");
    fsll_dump(cache_dir, "buckets/free_head", "buckets/free_tail");
}

/*
 * don't use this function directly.
 */
char * makebucket(uint64_t number)
{
    char *new_bucket = fsll_make_entry(cache_dir, "buckets", number);
    fsll_insert_as_head(cache_dir, new_bucket,
            "buckets/head", "buckets/tail");
    return new_bucket;
}

/*
 * make a new bucket
 *
 * either re-use one from the free queue,
 *   or increment the next_bucket_number file and return that.
 *
 * If one from the free queue is returned, that bucket is made the head of the
 * used queue.
 */
char * next_bucket()
{
    char *bucket = fsll_getlink(cache_dir, "buckets/free_head");
    if (bucket != NULL) {
        fprintf(stderr, "BackFS CACHE: re-using free bucket %s\n",
                bucketname(bucket));

        // disconnect from free queue
        fsll_disconnect(cache_dir, bucket,
                "buckets/free_head", "buckets/free_tail");

        // make head of the used queue
        fsll_insert_as_head(cache_dir, bucket,
                "buckets/head", "buckets/tail");

        return bucket;
    } else {
        char nbnpath[PATH_MAX];
        snprintf(nbnpath, PATH_MAX, "%s/buckets/next_bucket_number", cache_dir);
        uint64_t next = 0;
        
        FILE *f = fopen(nbnpath, "r+");
        if (f == NULL && errno != ENOENT) {
            perror("BackFS CACHE ERROR: open next_bucket");
            return makebucket(0);
        } else {
            if (f != NULL) {
                // we had a number already there; read it
                if (fscanf(f, "%llu", (unsigned long long *)&next) != 1) {
                    fprintf(stderr, "BackFS CACHE: ERROR: unable to read next_bucket\n");
                    fclose(f);
                    return makebucket(0);
                }
                f = freopen(nbnpath, "w+", f);
            } else {
                // next_bucket_number doesn't exist; create it and write a 1.
                f = fopen(nbnpath, "w+");
                if (f == NULL) {
                    perror("BackFS CACHE ERROR: open next_bucket again");
                    return makebucket(0);
                }
            }
            // write the next number
            if (f == NULL) {
                perror("BackFS CACHE ERROR: fdopen for writing in next_bucket");
                return makebucket(0);
            }
            fprintf(f, "%llu\n", (unsigned long long) next+1);
            fclose(f);
        }

        fprintf(stderr, "BackFS CACHE: making new bucket %lu\n",
                (unsigned long) next);

        char *new_bucket = makebucket(next);

        return new_bucket;
    }
}

/*
 * moves a bucket to the head of the used queue
 */
void bucket_to_head(const char *bucketpath)
{
    fprintf(stderr, "BackFS CACHE: bucket_to_head(%s)\n", bucketpath);
    return fsll_to_head(cache_dir, bucketpath, "buckets/head", "buckets/tail");
}

/*
 * returns the bucket number corresponding to a bucket path
 * i.e. reads the number off the end.
 */
uint32_t bucket_path_to_number(const char *bucketpath)
{
    uint32_t number = 0;
    size_t s = strlen(bucketpath);
    size_t i;
    for (i = 1; i < s; i++) {
        char c = bucketpath[s - i];
        if (c < '0' || c > '9') {
            i--;
            break;
        }
    }
    for (i = s - i; i < s; i++) {
        number *= 10;
        number += (bucketpath[i] - '0');
    }
    return number;
}

/*
 * free a bucket
 *
 * moves bucket from the tail of the used queue to the tail of the free queue,
 * deletes the data in the bucket
 * returns the size of the data deleted
 */
uint64_t free_bucket(const char *bucketpath)
{
    char *n = fsll_getlink(bucketpath, "next");
    if (n != NULL) {
        fprintf(stderr, "BackFS CACHE ERROR, bucket freed (#%lu) was not the queue tail\n",
                (unsigned long) bucket_path_to_number(bucketpath));
        return 0;
    }

    fsll_disconnect(cache_dir, bucketpath, 
            "buckets/head", "buckets/tail");

    fsll_insert_as_tail(cache_dir, bucketpath,
            "buckets/free_head", "buckets/free_tail");

    char data[PATH_MAX];
    snprintf(data, PATH_MAX, "%s/data", bucketpath);
    
    struct stat s;
    if (stat(data, &s) == -1) {
        perror("BackFS CACHE ERROR: stat in free_bucket");
    }

    cache_used_size -= (uint64_t) s.st_size;

    if (unlink(data) == -1) {
        perror("BackFS CACHE ERROR: unlink in free_bucket");
        return 0;
    } else {
        cache_used_size -= (uint64_t) s.st_size;
        return (uint64_t) s.st_size;
    }
}

/*
 * do not use this function directly
 */
void cache_invalidate_bucket(const char *filename, uint32_t block, 
                                const char *bucket)
{
    fprintf(stderr, "BackFS CACHE: invalidating block %lu of file %s\n",
            (unsigned long) block, filename);

    uint64_t freed_size = free_bucket(bucket);

    fprintf(stderr, "BackFS CACHE: freed %llu bytes in bucket %s\n",
            (unsigned long long) freed_size,
            bucketname(bucket));
}

void cache_invalidate_file(const char *filename)
{
    pthread_mutex_lock(&lock);

    char mappath[PATH_MAX];
    snprintf(mappath, PATH_MAX, "%s/map%s", cache_dir, filename);
    DIR *d = opendir(mappath);
    if (d == NULL) {
        perror("BackFS CACHE ERROR: opendir in cache_invalidate");
        return;
    }

    struct dirent *e = malloc(offsetof(struct dirent, d_name) + PATH_MAX + 1);
    struct dirent *result = e;
    while (readdir_r(d, e, &result) == 0 && result != NULL) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;

        char *bucket = fsll_getlink(mappath, e->d_name);
        uint32_t block;
        sscanf(e->d_name, "%lu", (unsigned long *)&block);
    
        cache_invalidate_bucket(filename, block, bucket);
    }

    pthread_mutex_unlock(&lock);
}

void cache_invalidate_block(const char *filename, uint32_t block)
{
    char mappath[PATH_MAX];
    snprintf(mappath, PATH_MAX, "map%s/%lu",
            filename, (unsigned long) block);

    pthread_mutex_lock(&lock);
    
    char *bucket = fsll_getlink(cache_dir, mappath);
    if (bucket == NULL) {
        fprintf(stderr, "BackFS Warning: Cache invalidation: block %lu of file %s doesn't exist.\n",
                (unsigned long) block, filename);
        return;
    }

    cache_invalidate_bucket(filename, block, bucket);

    pthread_mutex_unlock(&lock);
}

/*
 * Read a block from the cache.
 * Important: you can specify less than one block, but not more.
 * Nor can a read be across block boundaries.
 *
 * Returns 0 on success.
 * On error returns -1 and sets errno.
 * In particular, if the block is not in the cache, sets ENOENT
 */
int cache_fetch(const char *filename, uint32_t block, uint64_t offset, 
        char *buf, uint64_t len, uint64_t *bytes_read)
{
    if (offset + len > BUCKET_MAX_SIZE || filename == NULL) {
        errno = EINVAL;
        return -1;
    }

    if (len == 0) {
        *bytes_read = 0;
        return 0;
    }

    fprintf(stderr, "BackFS CACHE: getting block %lu of file %s\n", 
            (unsigned long) block, filename);

    //###
    pthread_mutex_lock(&lock);

    char mapfile[PATH_MAX];
    snprintf(mapfile, PATH_MAX, "%s/map/%s/%lu",
            cache_dir, filename, (unsigned long) block);
    char bucketpath[PATH_MAX];
    ssize_t bplen;
    if ((bplen = readlink(mapfile, bucketpath, PATH_MAX-1)) == -1) {
        if (errno == ENOENT || errno == ENOTDIR) {
            fprintf(stderr, "BackFS CACHE: block not in cache\n");
            errno = ENOENT;
            pthread_mutex_unlock(&lock);
            return -1;
        } else {
            perror("BackFS CACHE ERROR: readlink error");
            errno = EIO;
            pthread_mutex_unlock(&lock);
            return -1;
        }
    }
    bucketpath[bplen] = '\0';

    bucket_to_head(bucketpath);

    // [cache_dir]/buckets/%010lu/data
    char bucketdata[PATH_MAX];
    snprintf(bucketdata, PATH_MAX, "%s/data", bucketpath);

    uint64_t size = 0;
    struct stat stbuf;
    if (stat(bucketdata, &stbuf) == -1) {
        perror("BackFS CACHE ERROR: stat on bucket error");
        errno = EIO;
        pthread_mutex_unlock(&lock);
        return -1;
    }
    size = (uint64_t) stbuf.st_size;

    if (size < offset) {
        fprintf(stderr, "BackFS CACHE: offset for read is past the end\n");
        pthread_mutex_unlock(&lock);
        *bytes_read = 0;
        return 0;
    }

    /*
    if (e->bucket->size - offset < len) {
        fprintf(stderr, "BackFS CACHE: length + offset for read is past the end\n");
        errno = ENXIO;
        free(f);
        pthread_mutex_unlock(&lock);
        return -1;
    }
    */

    int fd = open(bucketdata, O_RDONLY);
    if (fd == -1) {
        perror("BackFS Cache ERROR: error opening file from cache dir");
        errno = EBADF;
        pthread_mutex_unlock(&lock);
        return -1;
    }

    *bytes_read = pread(fd, buf, len, offset);
    if (*bytes_read == -1) {
        perror("BackFS Cache ERROR: error reading file from cache dir");
        errno = EIO;
        close(fd);
        pthread_mutex_unlock(&lock);
        return -1;
    }

    if (*bytes_read != len) {
        fprintf(stderr, "BackFS CACHE: read fewer than requested bytes from cache file: %llu instead of %llu\n", 
                (unsigned long long) *bytes_read,
                (unsigned long long) len
        );
        /*
        errno = EIO;
        free(f);
        free(cachefile);
        close(fd);
        pthread_mutex_unlock(&lock);
        return -1;
        */
    }

    close(fd);

    pthread_mutex_unlock(&lock);
    //###

    return 0;
}

void make_space_available(uint64_t bytes_needed)
{
    uint64_t bytes_freed = 0;

    if (bytes_needed == 0)
        return;

    if (cache_used_size + bytes_needed <= cache_size)
        return;

    fprintf(stderr, "BackFS CACHE: need to free %llu bytes\n",
            (unsigned long long) bytes_needed);

    while (bytes_freed < bytes_needed) {
        char *b = fsll_getlink(cache_dir, "buckets/tail");
        
        if (b == NULL) {
            fprintf(stderr, "BackFS CACHE: ERROR: bucket queue empty in make_space_available!\n");
            return;
        }

        uint64_t f = free_bucket(b);
        fprintf(stderr, "BackFS CACHE: freed %llu bytes in bucket #%lu\n",
                (unsigned long long) f, (unsigned long) bucket_path_to_number(b));
        free(b);
        bytes_freed += f;
    }

    fprintf(stderr, "BackFS CACHE: freed %llu bytes total\n",
            (unsigned long long) bytes_freed);
}

/*
 * Adds a data block to the cache.
 * Important: this must be the FULL block. All subsequent reads will
 * assume that the full block is here.
 */
int cache_add(const char *filename, uint32_t block, char *buf, uint64_t len)
{
    if (len > bucket_max_size) {
        errno = EOVERFLOW;
        return -1;
    }

    if (len == 0) {
        return 0;
    }

    char fileandblock[PATH_MAX];
    snprintf(fileandblock, PATH_MAX, "map%s/%lu", filename, (unsigned long) block);

    fprintf(stderr, "BackFS CACHE: writing %llu bytes to %s\n",
            (unsigned long long) len, fileandblock);

    //###
    pthread_mutex_lock(&lock);

    char *bucketpath = fsll_getlink(cache_dir, fileandblock);

    if (bucketpath != NULL) {
        if (fsll_file_exists(bucketpath, "data")) {
            fprintf(stderr, "BackFS CACHE: warning: data already exists in cache\n");
            free(bucketpath);
            pthread_mutex_unlock(&lock);
            return 0;
        }
    }

    make_space_available(len);

    bucketpath = next_bucket();

    /*
    char *head = getlink(cache_dir, "buckets/head");
    if (head == NULL) {
        makelink(cache_dir, "buckets/head", bucketpath);
        makelink(cache_dir, "buckets/tail", bucketpath);
    } else {
        makelink(head, "prev", bucketpath);
        makelink(bucketpath, "next", head);
        makelink(cache_dir, "buckets/head", bucketpath);
    }
    */

    char *filemap = (char*)malloc(strlen(filename) + 4);
    snprintf(filemap, strlen(filename)+4, "map%s", filename);

    char *full_filemap_dir = (char*)malloc(strlen(cache_dir) + 5 + strlen(filename) + 1);
    snprintf(full_filemap_dir, strlen(cache_dir)+5+strlen(filename)+1, "%s/map%s/",
            cache_dir, filename);

    fprintf(stderr, "BackFS CACHE: map file = %s\n", filemap);
    fprintf(stderr, "BackFS CACHE: full filemap dir = %s\n", full_filemap_dir);
    fprintf(stderr, "BackFS CACHE: bucket path = %s\n", bucketpath);

    if (!fsll_file_exists(cache_dir, filemap)) {
        free(filemap);
        size_t i;
        // start from "$cache_dir/map/"
        for (i = strlen(cache_dir) + 5; i < strlen(full_filemap_dir); i++) {
            if (full_filemap_dir[i] == '/') {
                char *component = (char*)malloc(i+1);
                strncpy(component, full_filemap_dir, i+1);
                component[i] = '\0';
                fprintf(stderr, "BackFS CACHE: making %s\n", component);
                if(mkdir(component, 0700) == -1 && errno != EEXIST) {
                    perror("BackFS CACHE ERROR: mkdir in cache_add");
                    fprintf(stderr, "\tcaused by mkdir(%s)\n", component);
                    errno = EIO;
                    free(component);
                    pthread_mutex_unlock(&lock);
                    return -1;
                }
                free(component);
            }
        }
    } else {
        free(filemap);
    }
    free(full_filemap_dir);

    fsll_makelink(cache_dir, fileandblock, bucketpath);

    char *fullfilemap = (char*)malloc(PATH_MAX);
    snprintf(fullfilemap, PATH_MAX, "%s/%s", cache_dir, fileandblock);
    fsll_makelink(bucketpath, "parent", fullfilemap);
    free(fullfilemap);

    // finally, write

    char datapath[PATH_MAX];
    snprintf(datapath, PATH_MAX, "%s/data", bucketpath);

    int fd = open(datapath, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        perror("BackFS CACHE ERROR: open in cache_add");
        fprintf(stderr, "\tcaused by open(%s, O_WRONLY|O_CREAT)\n", datapath);
        errno = EIO;
        pthread_mutex_unlock(&lock);
        return -1;
    }

    ssize_t bytes_written = write(fd, buf, len);
    if (bytes_written == -1) {
        perror("BackFS CACHE ERROR: write in cache_add");
        errno = EIO;
        close(fd);
        pthread_mutex_unlock(&lock);
        return -1;
    }

    fprintf(stderr, "BackFS CACHE: %llu bytes written to cache\n",
            (unsigned long long) bytes_written);

    if (bytes_written != len) {
        fprintf(stderr, "BackFS CACHE: not all bytes written to cache!\n");
        errno = EIO;
        close(fd);
        pthread_mutex_unlock(&lock);
        return -1;
    }

    cache_used_size += bytes_written;
    fprintf(stderr, "BackFS CACHE: size now %llu bytes of %llu bytes (%lf%%)\n",
            (unsigned long long) cache_used_size,
            (unsigned long long) cache_size,
            (double)100 * cache_used_size / cache_size
    );

    dump_queues();

    close(fd);

    pthread_mutex_unlock(&lock);
    //###

    return 0;
}
