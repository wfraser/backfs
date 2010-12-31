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

#include <pthread.h>
static pthread_mutex_t lock;

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

char * getlink(const char *base, const char *file)
{
    char path[PATH_MAX];
    char *result = (char*)malloc(PATH_MAX);
    snprintf(path, PATH_MAX, "%s/%s", base, file);
    ssize_t len;
    if ((len = readlink(path, result, PATH_MAX-1)) == -1) {
        if (errno == ENOENT || errno == ENOTDIR) {
            free(result);
            return NULL;
        } else {
            perror("BackFS CACHE ERROR: readlink in getlink");
            free(result);
            return NULL;
        }
    } else {
        result[len] = '\0';
        return result;
    }
}

void makelink(const char *base, const char *file, const char *dest)
{
    char source[PATH_MAX];
    snprintf(source, PATH_MAX, "%s/%s", base, file);

    if (unlink(source) == -1) {
        if (errno != ENOENT && errno != ENOTDIR) {
            perror("BackFS CACHE ERROR: unlink in makelink");
            fprintf(stderr, "\tcaused by unlink(%s)\n", source);
            return;
        }
    }

    if (dest != NULL) {
        if (symlink(dest, source) == -1) {
            perror("BackFS CACHE ERROR: symlink in makelink");
            fprintf(stderr, "\tcaused by symlink(%s,%s)\n", dest, source);
        }
    }
}

void makelinkno(const char *base, const char *file, uint64_t number)
{
    char dest[PATH_MAX];

    snprintf(dest, PATH_MAX, "%s/buckets/%010lu",
            cache_dir, (unsigned long) number);

    makelink(base, file, dest);
}

bool file_exists(const char *base, const char *file)
{
    char path[PATH_MAX];
    snprintf(path, PATH_MAX, "%s/%s", base, file);

    if (access(path, F_OK) == -1) {
        return false;
    } else {
        return true;
    }
}

char bucketname_buf[PATH_MAX];
char * bucketname(const char *path)
{
    if (path == NULL) {
        strcpy(bucketname_buf, "NULL");
        return bucketname_buf;
    }

    char *copy = (char*)malloc(strlen(path)+1);
    strncpy(copy, path, strlen(path)+1);

    strncpy(bucketname_buf, basename(copy), PATH_MAX);

    free(copy);

    return bucketname_buf;
}

/*
 * don't use this function directly.
 */
char * makebucket(uint64_t number)
{
    char *bucketpath = (char*)malloc(PATH_MAX);
    snprintf(bucketpath, PATH_MAX, "%s/buckets/%010llu",
            cache_dir, (unsigned long long) number);

    if (mkdir(bucketpath, 0700) == -1) {
        perror("BackFS CACHE ERROR: mkdir in makebucket");
        return NULL;
    }

    return bucketpath;
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
    char *bucket = getlink(cache_dir, "buckets/free_head");
    if (bucket != NULL) {
        fprintf(stderr, "BackFS CACHE: re-using free bucket %s\n",
                bucketname(bucket));

        // disconnect from free queue
        char *next = getlink(bucket, "next");
        if (next != NULL) {
            makelink(cache_dir, "buckets/free_head", next);
            makelink(next, "prev", NULL);
            free(next);
        } else {
            makelink(cache_dir, "buckets/free_head", NULL);
            makelink(cache_dir, "buckets/free_tail", NULL);
        }

        // make head of the used queue
        char *head = getlink(cache_dir, "buckets/head");
        if (head != NULL) {
            makelink(head, "prev", bucket);
            makelink(bucket, "next", head);
        } else {
            makelink(cache_dir, "buckets/tail", bucket);
        }

        makelink(cache_dir, "buckets/head", bucket);

        makelink(bucket, "prev", NULL);

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

        // make head of the used queue
        char *head = getlink(cache_dir, "buckets/head");
        if (head != NULL) {
            makelink(head, "prev", new_bucket);
            makelink(new_bucket, "next", head);
        } else {
            makelink(cache_dir, "buckets/tail", new_bucket);
        }

        makelink(cache_dir, "buckets/head", new_bucket);

        makelink(new_bucket, "prev", NULL);

        return new_bucket;
    }
}

/*
 * moves a bucket to the head of the used queue
 */
void bucket_to_head(const char *bucketpath)
{
    fprintf(stderr, "BackFS CACHE: bucket_to_head(%s)\n", bucketpath);

    char *p = getlink(bucketpath, "prev");
    fprintf(stderr, "\tp(%s)\n", p);
    
    if (p == NULL) {
        // bucket is already head; do nothing
        return;
    }

    char *n = getlink(bucketpath, "next");
    char *h = getlink(cache_dir, "buckets/head");

    fprintf(stderr, "\tn(%s)\n", n);
    fprintf(stderr, "\th(%s)\n", h);

    // prev.next = next
    makelink(p, "next", n);

    if (n) {
        // next.prev = prev
        makelink(n, "prev", p);
    } else {
        // we're the tail; tail needs changing

        // tail = prev
        makelink(cache_dir, "buckets/tail", p);

        // prev.next = null
        makelink(p, "next", NULL);
    }

    if (h) {
        // head.prev = this
        makelink(h, "prev", bucketpath);

        // next = head
        makelink(bucketpath, "next", h);
    }

    // head = this
    makelink(cache_dir, "buckets/head", bucketpath);

    if (!file_exists(cache_dir, "buckets/tail")) {
        makelink(cache_dir, "buckets/tail", bucketpath);
    }

    free(p);
    free(n);
    free(h);
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
    for (i = 0; i < s; i++) {
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
    char *n = getlink(bucketpath, "next");
    if (n != NULL) {
        fprintf(stderr, "BackFS CACHE: warning, bucket freed (#%lu) was not the queue tail\n",
                (unsigned long) bucket_path_to_number(bucketpath));

        char *p = getlink(bucketpath, "prev");
        if (p != NULL) {
            makelink(n, "prev", p);
            makelink(p, "next", n);
            free(p);
        } else {
            makelink(cache_dir, "buckets/head", n);
            makelink(n, "prev", NULL);
        }

        free(n);
    }
    
    makelink(bucketpath, "next", NULL);

    char *p = getlink(bucketpath, "prev");
    if (p != NULL) {
        makelink(p, "next", NULL);
        makelink(cache_dir, "buckets/tail", p);
    } else {
        fprintf(stderr, "BackFS CACHE: ERROR: free_bucket emptied the queue!\n");
        makelink(cache_dir, "buckets/tail", NULL);
        makelink(cache_dir, "buckets/head", NULL);
    }
    free(p);

    char *tail = getlink(cache_dir, "buckets/free_tail");
    if (tail != NULL) {
        makelink(bucketpath, "prev", tail);
        makelink(tail, "next", bucketpath);
        makelink(cache_dir, "buckets/free_tail", bucketpath);
    } else {
        // 1st free bucket
        makelink(cache_dir, "buckets/free_head", bucketpath);
        makelink(cache_dir, "buckets/free_tail", bucketpath);
    }
    free(tail);

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

void dump_queues()
{
    char *bucket = NULL;

    int i;
    for (i = 0; i < 2; i++) {
        if (i == 0) {
            bucket = getlink(cache_dir, "buckets/head");
            fprintf(stderr, "BackFS Used Bucket Queue:\n");
        } else {
            bucket = getlink(cache_dir, "buckets/free_head");
            fprintf(stderr, "BackFS Free Bucket Queue:\n");
        }

        if (bucket) {
            char *p, *n;
            do {
                p = getlink(bucket, "prev");
                n = getlink(bucket, "next");
                fprintf(stderr, "BackFS: %s <- ", bucketname(p));
                fprintf(stderr, "%s -> ", bucketname(bucket));
                fprintf(stderr, "%s\n", bucketname(n));
                if (bucket && n && strcmp(bucket, n) == 0) {
                    fprintf(stderr, "BackFS CACHE: ERROR: queue has a loop!\n");
                    break;
                }
                if (bucket) free(bucket);
                bucket = NULL;
                if (p) free(p);
            } while ((n == NULL) ? false : (bucket = n));

            char *tail;
            if (i == 0)
                tail = getlink(cache_dir, "buckets/tail");
            else
                tail = getlink(cache_dir, "buckets/free_tail");

            if (n && tail && strcmp(n, tail) != 0) {
                fprintf(stderr, "BackFS: queue doesn't end with the tail!\n"
                        "\ttail is %s\n", bucketname(tail));
            }
            if (bucket) free(bucket);
            if (tail) free(tail);
            if (n) free(n);
        }
    }
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
        fprintf(stderr, "BackFS CACHE: read fewer than requested bytes from cache file: %llu instead of %llu\n", *bytes_read, len);
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
        char *b = getlink(cache_dir, "buckets/tail");
        
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

    char *bucketpath = getlink(cache_dir, fileandblock);

    if (bucketpath != NULL) {
        if (file_exists(bucketpath, "data")) {
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

    if (!file_exists(cache_dir, filemap)) {
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

    makelink(cache_dir, fileandblock, bucketpath);

    char *fullfilemap = (char*)malloc(PATH_MAX);
    snprintf(fullfilemap, PATH_MAX, "%s/%s", cache_dir, fileandblock);
    makelink(bucketpath, "parent", fullfilemap);
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
