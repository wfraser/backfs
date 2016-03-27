/*
 * BackFS Filesystem Cache
 * Copyright (c) 2010-2014 William R. Fraser
 */

#define _XOPEN_SOURCE 500 // for pread()

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

#define BACKFS_LOG_SUBSYS "Cache"
#include "global.h"
#include "fsll.h"
#include "util.h"

extern int backfs_log_level;
extern bool backfs_log_stderr;

static char *cache_dir;
static uint64_t cache_size;
static volatile uint64_t cache_used_size = 0;
struct bucket_node { char path[PATH_MAX]; struct bucket_node* next; };
static struct bucket_node * volatile to_check;
static bool use_whole_device;
static uint64_t bucket_max_size;

uint64_t cache_number_of_buckets(const char *root)
{
    INFO("taking inventory of cache directory\n");
    uint64_t total = 0;
    struct dirent *e = malloc(offsetof(struct dirent, d_name) + PATH_MAX + 1);
    struct dirent *result = e;
    DIR *dir = opendir(root);
    struct bucket_node* volatile * next = &to_check;
    while (readdir_r(dir, e, &result) == 0 && result != NULL) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
        *next = (struct bucket_node*)malloc(sizeof(struct bucket_node));
        snprintf((*next)->path, PATH_MAX, "%s/%s/data", root, e->d_name);
        next = &((*next)->next);
        *next = NULL;
        ++total;
    }
    closedir(dir);
    FREE(e);

    return total;
}

bool is_unchecked(const char* path)
{
  struct bucket_node* node = to_check;
  while(node) {
    if (strcmp(node->path, path) == 0)
      return true;
    node = node->next;
  }
  return false;
}

void* check_buckets_size(void* arg)
{
  struct stat s;
  struct bucket_node* bucket;
  if (arg != NULL) {
    abort();
  }

  while (to_check) {
    pthread_mutex_lock(&lock);
    bucket = to_check;
    if (bucket) {
      s.st_size = 0;
      if (stat(bucket->path, &s) == -1 && errno != ENOENT) {
           PERROR("stat in get_cache_used_size");
           ERROR("\tcaused by stat(%s)\n", bucket->path);
           abort();
       }
       DEBUG("bucket %s: %llu bytes\n",
               bucket->path, (unsigned long long) s.st_size);
       cache_used_size -= bucket_max_size - s.st_size;
       to_check = bucket->next;
    }
    pthread_mutex_unlock(&lock);
    free(bucket);
  }
  return NULL;
}

uint64_t get_cache_fs_free_size(const char *root)
{
    struct statvfs s;
    if (statvfs(root, &s) == -1) {
        PERROR("statfs in get_cache_fs_free_size");
        return 0;
    }
    uint64_t dev_free = (uint64_t) s.f_bavail * s.f_bsize;

    return dev_free;
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
    uint64_t number_of_buckets = cache_number_of_buckets(bucket_dir);
    INFO("%llu buckets used in cache dir\n",
            (unsigned long long) number_of_buckets);
    cache_used_size = number_of_buckets * a_bucket_max_size;
    INFO("Estimated %llu bytes used in cache dir\n",
            (unsigned long long) cache_used_size);
    uint64_t cache_free_size = get_cache_fs_free_size(bucket_dir);
    INFO("%llu bytes free in cache dir\n",
            (unsigned long long) cache_free_size);

    bucket_max_size = a_bucket_max_size;

    pthread_t thread;
    if (pthread_create(&thread, NULL, &check_buckets_size, NULL) != 0) {
      PERROR("cache_init: error creating checked thread");
      abort();
    }
}

const char * bucketname(const char *path)
{
    return fsll_basename(path);
}

void dump_queues()
{
#ifdef FSLL_DUMP
    fprintf(stderr, "BackFS Used Bucket Queue:\n");
    fsll_dump(cache_dir, "buckets/head", "buckets/tail");
    fprintf(stderr, "BackFS Free Bucket Queue:\n");
    fsll_dump(cache_dir, "buckets/free_head", "buckets/free_tail");
#endif //FSLL_DUMP
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
char * next_bucket(void)
{
    char *bucket = fsll_getlink(cache_dir, "buckets/free_head");
    if (bucket != NULL) {
        DEBUG("re-using free bucket %s\n", bucketname(bucket));

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
            PERROR("open next_bucket");
            return makebucket(0);
        } else {
            if (f != NULL) {
                // we had a number already there; read it
                if (fscanf(f, "%llu", (unsigned long long *)&next) != 1) {
                    ERROR("unable to read next_bucket\n");
                    fclose(f);
                    return makebucket(0);
                }
                f = freopen(nbnpath, "w+", f);
            } else {
                // next_bucket_number doesn't exist; create it and write a 1.
                f = fopen(nbnpath, "w+");
                if (f == NULL) {
                    PERROR("open next_bucket again");
                    return makebucket(0);
                }
            }
            // write the next number
            if (f == NULL) {
                PERROR("fdopen for writing in next_bucket");
                return makebucket(0);
            }
            fprintf(f, "%llu\n", (unsigned long long) next+1);
            fclose(f);
        }

        DEBUG("making new bucket %lu\n", (unsigned long) next);

        char *new_bucket = makebucket(next);

        return new_bucket;
    }
}

/*
 * moves a bucket to the head of the used queue
 */
void bucket_to_head(const char *bucketpath)
{
    DEBUG("bucket_to_head(%s)\n", bucketpath);
    fsll_to_head(cache_dir, bucketpath, "buckets/head", "buckets/tail");
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
 * Starting at the dirname of path, remove empty directories upwards in the
 * path heirarchy.
 *
 * Stops when it gets to <cache_dir>/buckets or <cache_dir>/map
 */
void trim_directory(const char *path)
{
    size_t len = strlen(path);
    char *copy = (char*)malloc(len+1);
    strncpy(copy, path, len+1);

    char map[PATH_MAX];
    char buckets[PATH_MAX];
    snprintf(map, PATH_MAX, "%s/map", cache_dir);
    snprintf(buckets, PATH_MAX, "%s/buckets", cache_dir);

    char *dir = dirname(copy);
    while ((strcmp(dir, map) != 0) && (strcmp(dir, buckets) != 0)) {
    
        DIR *d = opendir(dir);
        struct dirent *e;
        bool found_mtime = false;
        while ((e = readdir(d)) != NULL) {
            if (e->d_name[0] == '.')
                continue;
            
            // remove mtime files, if found
            if (strcmp(e->d_name, "mtime") == 0) {
                struct stat s;
                char mtime[PATH_MAX];
                snprintf(mtime, PATH_MAX, "%s/mtime", dir);
                stat(mtime, &s);
                if (S_IFREG & s.st_mode) {
                    found_mtime = true;
                    continue;
                }
            }
            
            // if we got here, the directory has entries
            DEBUG("directory has entries -- in %s found '%s'\n", dir, e->d_name);
            closedir(d);
            FREE(copy);
            return;
        }
        
        if (found_mtime) {
            char mtime[PATH_MAX];
            snprintf(mtime, PATH_MAX, "%s/mtime", dir);
            if (unlink(mtime) == -1) {
                PERROR("in trim_directory, unable to unlink mtime file");
                ERROR("\tpath was %s\n", mtime);
            } else {
                DEBUG("removed mtime file %s/mtime\n", dir);
            }
        }

        closedir(d);
        d = NULL;

        int result = rmdir(dir);
        if (result == -1) {
            if (errno != EEXIST && errno != ENOTEMPTY) {
                PERROR("in trim_directory, rmdir");
            }
            
            WARN("in trim_directory, directory still not empty, but how? path was %s\n", dir);
            FREE(copy);
            return;
        } else {
            DEBUG("removed empty map directory %s\n", dir);
        }

        dir = dirname(dir);
    }

    FREE(copy);
}

/*
 * free a bucket
 *
 * moves bucket from the tail of the used queue to the tail of the free queue,
 * deletes the data in the bucket
 * returns the size of the data deleted
 */
uint64_t free_bucket_real(const char *bucketpath, bool free_in_the_middle_is_bad)
{
    char *parent = fsll_getlink(bucketpath, "parent");
    if (parent && fsll_file_exists(parent, NULL)) {
        DEBUG("bucket parent: %s\n", parent);
        if (unlink(parent) == -1) {
            PERROR("unlink parent in free_bucket");
        }

        // if this was the last block, remove the directory
        trim_directory(parent);
    }
    FREE(parent);
    fsll_makelink(bucketpath, "parent", NULL);

    if (free_in_the_middle_is_bad) {
        char *n = fsll_getlink(bucketpath, "next");
        if (n != NULL) {
            ERROR("bucket freed (#%lu) was not the queue tail\n",
                    (unsigned long) bucket_path_to_number(bucketpath));
            FREE(n);
            return 0;
        }
    }

    fsll_disconnect(cache_dir, bucketpath, 
            "buckets/head", "buckets/tail");

    fsll_insert_as_tail(cache_dir, bucketpath,
            "buckets/free_head", "buckets/free_tail");

    char data[PATH_MAX];
    snprintf(data, PATH_MAX, "%s/data", bucketpath);
    
    struct stat s;
    if (stat(data, &s) == -1) {
        PERROR("stat data in free_bucket");
    }

    pthread_mutex_lock(&lock);
    uint64_t result = 0;
    if (unlink(data) == -1) {
        PERROR("unlink data in free_bucket");
    } else {
      result = (uint64_t) s.st_size;
      if (!is_unchecked(bucketpath)) {
        cache_used_size -= result;
      }
    }
    pthread_mutex_unlock(&lock);
    return result;
}

uint64_t free_bucket_mid_queue(const char *bucketpath)
{
    return free_bucket_real(bucketpath, false);
}

uint64_t free_bucket(const char *bucketpath)
{
    return free_bucket_real(bucketpath, true);
}

/*
 * do not use this function directly
 */
int cache_invalidate_bucket(const char *filename, uint32_t block, 
                                const char *bucket)
{
    DEBUG("invalidating block %lu of file %s\n",
            (unsigned long) block, filename);

    uint64_t freed_size = free_bucket_mid_queue(bucket);

    DEBUG("freed %llu bytes in bucket %s\n",
            (unsigned long long) freed_size,
            bucketname(bucket));

    return 0;
}

int cache_invalidate_file_real(const char *filename, bool error_if_not_exist)
{
    char mappath[PATH_MAX];
    snprintf(mappath, PATH_MAX, "%s/map%s", cache_dir, filename);
    DIR *d = opendir(mappath);
    if (d == NULL) {
        if (errno != ENOENT || error_if_not_exist) {
            PERROR("opendir in cache_invalidate");
        }
        return -errno;
    }

    struct dirent *e = malloc(offsetof(struct dirent, d_name) + PATH_MAX + 1);
    struct dirent *result = e;
    while (readdir_r(d, e, &result) == 0 && result != NULL) {
        // probably not needed, because trim_directory would take care of the
        // mtime file, but might as well do it now to save time.
        if (strcmp(e->d_name, "mtime") == 0) {
            char mtime[PATH_MAX];
            snprintf(mtime, PATH_MAX, "%s/mtime", mappath);
            DEBUG("removed mtime file %s\n", mtime);
            unlink(mtime);
            continue;
        }
    
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;

        char *bucket = fsll_getlink(mappath, e->d_name);
        uint32_t block;
        sscanf(e->d_name, "%lu", (unsigned long *)&block);
    
        cache_invalidate_bucket(filename, block, bucket);

        FREE(bucket);
    }

    FREE(e);
    closedir(d);

    return 0;
}

int cache_invalidate_file_(const char *filename, bool error_if_not_exist)
{
    pthread_mutex_lock(&lock);
    int retval = cache_invalidate_file_real(filename, error_if_not_exist);
    pthread_mutex_unlock(&lock);   
    return retval;
}

int cache_invalidate_file(const char *filename)
{
    return cache_invalidate_file_(filename, true);
}

int cache_try_invalidate_file(const char *filename)
{
    return cache_invalidate_file_(filename, false);
}

int cache_invalidate_block_(const char *filename, uint32_t block,
    bool warn_if_not_exist)
{
    char mappath[PATH_MAX];
    snprintf(mappath, PATH_MAX, "map%s/%lu",
            filename, (unsigned long) block);

    pthread_mutex_lock(&lock);
    
    char *bucket = fsll_getlink(cache_dir, mappath);
    if (bucket == NULL) {
        if (warn_if_not_exist) {
            WARN("Cache invalidation: block %lu of file %s doesn't exist.\n",
                    (unsigned long) block, filename);
        }
        pthread_mutex_unlock(&lock);
        return -ENOENT;
    }

    cache_invalidate_bucket(filename, block, bucket);

    FREE(bucket);

    pthread_mutex_unlock(&lock);

    return 0;
}

int cache_invalidate_block(const char *filename, uint32_t block)
{
    return cache_invalidate_block_(filename, block, true);
}

int cache_try_invalidate_block(const char *filename, uint32_t block)
{
    return cache_invalidate_block_(filename, block, false);
}

int cache_try_invalidate_blocks_above(const char *filename, uint32_t block)
{
    DEBUG("trying to invalidate blocks >= %ld in %s\n", block, filename);
    int ret = 0;
    DIR *mapdir = NULL;
    bool locked = false;
    struct dirent *e = NULL;

    char mappath[PATH_MAX];
    snprintf(mappath, PATH_MAX, "%s/map%s", cache_dir, filename);

    pthread_mutex_lock(&lock);
    locked = true;

    mapdir = opendir(mappath);
    if (mapdir == NULL) {
        ret = -errno;
        goto exit;
    }

    e = malloc(offsetof(struct dirent, d_name) + PATH_MAX + 1);
    struct dirent *result = e;
    while ((readdir_r(mapdir, e, &result) == 0) && (result != NULL)) {
        if ((e->d_name[0] < '0') || (e->d_name[0] > '9')) continue;

        uint32_t block_found;
        sscanf(e->d_name, "%lu", &block_found);

        if (block_found >= block) {
            char *bucket = fsll_getlink(mappath, e->d_name);
            cache_invalidate_bucket(filename, block_found, bucket);
            FREE(bucket);
        }
    }

exit:
    closedir(mapdir);
    FREE(e);
    if (locked)
        pthread_mutex_unlock(&lock);
    return ret;
}

int cache_free_orphan_buckets(void)
{
    char bucketdir[PATH_MAX];
    snprintf(bucketdir, PATH_MAX, "%s/buckets", cache_dir);

    pthread_mutex_lock(&lock);

    DIR *d = opendir(bucketdir);
    if (d == NULL) {
        PERROR("opendir in cache_free_orphan_buckets");
        pthread_mutex_unlock(&lock);
        return -1*errno;
    }

    struct dirent *e = malloc(offsetof(struct dirent, d_name) + PATH_MAX + 1);
    struct dirent *result = e;
    while (readdir_r(d, e, &result) == 0 && result != NULL) {
        if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;

        char bucketpath[PATH_MAX];
        snprintf(bucketpath, PATH_MAX, "%s/buckets/%s", cache_dir, e->d_name);

        char *parent = fsll_getlink(bucketpath, "parent");

        if (fsll_file_exists(bucketpath, "data") &&
                (parent == NULL || !fsll_file_exists(parent, NULL))) {
            DEBUG("bucket %s is an orphan\n", e->d_name);
            if (parent) {
                DEBUG("\tparent was %s\n", parent);
            }
            free_bucket_mid_queue(bucketpath);
        }

        FREE(parent);
    }

    closedir(d);
    FREE(e);
    pthread_mutex_unlock(&lock);

    return 0;
}

/*
 * Read a block from the cache.
 * Important: you can specify less than one block, but not more.
 * Nor can a read be across block boundaries.
 *
 * mtime is the file modification time. If what's in the cache doesn't match
 * this, the cache data is invalidated and this function returns -1 and sets
 * ENOENT.
 *
 * Returns 0 on success.
 * On error returns -1 and sets errno.
 * In particular, if the block is not in the cache, sets ENOENT
 */
int cache_fetch(const char *filename, uint32_t block, uint64_t offset, 
        char *buf, uint64_t len, uint64_t *bytes_read, time_t mtime)
{
    if (offset + len > bucket_max_size || filename == NULL) {
        errno = EINVAL;
        return -1;
    }

    if (len == 0) {
        *bytes_read = 0;
        return 0;
    }

    DEBUG("getting block %lu of file %s\n", (unsigned long) block, filename);

    //###
    pthread_mutex_lock(&lock);

    char mapfile[PATH_MAX];
    snprintf(mapfile, PATH_MAX, "%s/map%s/%lu",
            cache_dir, filename, (unsigned long) block);
    char bucketpath[PATH_MAX];
    ssize_t bplen;
    if ((bplen = readlink(mapfile, bucketpath, PATH_MAX-1)) == -1) {
        if (errno == ENOENT || errno == ENOTDIR) {
            DEBUG("block not in cache\n");
            errno = ENOENT;
            pthread_mutex_unlock(&lock);
            return -1;
        } else {
            PERROR("readlink error");
            errno = EIO;
            pthread_mutex_unlock(&lock);
            return -1;
        }
    }
    bucketpath[bplen] = '\0';

    bucket_to_head(bucketpath);
    
    uint64_t bucket_mtime;
    char mtimepath[PATH_MAX];
    snprintf(mtimepath, PATH_MAX, "%s/map%s/mtime", cache_dir, filename);
    FILE *f = fopen(mtimepath, "r");
    if (f == NULL) {
        PERROR("open mtime file failed");
        bucket_mtime = 0; // will cause invalidation
    } else {
        if (fscanf(f, "%llu", (unsigned long long *) &bucket_mtime) != 1) {
            ERROR("error reading mtime file");

            // debug
            char buf[4096];
            fseek(f, 0, SEEK_SET);
            size_t b = fread(buf, 1, 4096, f);
            buf[b] = '\0';
            ERROR("mtime file contains: %u bytes: %s", (unsigned int) b, buf);

            fclose(f);
            f = NULL;
            unlink(mtimepath);

            bucket_mtime = 0; // will cause invalidation
        }
    }
    if (f) fclose(f);
    
    if (bucket_mtime != (uint64_t)mtime) {
        // mtime mismatch; invalidate and return
        if (bucket_mtime < (uint64_t)mtime) {
            DEBUG("cache data is %llu seconds older than the backing data\n",
                 (unsigned long long) mtime - bucket_mtime);
        } else {
            DEBUG("cache data is %llu seconds newer than the backing data\n",
                 (unsigned long long) bucket_mtime - mtime);
        }
        cache_invalidate_file_real(filename, true);
        errno = ENOENT;
        pthread_mutex_unlock(&lock);
        return -1;
    }
    
    // [cache_dir]/buckets/%lu/data
    char bucketdata[PATH_MAX];
    snprintf(bucketdata, PATH_MAX, "%s/data", bucketpath);

    uint64_t size = 0;
    struct stat stbuf;
    if (stat(bucketdata, &stbuf) == -1) {
        PERROR("stat on bucket error");
        errno = EIO;
        pthread_mutex_unlock(&lock);
        return -1;
    }
    size = (uint64_t) stbuf.st_size;

    if (size < offset) {
        WARN("offset for read is past the end: %llu vs %llu, bucket %s\n",
                (unsigned long long) offset,
                (unsigned long long) size,
                bucketname(bucketpath));
        pthread_mutex_unlock(&lock);
        *bytes_read = 0;
        return 0;
    }

    /*
    if (e->bucket->size - offset < len) {
        WARN("length + offset for read is past the end\n");
        errno = ENXIO;
        FREE(f);
        pthread_mutex_unlock(&lock);
        return -1;
    }
    */

    int fd = open(bucketdata, O_RDONLY);
    if (fd == -1) {
        PERROR("error opening file from cache dir");
        errno = EBADF;
        pthread_mutex_unlock(&lock);
        return -1;
    }

    *bytes_read = pread(fd, buf, len, offset);
    if (*bytes_read == -1) {
        PERROR("error reading file from cache dir");
        errno = EIO;
        close(fd);
        pthread_mutex_unlock(&lock);
        return -1;
    }

    if (*bytes_read != len) {
        DEBUG("read fewer than requested bytes from cache file: %llu instead of %llu\n", 
                (unsigned long long) *bytes_read,
                (unsigned long long) len
        );
        /*
        errno = EIO;
        FREE(f);
        FREE(cachefile);
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

uint64_t free_tail_bucket()
{
    uint64_t freed_bytes = 0;
    char *tail = fsll_getlink(cache_dir, "buckets/tail");

    if (tail == NULL) {
        ERROR("can't free the tail bucket, no buckets in queue!\n");
        goto exit;
    }

    freed_bytes = free_bucket(tail);
    DEBUG("freed %llu bytes in bucket %lu\n",
            (unsigned long long)freed_bytes,
            (unsigned long)bucket_path_to_number(tail));

exit:
    FREE(tail);
    return freed_bytes;
}

void make_space_available(uint64_t bytes_needed)
{
    uint64_t bytes_freed = 0;

    if (bytes_needed == 0)
        return;

    uint64_t dev_free = get_cache_fs_free_size(cache_dir);
    if (dev_free >= bytes_needed) {
        // device has plenty
        if (use_whole_device) {
            return;
        } else {
            // cache_size is limiting factor
            if (cache_used_size + bytes_needed <= cache_size) {
                return;
            } else {
                bytes_needed = (cache_used_size + bytes_needed) - cache_size;
            }
        }
    } else {
        // dev_free is limiting factor
        bytes_needed = bytes_needed - dev_free;
    }

    DEBUG("need to free %llu bytes\n",
            (unsigned long long) bytes_needed);

    while (bytes_freed < bytes_needed) {
        bytes_freed += free_tail_bucket();
    }

    DEBUG("freed %llu bytes total\n",
            (unsigned long long) bytes_freed);
}

/*
 * Adds a data block to the cache.
 * Important: this must be the FULL block. All subsequent reads will
 * assume that the full block is here.
 */
int cache_add(const char *filename, uint32_t block, const char *buf,
              uint64_t len, time_t mtime)
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

    DEBUG("writing %llu bytes to %s\n", (unsigned long long) len, fileandblock);

    //###
    pthread_mutex_lock(&lock);

    char *bucketpath = fsll_getlink(cache_dir, fileandblock);

    if (bucketpath != NULL) {
        if (fsll_file_exists(bucketpath, "data")) {
            WARN("data already exists in cache\n");
            FREE(bucketpath);
            pthread_mutex_unlock(&lock);
            return 0;
        }
    }

    char *filemap = (char*)malloc(strlen(filename) + 4);
    snprintf(filemap, strlen(filename)+4, "map%s", filename);

    char *full_filemap_dir = (char*)malloc(strlen(cache_dir) + 5 + strlen(filename) + 1);
    snprintf(full_filemap_dir, strlen(cache_dir)+5+strlen(filename)+1, "%s/map%s/",
            cache_dir, filename);

    DEBUG("map file = %s\n", filemap);
    DEBUG("full filemap dir = %s\n", full_filemap_dir);

    if (!fsll_file_exists(cache_dir, filemap)) {
        FREE(filemap);
        size_t i;
        // start from "$cache_dir/map/"
        for (i = strlen(cache_dir) + 5; i < strlen(full_filemap_dir); i++) {
            if (full_filemap_dir[i] == '/') {
                char *component = (char*)malloc(i+1);
                strncpy(component, full_filemap_dir, i+1);
                component[i] = '\0';
                DEBUG("making %s\n", component);
                if(mkdir(component, 0700) == -1 && errno != EEXIST) {
                    if (errno == ENOSPC) {
                        // try to free some space
                        DEBUG("mkdir says ENOSPC, freeing and trying again\n");
                        free_tail_bucket();
                        errno = EAGAIN;
                    }
                    else {
                        PERROR("mkdir in cache_add");
                        ERROR("\tcaused by mkdir(%s)\n", component);
                        errno = EIO;
                    }
                    FREE(component);
                    FREE(full_filemap_dir);
                    pthread_mutex_unlock(&lock);
                    return -1;
                }
                FREE(component);
            }
        }
    } else {
        FREE(filemap);
    }
    FREE(full_filemap_dir);

    make_space_available(len);

    FREE(bucketpath);
    bucketpath = next_bucket();
    DEBUG("bucket path = %s\n", bucketpath);

    fsll_makelink(cache_dir, fileandblock, bucketpath);

    char *fullfilemap = (char*)malloc(PATH_MAX);
    snprintf(fullfilemap, PATH_MAX, "%s/%s", cache_dir, fileandblock);
    fsll_makelink(bucketpath, "parent", fullfilemap);
    FREE(fullfilemap);
    
    // write mtime
    
    char mtimepath[PATH_MAX];
    snprintf(mtimepath, PATH_MAX, "%s/map%s/mtime", cache_dir, filename);
    FILE *f = fopen(mtimepath, "w");
    if (f == NULL) {
        PERROR("opening mtime file in cache_add failed");
    } else {
        fprintf(f, "%llu\n", (unsigned long long) mtime);
        fclose(f);
    }

    // finally, write data

    char datapath[PATH_MAX];
    snprintf(datapath, PATH_MAX, "%s/data", bucketpath);

    int fd = open(datapath, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        PERROR("open in cache_add");
        ERROR("\tcaused by open(%s, O_WRONLY|O_CREAT)\n", datapath);
        errno = EIO;
        pthread_mutex_unlock(&lock);
        return -1;
    }

    ssize_t bytes_written = write(fd, buf, len);
    if (bytes_written == -1) {
        if (errno == ENOSPC) {
            DEBUG("nothing written (no space on device)\n");
            bytes_written = 0;
        } else {
            PERROR("write in cache_add");
            errno = EIO;
            close(fd);
            pthread_mutex_unlock(&lock);
            return -1;
        }
    }

    DEBUG("%llu bytes written to cache\n",
            (unsigned long long) bytes_written);

    bool unchecked = is_unchecked(bucketpath);
    if (unchecked) {
      cache_used_size += bytes_written;
    }

    // for some reason (filesystem metadata overhead?) this may need to loop a
    // few times to write everything out.
    while (bytes_written != len) {
        DEBUG("not all bytes written to cache\n");

        // Try again, more forcefully this time.
        // Don't care if the FS says it has space, make some space anyway.
        free_tail_bucket();

        ssize_t more_bytes_written = write(fd, buf + bytes_written, len - bytes_written);

        if (more_bytes_written == -1) {
            if (errno == ENOSPC) {
                // this is normal
                DEBUG("nothing written (no space on device)\n");
                more_bytes_written = 0;
            } else {
                PERROR("write error");
                close(fd);
                pthread_mutex_unlock(&lock);
                return -EIO;
            }
        }

        DEBUG("%llu more bytes written to cache (%llu total)\n",
            (unsigned long long) more_bytes_written,
            (unsigned long long) more_bytes_written + bytes_written);

        if (unchecked) {
          cache_used_size += more_bytes_written;
        }
        bytes_written += more_bytes_written;
    }

    DEBUG("size now %llu bytes of %llu bytes (%lf%%)\n",
            (unsigned long long) cache_used_size,
            (unsigned long long) cache_size,
            (double)100 * cache_used_size / cache_size
    );

    dump_queues();

    close(fd);

    pthread_mutex_unlock(&lock);
    //###

    FREE(bucketpath);
    return 0;
}

int cache_has_file_real(const char *filename, uint64_t *cached_byte_count, bool do_lock)
{
    DEBUG("cache_has_file %s\n", filename);

    int ret = 0;
    bool locked = false;
    char *mapdir = NULL;
    char *data = NULL;
    DIR *dir = NULL;

    if (filename == NULL || cached_byte_count == NULL) {
        errno = EINVAL;
        ret = -1;
        goto exit;
    }

    if (do_lock) {
        pthread_mutex_lock(&lock);
        locked = true;
    }

    // Look up the file in the map.
    asprintf(&mapdir, "%s/map%s", cache_dir, filename);
    dir = opendir(mapdir);
    if (dir == NULL) {
        if (errno == ENOENT) {
            DEBUG("not in cache (%s)\n", mapdir);
            ret = 0;
            goto exit;
        }
        else {
            PERROR("opendir");
            ERROR("\topendir on %s\n", mapdir);
            ret = -EIO;
            goto exit;
        }
    }

    // Check if it's a file or directory (see if it has an mtime file).
    asprintf(&data, "%s/mtime", mapdir);
    bool is_file = false;
    struct stat mtime_stat = {0};
    if (0 == stat(data, &mtime_stat)) {
        is_file = (mtime_stat.st_mode & S_IFREG) != 0;
    }
    else if (errno != ENOENT) {
        PERROR("stat");
        ERROR("\tstat on %s\n", mapdir);
        ret = -EIO;
        goto exit;
    }

    // Loop over the sub-entries in the map.
    struct dirent *dirent = NULL;
    while ((dirent = readdir(dir)) != NULL) {
        if (dirent->d_name[0] != '.' && strcmp(dirent->d_name, "mtime") != 0) {
            FREE(data);

            if (is_file) {
                asprintf(&data, "%s/%s/data", mapdir, dirent->d_name);
            
                struct stat statbuf = {0};
                if (0 != stat(data, &statbuf)) {
                    PERROR("stat");
                    ERROR("\tstat on %s\n", data);
                    ret = -EIO;
                    goto exit;
                }
                
                DEBUG("%llu bytes in %s\n", statbuf.st_size, data);
                *cached_byte_count += statbuf.st_size;
            }
            else {
                asprintf(&data, "%s/%s", filename, dirent->d_name);
                ret = cache_has_file_real(data, cached_byte_count, false /*don't lock*/);
                if (ret != 0) {
                    break;
                }
            }
        }
    }

exit:
    if (locked)
        pthread_mutex_unlock(&lock);
    FREE(mapdir);
    FREE(data);
    closedir(dir);
    return ret;
}

int cache_has_file(const char *filename, uint64_t *cached_bytes)
{
    *cached_bytes = 0;
    return cache_has_file_real(filename, cached_bytes, true);
}

int cache_rename(const char *path, const char *path_new)
{
    DEBUG("cache_rename %s\n\t%s\n", path, path_new);

    int ret = 0;
    char *mapdir = NULL;
    char *mapdir_new = NULL;
    DIR *dir = NULL;
    char *parentlink = NULL;

    if (path == NULL || path_new == NULL) {
        errno = EINVAL;
        ret = -1;
        goto exit;
    }

    // Look up and rename the cache map dir.
    asprintf(&mapdir, "%s/map%s", cache_dir, path);
    asprintf(&mapdir_new, "%s/map%s", cache_dir, path_new);
    ret = rename(mapdir, mapdir_new);
    if (0 != ret) {
        if (ENOENT == errno) {
            DEBUG("not in cache: %s\n", path);
            ret = 0;
        }
        else {
            PERROR("rename");
            ret = -EIO;
        }
        goto exit;
    }

    // Next, need to fix all the buckets' parent links.
    dir = opendir(mapdir_new);
    if (dir == NULL) {
        PERROR("opendir");
        ERROR("\topendir on %s\n", mapdir_new);
        ret = -EIO;
        goto exit;
    }

    size_t cch_parentlink = strlen(mapdir_new) + 20;
    parentlink = (char*)malloc(cch_parentlink);

    struct dirent *dirent = NULL;
    while ((dirent = readdir(dir)) != NULL) {
        if (dirent->d_name[0] != '.' && strcmp(dirent->d_name, "mtime") != 0) {
            snprintf(parentlink, cch_parentlink, "%s/%s/parent",
                mapdir_new, dirent->d_name);

            ret = unlink(parentlink);
            if (0 != ret) {
                PERROR("unlink");
                ERROR("\tunlink on %s\n", parentlink);
                ret = -EIO;
                goto exit;
            }

            ret = symlink(mapdir_new, parentlink);
            if (0 != ret) {
                PERROR("symlink");
                ERROR("\tsymlink from %s\n\tto %s\n", parentlink, mapdir_new);
                ret = -EIO;
                goto exit;
            }
        }
    }

exit:
    FREE(mapdir);
    FREE(mapdir_new);
    FREE(parentlink);
    closedir(dir);
    return ret;
}

/*

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

*/

