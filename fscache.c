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

#include <pthread.h>
static pthread_mutex_t lock;

char *cache_dir;
uint64_t cache_size;
uint64_t cache_used_size;
bool use_whole_device;
uint64_t bucket_max_size;

uint64_t get_cache_used_size(const char *root)
{
    uint64_t total = 0;
    struct dirent *e = malloc(offsetof(struct dirent, d_name) + PATH_MAX + 1);
    struct dirent *result = e;
    struct stat s;
    char buf[PATH_MAX];
    DIR *dir = opendir(root);
    while (readdir_r(dir, e, &result) == 0 && result != NULL) {
        snprintf(buf, PATH_MAX, "%s/%s/data", root, e->d_name);
        if (stat(buf, &s) == -1) {
            perror("BackFS CACHE ERROR: stat in get_cache_used_size");
            abort();
        }
        total += s.st_size;
    }
    if (result != NULL) {
        perror("BackFS CACHE ERROR: readdir in get_cache_used_size");
        abort();
    }

    closedir(dir);
    free(e);

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
    cache_used_size = get_cache_used_size(a_cache_dir);
    bucket_max_size = a_bucket_max_size;
}

char * getlink(const char *base, const char *file)
{
    char path[PATH_MAX];
    char *result = (char*)malloc(PATH_MAX);
    snprintf(result, PATH_MAX, "%s/%s", base, file);
    if (readlink(path, result, PATH_MAX) == -1) {
        if (errno == ENOENT || errno == ENOTDIR) {
            free(result);
            return NULL;
        } else {
            perror("BackFS CACHE ERROR: readlink in getlink");
            free(result);
            return NULL;
        }
    } else {
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
            return;
        }
    }

    if (dest != NULL) {
        if (symlink(dest, source) == -1) {
            perror("BackFS CACHE ERROR: symlink in makelink");
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

void bucket_to_head(const char *bucketpath)
{
    char *p = getlink(bucketpath, "prev");
    
    if (p == NULL) {
        // bucket is already head; do nothing
        return;
    }

    char *n = getlink(bucketpath, "next");
    char *h = getlink(cache_dir, "buckets/head");

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
    if (readlink(mapfile, bucketpath, PATH_MAX) == -1) {
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
        fprintf(stderr, "BackFS CACHE: read fewer than requested bytes from cache file: %lu instead of %lu\n", *bytes_read, len);
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
    //TODO
}

/*
 * Adds a data block to the cache.
 * Important: this must be the FULL block. All subsequent reads will
 * assume that the full block is here.
 */
int cache_add(const char *filename, uint32_t block, char *buf, uint64_t len)
{
    //TODO
    return -EIO;
}
