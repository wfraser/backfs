#include "cache.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <fcntl.h>

#include <pthread.h>
static pthread_mutex_t lock;

#undef get16bits
#if (defined(__GNUC__) && defined(__i386__)) || defined(__WATCOMC__) \
  || defined(_MSC_VER) || defined (__BORLANDC__) || defined (__TURBOC__)
#define get16bits(d) (*((const uint16_t *) (d)))
#else
#define get16bits(d)  ((((uint32_t)(((const uint8_t *)(d))[1])) << 8)\
                        +(uint32_t)(((const uint8_t *)(d))[0]) )
#endif

char *cache_dir;
uint64_t cache_size;
uint64_t cache_used_size;
bool use_whole_device;
uint32_t next_bucket_number;
struct bucket_list free_buckets;
struct bucket_list bqueue;
struct bht_entry *bht[BHT_SIZE];
struct bht_entry *filehash[BHT_SIZE];

/*
 * Initialize the cache.
 */
void cache_init(const char *a_cache_dir, uint64_t a_cache_size, bool a_use_whole_device)
{
    use_whole_device = a_use_whole_device;
    cache_dir = (char*)malloc(strlen(a_cache_dir)+1);
    strcpy(cache_dir, a_cache_dir);
    cache_size = a_cache_size;
    cache_used_size = 0;
    next_bucket_number = 0;
    free_buckets.head = NULL;
    free_buckets.tail = NULL;
    bqueue.head = NULL;
    bqueue.tail = NULL;
    memset(bht, 0, sizeof(bht));
    memset(filehash, 0, sizeof(filehash));
}

int cache_delete(const char *filename)
{
    uint32_t hash = sfh(filename, strlen(filename));
    struct bht_entry *e = filehash[hash % BHT_SIZE];
    while (e != NULL) {
        if (strcmp(e->filename, filename) == 0)
            break;
        e = e->next;
    }
    if (e == NULL) {
        fprintf(stderr, "BackFS CACHE: can't remove %s from the cache; it's not in there.\n", filename);
        errno = ENOENT;
        return -1;
    }

    struct bucket *b = e->bucket;
    // [cache_dir]/buckets/%010u
    char *cachefile = (char*)malloc(strlen(cache_dir) + 9 + 10 + 1);
    while (b != NULL) {
        snprintf(cachefile, strlen(cache_dir)+9+10+1, "%s/buckets/%010lu",
                cache_dir, (unsigned long) b->number);
        unlink(cachefile);
        cache_used_size -= b->size;

        // remove bucket to the tail of the free_buckets queue
        
        if (b->prev)
            b->prev->next = b->next;
        if (b->next)
            b->next->prev = b->prev;

        b->prev = free_buckets.tail;
        if (free_buckets.tail)
            free_buckets.tail->next = b;
        free_buckets.tail = b;
        b->next = NULL;

        b = b->file_next;
    }

    return 0;
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

    // file + block
    // max block len is 10 chars (4294967296)
    size_t fl = strlen(filename) + 12;
    char *f = (char*)malloc(fl);
    fl = snprintf(f, fl, "%s/%010lu", filename, (unsigned long)block);
    uint32_t hash = sfh(f, fl);

    fprintf(stderr, "BackFS CACHE: getting block %lu of file %s: hash %08lx\n", 
            (unsigned long) block, filename, (unsigned long) hash);

    //###
    pthread_mutex_lock(&lock);

    struct bht_entry *e = bht[hash % BHT_SIZE];
    while (e != NULL) {
        if (strcmp(e->filename, filename) == 0)
            break;
        e = e->next;
    }

    if (e == NULL) {
        fprintf(stderr, "BackFS CACHE: block not in cache\n");
        errno = ENOENT;
        free(f);
        pthread_mutex_unlock(&lock);
        return -1;
    }

    if (e->bucket->size < offset) {
        fprintf(stderr, "BackFS CACHE: offset for read is past the end\n");
        free(f);
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

    // [cache_dir]/buckets/%010u
    char *cachefile = (char*)malloc(strlen(cache_dir) + 9 + 10 + 1);
    snprintf(cachefile, strlen(cache_dir)+9+10+1, "%s/buckets/%010lu", 
           cache_dir, (unsigned long) e->bucket->number);
    fprintf(stderr, "BackFS CACHE: reading from %s\n", cachefile);
    int fd = open(cachefile, O_RDONLY);
    if (fd == -1) {
        perror("BackFS Cache ERROR: error opening file from cache dir");
        errno = EBADF;
        free(f);
        free(cachefile);
        pthread_mutex_unlock(&lock);
        return -1;
    }

    *bytes_read = pread(fd, buf, len, offset);
    if (*bytes_read == -1) {
        perror("BackFS Cache ERROR: error reading file from cache dir");
        errno = EIO;
        free(f);
        free(cachefile);
        close(fd);
        return -1;
        pthread_mutex_unlock(&lock);
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

struct bht_entry *new_bht_entry()
{
    struct bht_entry *e = (struct bht_entry *)malloc(sizeof(struct bht_entry));
    return e;
}

struct bucket *new_bucket()
{
    struct bucket *b = (struct bucket *)malloc(sizeof(struct bucket));
    return b;
}

void make_space_available(uint64_t bytes_needed)
{
    struct statvfs svfs;
    if (statvfs(cache_dir, &svfs) == -1) {
        perror("BackFS Cache ERROR: in make_space_available: unable to stat filesystem!! ");
        return;
    }

    // free space is either the free space on the device,
    //   or the free space allowed to us,
    //   whichever is less.

    uint64_t device_free = (uint64_t) svfs.f_bsize * svfs.f_bfree;
    uint64_t bytes_free;

    if (!use_whole_device) {
        uint64_t cache_free = cache_size - cache_used_size;
        if (cache_free > device_free) {
            fprintf(stderr, "BackFS CACHE: WARNING: limited by device free space! "
                    "Cache should have %llu bytes free, but device only has %llu bytes.\n",
                    (unsigned long long) cache_free, (unsigned long long) device_free);
        }

        bytes_free = (cache_free < device_free) ? cache_free : device_free;
    } else {
        bytes_free = device_free;
    }

    if (bytes_free >= bytes_needed) {
        return;
    }

    uint64_t bytes_to_free = bytes_needed - bytes_free;
    uint64_t bytes_freed = 0;
    struct bucket *b = bqueue.tail;
    char *cache_filename = (char*)malloc(strlen(cache_dir)+9+10+1);
    while (bytes_freed < bytes_to_free) {
        fprintf(stderr, "BackFS CACHE: freeing %llu bytes in bucket #%lu\n", 
                (unsigned long long) b->size, (unsigned long) b->number);
        snprintf(cache_filename, strlen(cache_dir)+9+10+1, "%s/buckets/%010lu", 
                cache_dir, (unsigned long) b->number);
        unlink(cache_filename);
        bytes_freed += b->size;
        cache_used_size -= b->size;

        // take the bucket off the bqueue tail and make it the free_buckets tail
        
        b->prev->next = NULL;
        bqueue.tail = b->prev;
        
        b->prev = free_buckets.tail;
        free_buckets.tail->next = b;
        free_buckets.tail = b;

        b = bqueue.tail;
    }
    fprintf(stderr, "BackFS CACHE: freed %llu bytes\n", (unsigned long long) bytes_freed);
}

/*
 * Adds a data block to the cache.
 * Important: this must be the FULL block. All subsequent reads will
 * assume that the full block is here.
 */
int cache_add(const char *filename, uint32_t block, char *buf, uint64_t len)
{
    if (len > BUCKET_MAX_SIZE) {
        errno = EOVERFLOW;
        return -1;
    }

    // /filename/%010lu
    char *fileandblock = (char*)malloc(strlen(filename) + 12);
    size_t fl = snprintf(fileandblock, strlen(filename)+12, "%s/%010lu",
            filename, (unsigned long) block);
    fprintf(stderr, "BackFS CACHE: adding %s to cache\n", fileandblock);

    uint32_t hash = sfh(fileandblock, fl);

    //###
    pthread_mutex_lock(&lock);

    struct bht_entry *e = bht[hash % BHT_SIZE];
    if (e == NULL) {
        // hash table entry is empty
        e = new_bht_entry();
        bht[hash % BHT_SIZE] = e;
        e->prev = NULL;
    } else {
        while (e->next != NULL) {
            if (strcmp(e->filename, filename) == 0) {
                fprintf(stderr, "BackFS CACHE: block already exists in cache: #%lu\n",
                        (unsigned long) e->bucket->number);
                // move bucket to front of queue
                struct bucket *old_head = bqueue.head;
                bqueue.head = e->bucket;
                e->bucket->prev = NULL;
                e->bucket->next = old_head;
                old_head->prev = e->bucket;
                pthread_mutex_unlock(&lock);
                return 0;
            }
            e = e->next;
        }
        // e is now tail of hashtable entry list
        e->next = new_bht_entry();
        e->next->prev = e;
        e = e->next;
    }

    e->filename = (char*)malloc(strlen(filename)+1);
    strcpy(e->filename, filename);
    e->next = NULL;

    make_space_available(len);

    // grab a bucket
    if (free_buckets.head != free_buckets.tail) {
        // re-use a free bucket
        fprintf(stderr, "BackFS CACHE: re-using free bucket #%u\n",
                free_buckets.head->number);
        e->bucket = free_buckets.head;
        free_buckets.head = free_buckets.head->next;
        if (free_buckets.head != NULL) {
            free_buckets.head->prev = NULL;
        }
    } else {
        // create a new bucket
        fprintf(stderr, "BackFS CACHE: making new bucket #%u\n", next_bucket_number);
        e->bucket = new_bucket();
        e->bucket->number = next_bucket_number++;
    }

    cache_used_size += len;
    e->bucket->bht_entry = e;
    e->bucket->size = len;
    e->bucket->next = NULL;
    e->bucket->prev = bqueue.head;
    if (bqueue.head != NULL) {
        bqueue.head->next = e->bucket;
        bqueue.head = e->bucket;
    }

    // add to chain for filename
    struct bucket *b = e->bucket;
    hash = sfh(filename, strlen(filename));
    e = filehash[hash % BHT_SIZE];
    while (e != NULL && strcmp(e->filename, filename) != 0) {
        e = e->next;
    }
    if (e == NULL) {
        // new file to the cache
        e = new_bht_entry();
        e->filename = (char*)malloc(strlen(filename)+1);
        strcpy(e->filename, filename);
        e->prev = NULL;
        e->next = NULL;
        e->bucket = b;
        filehash[hash % BHT_SIZE] = e;
    }
    e->bucket->file_next = e->bucket;

    // [cache_dir]/buckets/%010u
    char *cachefile = (char*)malloc(strlen(cache_dir) + 9 + 10 + 1);
    snprintf(cachefile, strlen(cache_dir)+9+10+1, "%s/buckets/%010lu",
            cache_dir, (unsigned long) b->number);

    fprintf(stderr, "BackFS CACHE: cache file: %s, hash %08lx\n",
            cachefile, (unsigned long) hash);


    int fd = open(cachefile, O_WRONLY | O_CREAT | O_EXCL, 0660);
    if (fd == -1) {
        perror("BackFS Cache ERROR: error opening cache file");
        errno = EBADF;
        free(fileandblock);
        free(cachefile);
        pthread_mutex_unlock(&lock);
        return -1;
    }

    ssize_t bytes_written = write(fd, buf, len);
    if (bytes_written == -1) {
        perror("BackFS Cache ERROR: error writing cache file");
        errno = EIO;
        free(fileandblock);
        free(cachefile);
        close(fd);
        pthread_mutex_unlock(&lock);
        return -1;
    }
    if (bytes_written != len) {
        fprintf(stderr, "BackFS CACHE: not all bytes written to cache!\n");
        errno = EIO;
        free(fileandblock);
        free(cachefile);
        close(fd);
        pthread_mutex_unlock(&lock);
        return -1;
    }

    close(fd);

    pthread_mutex_unlock(&lock);
    //###

    return 0;
}

/*
 * SuperFastHash by Paul Hsieh
 * from http://www.azillionmonkeys.com/qed/hash.html
 * LGPL 2.1
 */
uint32_t sfh(const char *data, size_t len)
{
    uint32_t hash = len;
    uint32_t tmp;
    int rem;

    if (len <= 0 || data == NULL) {
        return 0;
    }

    rem = len & 3;
    len >>= 2;

    for (; len > 0; len--) {
        hash += get16bits(data);
        tmp   = (get16bits(data + 2) << 11) ^ hash;
        hash  = (hash << 16) ^ tmp;
        data += 2 * sizeof(uint16_t);
        hash += hash >> 11;
    }

    switch (rem) {
    case 3:
        hash += get16bits(data);
        hash ^= hash << 16;
        hash ^= data[sizeof(uint16_t)] << 18;
        hash += hash >> 11;
        break;
    case 2:
        hash += get16bits(data);
        hash ^= hash << 11;
        hash += hash >> 17;
        break;
    case 1:
        hash += *data;
        hash ^= hash << 10;
        hash += hash >> 1;
        break;
    }

    hash ^= hash << 3;
    hash += hash >> 5;
    hash ^= hash << 4;
    hash += hash >> 17;
    hash ^= hash << 25;
    hash += hash >> 6;

    return hash;
}


