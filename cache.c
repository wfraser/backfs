#include "cache.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

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
uint32_t next_bucket_number;
struct bucket_list free_buckets;
struct bucket_list bqueue;
struct bht_entry *bht[BHT_SIZE];

/*
 * Initialize the cache.
 */
void cache_init(const char *a_cache_dir, uint64_t a_cache_size)
{
    cache_dir = (char*)malloc(strlen(a_cache_dir)+1);
    strcpy(cache_dir, a_cache_dir);
    cache_size = a_cache_size;
    next_bucket_number = 0;
    free_buckets.head = NULL;
    free_buckets.tail = NULL;
    bqueue.head = NULL;
    bqueue.tail = NULL;
    memset(bht, 0, sizeof(bht));
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
        char *buf, uint64_t len)
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

    fprintf(stderr, "getting block %lu of file %s: hash %08lx\n", 
            (unsigned long) block, filename, (unsigned long) hash);

    struct bht_entry *e = bht[hash % BHT_SIZE];
    while (e != NULL) {
        if (e->hash == hash)
            break;
        e = e->next;
    }

    if (e == NULL) {
        fprintf(stderr, "block not in cache\n");
        errno = ENOENT;
        free(f);
        return -1;
    }

    if (e->bucket->size < offset) {
        fprintf(stderr, "offset for read is past the end\n");
        errno = ENXIO;
        free(f);
        return -1;
    }

    if (e->bucket->size - offset < len) {
        fprintf(stderr, "length + offset for read is past the end\n");
        errno = ENXIO;
        free(f);
        return -1;
    }

    // [cache_dir]/buckets/%010u
    char *cachefile = (char*)malloc(strlen(cache_dir) + 9 + 10 + 1);
    snprintf(cachefile, strlen(cache_dir)+9+10+1, "%s/buckets/%010lu", 
           cache_dir, (unsigned long) block);
    fprintf(stderr, "reading from %s\n", cachefile);
    int fd = open(cachefile, O_RDONLY);
    if (fd == -1) {
        perror("error opening file from cache dir");
        errno = EBADF;
        free(f);
        free(cachefile);
        return -1;
    }

    ssize_t bytes_read = pread(fd, buf, len, offset);
    if (bytes_read == -1) {
        perror("error reading file from cache dir");
        errno = EIO;
        free(f);
        free(cachefile);
        close(fd);
        return -1;
    }
    if (bytes_read != len) {
        fprintf(stderr, "read fewer than requested bytes from cache file");
        errno = EIO;
        free(f);
        free(cachefile);
        close(fd);
        return -1;
    }

    close(fd);

    return 0;
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

    uint32_t hash = sfh(fileandblock, fl);

    // [cache_dir]/buckets/%010u
    char *cachefile = (char*)malloc(strlen(cache_dir) + 9 + 10 + 1);
    snprintf(cachefile, strlen(cache_dir)+9+10+1, "%s/buckets/%010lu",
            cache_dir, (unsigned long) block);

    fprintf(stderr, "cache file: %s, hash %08lx\n",
            cachefile, (unsigned long) hash);

    struct bht_entry *e = bht[hash % BHT_SIZE];
    if (e == NULL) {
        e = (struct bht_entry *)malloc(sizeof(struct bht_entry));
        bht[hash % BHT_SIZE] = e;
    } else {
        while (e->next != NULL) {
            if (e->hash == hash) {
                fprintf(stderr, "block already exists in cache: #%lu\n",
                        (unsigned long) e->bucket->number);
                // move bucket to front of queue
                struct bucket *old_head = bqueue.head;
                bqueue.head = e->bucket;
                e->bucket->prev = NULL;
                e->bucket->next = old_head;
                old_head->prev = e->bucket;
                return 0;
            }
            e = e->next;
        }
        e->next = (struct bht_entry *)malloc(sizeof(struct bht_entry));
        e = e->next;
    }

    e->hash = hash;
    e->next = NULL;
    if (free_buckets.head != free_buckets.tail) {
        // re-use a free bucket
        fprintf(stderr, "re-using free bucket #%u\n",
                free_buckets.head->number);
        e->bucket = free_buckets.head;
        free_buckets.head = free_buckets.head->next;
        if (free_buckets.head != NULL) {
            free_buckets.head->prev = NULL;
        }
    } else {
        // create a new bucket
        fprintf(stderr, "making new bucket #%u\n", next_bucket_number);
        e->bucket = (struct bucket *)malloc(sizeof(struct bucket));
        e->bucket->number = next_bucket_number++;
    }

    e->bucket->size = len;
    e->bucket->next = NULL;
    e->bucket->prev = bqueue.head;
    if (bqueue.head != NULL) {
        bqueue.head->next = e->bucket;
        bqueue.head = e->bucket;
    }

    int fd = open(cachefile, O_WRONLY | O_CREAT | O_EXCL, 0660);
    if (fd == -1) {
        perror("error opening cache file");
        errno = EBADF;
        free(fileandblock);
        free(cachefile);
        return -1;
    }

    ssize_t bytes_written = write(fd, buf, len);
    if (bytes_written == -1) {
        perror("error writing cache file");
        errno = EIO;
        free(fileandblock);
        free(cachefile);
        close(fd);
        return -1;
    }
    if (bytes_written != len) {
        fprintf(stderr, "not all bytes written to cache!\n");
        errno = EIO;
        free(fileandblock);
        free(cachefile);
        close(fd);
    }

    close(fd);

    return 0;
}

/*
 * SuperFastHash by Paul Hsieh
 * from http://www.azillionmonkeys.com/qed/hash.html
 * LGPL 2.1
 */
uint32_t sfh(char *data, size_t len)
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


