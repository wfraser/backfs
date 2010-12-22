#ifndef BACKFS_CACHE_WRF_H
#define BACKFS_CACHE_WRF_H

#include <stdlib.h>
#include <stdint.h>

// 1 MiB
#define BUCKET_MAX_SIZE 1048576

struct bucket {
    uint32_t number;
    uint64_t size;
    struct bucket *prev, *next;
};

struct bucket_list {
    struct bucket *head, *tail;
};

// n.b.: bht = bucket_hash_table

#define BHT_SIZE 4096

struct bht_entry {
    uint32_t hash;
    struct bucket *bucket;
    struct bht_entry *next;
};

void cache_init(const char *cache_dir, uint64_t cache_size);
int cache_fetch(const char *filename, uint32_t block, uint64_t offset,
    char *buf, uint64_t len);
int cache_add(const char *filename, uint32_t block, char *buf, 
    uint64_t len);

uint32_t sfh(char *data, size_t len);

#endif //BACKFS_CACHE_WRF_H
