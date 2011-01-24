#ifndef BACKFS_CACHE_WRF_H
#define BACKFS_CACHE_WRF_H
/*
 * BackFS Filesystem Cache
 * Copyright (c) 2010-2011 William R. Fraser
 */

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <limits.h>

void cache_init(const char *cache_dir, uint64_t cache_size, uint64_t bucket_max_size);
int cache_fetch(const char *filename, uint32_t block, uint64_t offset,
        char *buf, uint64_t len, uint64_t *bytes_read, time_t mtime);
int cache_add(const char *filename, uint32_t block, char *buf, 
        uint64_t len, time_t mtime);
int cache_invalidate_block(const char *filename, uint32_t block);
int cache_invalidate_file(const char *filename);
int cache_free_orphan_buckets();

#ifndef ERROR
#ifdef SYSLOG
#include <syslog.h>
#define ERROR(...) syslog(LOG_ERR, "CACHE ERROR: " __VA_ARGS__)
#define WARN(...) syslog(LOG_WARNING, "CACHE WARNING: " __VA_ARGS__)
#define INFO(...) syslog(LOG_INFO, "CACHE: " __VA_ARGS__)
#define PERROR(msg) syslog(LOG_ERR, "CACHE ERROR: " msg ": %m")
#else
#define ERROR(...) fprintf(stderr, "BackFS CACHE ERROR: " __VA_ARGS__)
#define WARN(...) fprintf(stderr, "BackFS CACHE WARNING: " __VA_ARGS__)
#define INFO(...) fprintf(stderr, "BackFS CACHE: " __VA_ARGS__)
#define PERROR(msg) perror("BackFS CACHE ERROR: " msg)
#endif //SYSLOG
#endif //ERROR

#endif //BACKFS_CACHE_WRF_H
