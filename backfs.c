// this needs to be first
#define _GNU_SOURCE
#include <fuse.h>
#include <fuse_opt.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/statvfs.h>

#include <pthread.h>

#include "cache.h"

struct backfs { 
    char *cache_dir;
    unsigned long long cache_size;
    pthread_mutex_t lock;
};

static struct backfs backfs;
#define BACKFS_OPT(t,p,v) { t, offsetof(struct 
static struct fuse_opt backfs_opts[] = {
    {"cache=%s",        offsetof(struct backfs, cache_dir),     0},
    {"cache_size=%llu", offsetof(struct backfs, cache_size),    0}
};

int backfs_open(const char *path, struct fuse_file_info *fi)
{
    fprintf(stderr, "open %s\n", path);

    if ((fi->flags & 3) != O_RDONLY) {
        return -EACCES;
    }

    //TODO
    //return -ENOENT;
    return 0;
}

int backfs_getattr(const char *path, struct stat *stbuf)
{
    //TODO
    fprintf(stderr, "getattr %s\n", path);
    memset(stbuf, 0, sizeof(struct stat));
    if (strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR || 0755;
        stbuf->st_nlink = 2;
    } else {
        stbuf->st_mode = S_IFREG || 0444;
        stbuf->st_nlink = 1;
        stbuf->st_size = 100;
    }

    return 0;
}

int backfs_read(const char *path, char *rbuf, size_t size, off_t offset,
        struct fuse_file_info *fi)
{
    fprintf(stderr, "reading from 0x%lx to 0x%lx, block size is 0x%lx\n",
            offset, offset+size, (unsigned long)BUCKET_MAX_SIZE);

    uint32_t first_block = offset / BUCKET_MAX_SIZE;
    uint32_t last_block = (offset+size) / BUCKET_MAX_SIZE;
    uint32_t block;
    size_t buf_offset = 0;
    for (block = first_block; block <= last_block; block++) {
        off_t block_offset;
        size_t block_size;

        if (block == first_block) {
            block_offset = offset - block * BUCKET_MAX_SIZE;
        } else {
            block_offset = 0;
        }

        if (block == last_block) {
            block_size = (offset + size) - (block * BUCKET_MAX_SIZE);
        } else {
            block_size = BUCKET_MAX_SIZE - block_offset;
        }

        fprintf(stderr, "reading block %lu, 0x%lx to 0x%lx\n",
                (unsigned long) block, block_offset, block_offset + block_size);

        int result = cache_fetch(path, block, block_offset, 
                rbuf + buf_offset, block_size);
        if (result == -1) {
            if (errno == ENOENT) {
                // not an error
            } else {
                perror("read from cache failed");
            }
            // need to do a real read
            //TODO
        }
        buf_offset += block_size;
    }

    return size;
}

int backfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
        off_t offset, struct fuse_file_info *fi)
{
    //TODO
    fprintf(stderr, "readdir %s\n", path);
    if (strcmp(path, "/") != 0)
        return -ENOENT;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    filler(buf, "one.txt", NULL, 0);
    filler(buf, "two.txt", NULL, 0);

    return 0;
}

static struct fuse_operations BackFS_Opers = {
    .open       = backfs_open,
    .read       = backfs_read,
    .readdir    = backfs_readdir,
    .getattr    = backfs_getattr
};

int main(int argc, char **argv)
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct statvfs cachedir_statvfs;

    if (fuse_opt_parse(&args, &backfs, backfs_opts, 0) == -1) {
        fprintf(stderr, "fuse_opt_parse failed\n");
        return 1;
    }

    if (backfs.cache_dir == NULL) {
        fprintf(stderr, "error: you need to specify a cache location with \"-o cache\"\n");
        return -1;
    }

    if (statvfs(backfs.cache_dir, &cachedir_statvfs) == -1) {
        perror("error checking cache dir");
        return 2;
    }

    uint64_t device_size = (uint64_t)(cachedir_statvfs.f_bsize * cachedir_statvfs.f_blocks);
    
    if (device_size < backfs.cache_size) {
        fprintf(stderr, "error: specified cache size larger than device\ndevice is %llu bytes, but %llu bytes were specified.\n",
                (unsigned long long) device_size, backfs.cache_size);
        return -1;
    }

    bool use_whole_device = false;
    if (backfs.cache_size == 0) {
        use_whole_device = true;
        backfs.cache_size = device_size;
    }

    if (backfs.cache_size < BUCKET_MAX_SIZE) {
        fprintf(stderr, "error: refusing to use cache of size less than %llu bytes\n",
                (unsigned long long) BUCKET_MAX_SIZE);
        return -1;
    }

    double cache_human = (double)(backfs.cache_size);
    char *cache_units;
    if (backfs.cache_size >= 1024 * 1024 * 1024) {
        cache_human /= 1024*1024*1024;
        cache_units = "GiB";
    } else if (backfs.cache_size >= 1024 * 1024) {
        cache_human /= 1024*1024;
        cache_units = "MiB";
    } else if (backfs.cache_size >= 1024) {
        cache_human /= 1024;
        cache_units = "KiB";
    } else {
        cache_units = "B";
    }

    printf("cache size %.2lf %s\n"
        , cache_human
        , cache_units
    );

    cache_init(backfs.cache_dir, backfs.cache_size, use_whole_device);

    printf("%s %s\n", args.argv[1], args.argv[2]);

    fuse_main(args.argc, args.argv, &BackFS_Opers);

    return 0;
}
