// this needs to be first
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

#include "cache.h"

struct backfs { char *cache_dir; };
static struct backfs backfs;
#define BACKFS_OPT(t,p,v) { t, offsetof(struct 
static struct fuse_opt backfs_opts[] = {
    {"cache=%s", offsetof(struct backfs, cache_dir), 0}
};

int backfs_open_common(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    //TODO
    return 0;
}

int backfs_open(const char *path, struct fuse_file_info *fi)
{
    return backfs_open_common(path, 0, fi);
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
        }
        buf_offset += block_size;
    }

    return 0;
}

static struct fuse_operations BackFS_Opers = {
    .open       = backfs_open,
    .read       = backfs_read,
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
        fprintf(stderr, "you need to specify a cache location with \"-o cache\"\n");
        return -1;
    }

    if (statvfs(backfs.cache_dir, &cachedir_statvfs) == -1) {
        perror("error checking cache dir");
        return 2;
    }

    uint64_t cache_size = (uint64_t)(cachedir_statvfs.f_bsize * cachedir_statvfs.f_blocks);

    double cache_human = (double)cache_size;
    char *cache_units;
    if (cache_size > 1024 * 1024 * 1024) {
        cache_human /= 1024*1024*1024;
        cache_units = "GiB";
    } else if (cache_size > 1024 * 1024) {
        cache_human /= 1024*1024;
        cache_units = "MiB";
    } else if (cache_size > 1024) {
        cache_human /= 1024;
        cache_units = "KiB";
    } else {
        cache_units = "B";
    }

    printf("cache size %.2lf %s\n"
        , cache_human
        , cache_units
    );

    cache_init(backfs.cache_dir, cache_size);

    //fuse_main(argc, argv, &BackFS_Opers, NULL);

    return 0;
}
