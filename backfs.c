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

/*
static struct fuse_operations BackFS_Opers = {
    .open       = backfs_open,
    .read       = backfs_read,
};
*/

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

    //fuse_main(argc, argv, &BackFS_Opers, NULL);

    return 0;
}
