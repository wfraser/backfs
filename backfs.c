/*
 * BackFS
 * Copyright (c) 2010-2011 William R. Fraser
 */

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
#include <dirent.h>

#include <sys/statvfs.h>

#include <pthread.h>

#if FUSE_USE_VERSION > 25
#define backfs_fuse_main(argc, argv, opers) fuse_main(argc,argv,opers,NULL)
#else
#define backfs_fuse_main fuse_main
#endif

#include "fscache.h"

struct backfs { 
    char *cache_dir;
    char *real_root;
    unsigned long long cache_size;
    unsigned long long block_size;
    pthread_mutex_t lock;
};

enum {
    KEY_HELP,
    KEY_VERSION,
};

static struct backfs backfs;
#define BACKFS_OPT(t,p,v) { t, offsetof(struct 
static struct fuse_opt backfs_opts[] = {
    {"cache=%s",        offsetof(struct backfs, cache_dir),     0},
    {"cache_size=%llu", offsetof(struct backfs, cache_size),    0},
    {"backing_fs=%s",   offsetof(struct backfs, real_root),     0},
    {"block_size=%llu", offsetof(struct backfs, block_size),    0},
    FUSE_OPT_KEY("-h",          KEY_HELP),
    FUSE_OPT_KEY("--help",      KEY_HELP),
    FUSE_OPT_KEY("-V",          KEY_VERSION),
    FUSE_OPT_KEY("--version",   KEY_VERSION),
    FUSE_OPT_END
};

void usage()
{
    fprintf(stderr, 
        "usage: backfs [-o <options>] <mount point>\n"
        "\n"
        "BackFS options:\n"
        "    -o cache               cache location (REQUIRED)\n"
        "    -o backing_fs          backing filesystem location (REQUIRED)\n"
        "    -o cache_size          maximum size for the cache (0)\n"
        "                           (default is for cache to grow to fill the device\n"
        "                              it is on)\n"
        "\n"
    );
}

int backfs_open(const char *path, struct fuse_file_info *fi)
{
    fprintf(stderr, "BackFS: open %s\n", path);

    if (strcmp("/.backfs_control", path) == 0) {
        if ((fi->flags & 3) != O_WRONLY)
            return -EACCES;
        else
            return 0;
    }

    char real[PATH_MAX];
    snprintf(real, PATH_MAX, "%s%s", backfs.real_root, path);
    struct stat stbuf;
    int ret = lstat(real, &stbuf);
    if (ret == -1) {
        perror("BackFS: lstat returned -1");
        return -errno;
    }

    if ((fi->flags & 3) != O_RDONLY) {
        return -EACCES;
    }

    return 0;
}

int backfs_write(const char *path, const char *buf, size_t len, off_t offset,
        struct fuse_file_info *fi)
{
    if (strcmp(path, "/.backfs_control") != 0) {
        return -EACCES;
    }

    char *data = (char*)malloc(len+1);
    memcpy(data, buf, len);
    data[len] = '\0';

    char command[20];
    char *c = command;
    while (*data != ' ' && *data != '\n' && *data != '\0') {
        *c = *data;
        c++;
        data++;
    }

    *c = '\0';
    if (*data == ' ' || *data == '\n')
        data++;

    fprintf(stderr, "BackFS: command(%s) data(%s)\n", command, data);

    if (strcmp(command, "test") == 0) {
        // nonsensical error "Cross-device link"
        return -EXDEV;
    } else if (strcmp(command, "invalidate") == 0) {
        cache_delete(data);
    } else if (strcmp(command, "noop") == 0) {
        // test command; do nothing
    } else {
        return -EBADMSG;
    }

    return len;
}

int backfs_getattr(const char *path, struct stat *stbuf)
{
    fprintf(stderr, "BackFS: getattr %s\n", path);

    if (strcmp(path, "/.backfs_control") == 0) {
        memset(stbuf, 0, sizeof(struct stat));
        stbuf->st_mode = S_IFREG | 0200;
        stbuf->st_nlink = 1;
        stbuf->st_size = 0;
        stbuf->st_uid = 0;
        stbuf->st_gid = 0;
        stbuf->st_atime = 0;
        stbuf->st_mtime = 0;
        stbuf->st_ctime = 0;
        return 0;
    }

    char real[PATH_MAX];
    snprintf(real, PATH_MAX, "%s%s", backfs.real_root, path);
    int ret = lstat(real, stbuf);
    
    // no write or exec perms
    stbuf->st_mode &= ~0333;

    if (ret == -1) {
        perror("BackFS: lstat returned -1");
        return -errno;
    } else {
        fprintf(stderr, "BackFS: mode: %o\n", stbuf->st_mode);
        return 0;
    }
}

int backfs_read(const char *path, char *rbuf, size_t size, off_t offset,
        struct fuse_file_info *fi)
{
    // for debug output
    bool first = true;

    int bytes_read = 0;
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
            block_size = (offset+size) - (block*BUCKET_MAX_SIZE) - block_offset;
        } else {
            block_size = BUCKET_MAX_SIZE - block_offset;
        }

        // in case another thread is reading a full block as a result of a 
        // cache miss
        pthread_mutex_lock(&backfs.lock);

        if (first) {
            fprintf(stderr, "BackFS: reading from 0x%lx to 0x%lx, block size is 0x%lx\n",
                    offset, offset+size, (unsigned long)BUCKET_MAX_SIZE);
            first = false;
        }

        fprintf(stderr, "BackFS: reading block %lu, 0x%lx to 0x%lx\n",
                (unsigned long) block, block_offset, block_offset + block_size);

        uint64_t bread = 0;
        int result = cache_fetch(path, block, block_offset, 
                rbuf + buf_offset, block_size, &bread);
        if (result == -1) {
            if (errno == ENOENT) {
                // not an error
            } else {
                perror("BackFS ERROR: read from cache failed");
                pthread_mutex_unlock(&backfs.lock);
                return -EIO;
            }

            //
            // need to do a real read
            //

            char real[PATH_MAX];
            snprintf(real, PATH_MAX, "%s%s", backfs.real_root, path);
            fprintf(stderr, "BackFS: reading block %lu from real file: %s\n",
                    (unsigned long) block, real);
            int fd = open(real, O_RDONLY);
            if (fd == -1) {
                perror("BackFS ERROR: error opening real file");
                pthread_mutex_unlock(&backfs.lock);
                return -EIO;
            }
            
            // read the entire block
            char *block_buf = (char*)malloc(BUCKET_MAX_SIZE);
            int nread = pread(fd, block_buf, BUCKET_MAX_SIZE,
                    BUCKET_MAX_SIZE * block);
            if (nread == -1) {
                perror("BackFS ERROR: read error on real file");
                pthread_mutex_unlock(&backfs.lock);
                return -EIO;
            } else {
                fprintf(stderr, "BackFS: got %lu bytes from real file\n",
                        (unsigned long) nread);
                fprintf(stderr, "BackFS: adding to cache\n");
                cache_add(path, block, block_buf, nread);

                pthread_mutex_unlock(&backfs.lock);

                memcpy(rbuf+buf_offset, block_buf+block_offset, 
                        ((nread < block_size) ? nread : block_size));

                free(block_buf);

                if (nread < block_size) {
                    fprintf(stderr, "BackFS: read less than requested, %lu instead of %lu\n", 
                            (unsigned long) nread, (unsigned long) block_size);
                    bytes_read += nread;
                    fprintf(stderr, "BackFS: bytes_read=%lu\n", 
                            (unsigned long) bytes_read);
                    return bytes_read;
                } else {
                    fprintf(stderr, "BackFS: %lu bytes for fuse buffer\n",
                            block_size);
                    bytes_read += block_size;
                    fprintf(stderr, "BackFS: bytes_read=%lu\n", 
                            (unsigned long) bytes_read);
                }
            }
        } else {
            pthread_mutex_unlock(&backfs.lock);
            fprintf(stderr, "BackFS: got %lu bytes from cache\n",
                    (unsigned long) bread);
            bytes_read += bread;
            fprintf(stderr, "BackFS: bytes_read=%lu\n",
                    (unsigned long) bytes_read);

            if (bread < block_size) {
                // must have read the end of file
                fprintf(stderr, "BackFS: fewer than requested\n");
                return bytes_read;
            }
        }
        buf_offset += block_size;
    }

    return bytes_read;
}

int backfs_opendir(const char *path, struct fuse_file_info *fi)
{
    fprintf(stderr, "BackFS: opendir %s\n", path);

    char real[PATH_MAX];
    snprintf(real, PATH_MAX, "%s%s", backfs.real_root, path);

    DIR *dir = opendir(real);

    if (dir == NULL) {
        perror("BackFS: opendir failed");
        return -errno;
    }

    fi->fh = (uint64_t) dir;

    return 0;
}

int backfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
        off_t offset, struct fuse_file_info *fi)
{
    fprintf(stderr, "BackFS: readdir %s\n", path);

    DIR *dir = (DIR*)(fi->fh);

    if (dir == NULL) {
        fprintf(stderr, "BackFS: got null dir handle");
        return -EBADF;
    }

    char real[PATH_MAX];
    snprintf(real, PATH_MAX, "%s%s", backfs.real_root, path);

    // fs control handle
    if (strcmp("/", path) == 0) {
        filler(buf, ".backfs_control", NULL, 0);
    }

    int res;
    struct dirent *entry = malloc(offsetof(struct dirent, d_name) + pathconf(real, _PC_NAME_MAX) + 1);
    struct dirent *rp;
    while ((res = readdir_r(dir, entry, &rp) == 0) && (rp != NULL)) {
        filler(buf, rp->d_name, NULL, 0);
    }
    free(entry);

    return 0;
}

int backfs_access(const char *path, int mode)
{
    if (mode & W_OK) {
        return -EACCES;
    }

    return 0;
}

static struct fuse_operations BackFS_Opers = {
    .open       = backfs_open,
    .read       = backfs_read,
    .opendir    = backfs_opendir,
    .readdir    = backfs_readdir,
    .getattr    = backfs_getattr,
    .access     = backfs_access,
    .write      = backfs_write
};

int backfs_opt_proc(void *data, const char *arg, int key, 
        struct fuse_args *outargs)
{
    switch (key) {
    case FUSE_OPT_KEY_OPT:
        return 1;

    case FUSE_OPT_KEY_NONOPT:
        return 1;
    
    case KEY_HELP:
        fuse_opt_add_arg(outargs, "-ho");
        usage();
        backfs_fuse_main(outargs->argc, outargs->argv, &BackFS_Opers);
        exit(1);

    case KEY_VERSION:
        fprintf(stderr, "BackFS: BackFS\n");
        fuse_opt_add_arg(outargs, "--version");
        backfs_fuse_main(outargs->argc, outargs->argv, &BackFS_Opers);
        exit(0);
        
    default:
        fprintf(stderr, "BackFS: argument parsing error\n");
        abort();
    }
}

int main(int argc, char **argv)
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct statvfs cachedir_statvfs;

    if (fuse_opt_parse(&args, &backfs, backfs_opts, backfs_opt_proc) == -1) {
        fprintf(stderr, "BackFS: fuse_opt_parse failed\n");
        return 1;
    }

    if (backfs.real_root == NULL) {
        fprintf(stderr, "BackFS: error: you need to specify a backing filesystem with \"-o backing_fs\"\n");
        return -1;
    }

    DIR *d;
    if ((d = opendir(backfs.real_root)) == NULL) {
        perror("BackFS ERROR: error checking backing filesystem");
        return 2;
    }
    closedir(d);

    if (backfs.cache_dir == NULL) {
        fprintf(stderr, "BackFS: error: you need to specify a cache location with \"-o cache\"\n");
        return -1;
    }

    // TODO: move these to fscache.c?

    if (statvfs(backfs.cache_dir, &cachedir_statvfs) == -1) {
        perror("BackFS ERROR: error checking cache dir");
        return 3;
    }

    if (access(backfs.cache_dir, W_OK) == -1) {
        perror("BackFS ERROR: unable to write to cache dir");
        return 4;
    }

    char buf[PATH_MAX];
    snprintf(buf, PATH_MAX, "%s%s", backfs.cache_dir, "/buckets");
    if (mkdir(buf, 0700) == -1 && errno != EEXIST) {
        perror("BackFS ERROR: unable to create cache bucket directory");
        return 5;
    }

    snprintf(buf, PATH_MAX, "%s%s", backfs.cache_dir, "/map");
    if (mkdir(buf, 0700) == -1 && errno != EEXIST) {
        perror("BackFS ERROR: unable to create cache map directory");
        return 6;
    }

    if (backfs.block_size == 0) {
        backfs.block_size = 0x100000;    // 1 MiB
    }

    uint64_t device_size = (uint64_t)(cachedir_statvfs.f_bsize * cachedir_statvfs.f_blocks);
    
    if (device_size < backfs.cache_size) {
        fprintf(stderr, "BackFS: error: specified cache size larger than device\ndevice is %llu bytes, but %llu bytes were specified.\n",
                (unsigned long long) device_size, backfs.cache_size);
        return -1;
    }

    bool use_whole_device = false;
    if (backfs.cache_size == 0) {
        use_whole_device = true;
        backfs.cache_size = device_size;
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

    cache_init(backfs.cache_dir, backfs.cache_size, backfs.block_size);

    backfs_fuse_main(args.argc, args.argv, &BackFS_Opers);

    return 0;
}
