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

#include "backfs.h"

#if FUSE_USE_VERSION > 25
#define backfs_fuse_main(argc, argv, opers) fuse_main(argc,argv,opers,NULL)
#else
#define backfs_fuse_main fuse_main
#endif

// default cache block size: 128 KiB
#define BACKFS_DEFAULT_BLOCK_SIZE 0x20000

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
        "usage: backfs [-o <options>] <backing> <mount point>\n"
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
    INFO("BackFS: open %s\n", path);

    if (strcmp("/.backfs_control", path) == 0) {
        if ((fi->flags & 3) != O_WRONLY)
            return -EACCES;
        else
            return 0;
    }

    if (strcmp("/.backfs_version", path) == 0) {
        if ((fi->flags & 3) != O_RDONLY)
            return -EACCES;
        else
            return 0;
    }

    char real[PATH_MAX];
    snprintf(real, PATH_MAX, "%s%s", backfs.real_root, path);
    struct stat stbuf;
    int ret = lstat(real, &stbuf);
    if (ret == -1) {
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

    INFO("BackFS: command(%s) data(%s)\n", command, data);

    if (strcmp(command, "test") == 0) {
        // nonsensical error "Cross-device link"
        return -EXDEV;
    } else if (strcmp(command, "invalidate") == 0) {
        int err = cache_invalidate_file(data);
        if (err != 0)
            return err;
    } else if (strcmp(command, "free_orphans") == 0) {
        int err = cache_free_orphan_buckets();
        if (err != 0)
            return err;
    } else if (strcmp(command, "noop") == 0) {
        // test command; do nothing
    } else {
        return -EBADMSG;
    }

    return len;
}

int backfs_getattr(const char *path, struct stat *stbuf)
{
    INFO("BackFS: getattr %s\n", path);

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

    if (strcmp(path, "/.backfs_version") == 0) {
        memset(stbuf, 0, sizeof(struct stat));
        stbuf->st_mode = S_IFREG | 0444;
        stbuf->st_nlink = 1;
        stbuf->st_size = strlen(BACKFS_VERSION);
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
        return -errno;
    } else {
        INFO("BackFS: mode: %o\n", stbuf->st_mode);
        return 0;
    }
}

int backfs_read(const char *path, char *rbuf, size_t size, off_t offset,
        struct fuse_file_info *fi)
{
    if (strcmp(path, "/.backfs_version") == 0) {
        char *ver = BACKFS_VERSION;
        size_t len = strlen(ver);

        if (offset > len) {
            return 0;
        }

        int bytes_out = ((len - offset) > size) ? size : (len - offset);

        memcpy(rbuf, ver+offset, bytes_out);

        return bytes_out;
    }

    // for debug output
    bool first = true;

    int bytes_read = 0;
    uint32_t first_block = offset / backfs.block_size;
    uint32_t last_block = (offset+size) / backfs.block_size;
    uint32_t block;
    size_t buf_offset = 0;
    for (block = first_block; block <= last_block; block++) {
        off_t block_offset;
        size_t block_size;
        
        if (block == first_block) {
            block_offset = offset - block * backfs.block_size;
        } else {
            block_offset = 0;
        }

        if (block == last_block) {
            block_size = (offset+size) - (block * backfs.block_size) - block_offset;
        } else {
            block_size = backfs.block_size - block_offset;
        }
		
        if (block_size == 0)
            continue;

        // in case another thread is reading a full block as a result of a 
        // cache miss
        pthread_mutex_lock(&backfs.lock);

        if (first) {
            INFO("reading from 0x%lx to 0x%lx, block size is 0x%lx\n",
                    offset, offset+size, (unsigned long) backfs.block_size);
            first = false;
        }

        INFO("reading block %lu, 0x%lx to 0x%lx\n",
                (unsigned long) block, block_offset, block_offset + block_size);
                
        char real[PATH_MAX];
        snprintf(real, PATH_MAX, "%s%s", backfs.real_root, path);
        
        struct stat real_stat;
        real_stat.st_mtime = 0;
        if (stat(real, &real_stat) == -1) {
            PERROR("stat on real file failed");
            return -1 * errno;
        }

        uint64_t bread = 0;
        int result = cache_fetch(path, block, block_offset, 
                rbuf + buf_offset, block_size, &bread, real_stat.st_mtime);
        if (result == -1) {
            if (errno == ENOENT) {
                // not an error
            } else {
                PERROR("read from cache failed");
                pthread_mutex_unlock(&backfs.lock);
                return -EIO;
            }

            //
            // need to do a real read
            //

            INFO("reading block %lu from real file: %s\n",
                    (unsigned long) block, real);
            int fd = open(real, O_RDONLY);
            if (fd == -1) {
                PERROR("error opening real file");
                pthread_mutex_unlock(&backfs.lock);
                return -EIO;
            }
            
            // read the entire block
            char *block_buf = (char*)malloc(backfs.block_size);
            int nread = pread(fd, block_buf, backfs.block_size,
                    backfs.block_size * block);
            if (nread == -1) {
                PERROR("read error on real file");
                close(fd);
                pthread_mutex_unlock(&backfs.lock);
                return -EIO;
            } else {
                close(fd);
                INFO("got %lu bytes from real file\n", (unsigned long) nread);
                INFO("adding to cache\n");
                cache_add(path, block, block_buf, nread, real_stat.st_mtime);

                pthread_mutex_unlock(&backfs.lock);

                memcpy(rbuf+buf_offset, block_buf+block_offset, 
                        ((nread < block_size) ? nread : block_size));

                free(block_buf);

                if (nread < block_size) {
                    INFO("read less than requested, %lu instead of %lu\n", 
                            (unsigned long) nread, (unsigned long) block_size);
                    bytes_read += nread;
                    INFO("bytes_read=%lu\n", 
                            (unsigned long) bytes_read);
                    return bytes_read;
                } else {
                    INFO("%lu bytes for fuse buffer\n", block_size);
                    bytes_read += block_size;
                    INFO("bytes_read=%lu\n", (unsigned long) bytes_read);
                }
            }
        } else {
            pthread_mutex_unlock(&backfs.lock);
            INFO("got %lu bytes from cache\n", (unsigned long) bread);
            bytes_read += bread;
            INFO("bytes_read=%lu\n", (unsigned long) bytes_read);

            if (bread < block_size) {
                // must have read the end of file
                INFO("fewer than requested\n");
                return bytes_read;
            }
        }
        buf_offset += block_size;
    }

    return bytes_read;
}

int backfs_opendir(const char *path, struct fuse_file_info *fi)
{
    INFO("opendir %s\n", path);

    char real[PATH_MAX];
    snprintf(real, PATH_MAX, "%s%s", backfs.real_root, path);

    DIR *dir = opendir(real);

    if (dir == NULL) {
        PERROR("opendir failed");
        return -errno;
    }

    fi->fh = (uint64_t) dir;

    return 0;
}

int backfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
        off_t offset, struct fuse_file_info *fi)
{
    INFO("readdir %s\n", path);

    DIR *dir = (DIR*)(fi->fh);

    if (dir == NULL) {
        ERROR("got null dir handle");
        return -EBADF;
    }

    char real[PATH_MAX];
    snprintf(real, PATH_MAX, "%s%s", backfs.real_root, path);

    // fs control handle
    if (strcmp("/", path) == 0) {
        filler(buf, ".backfs_control", NULL, 0);
        filler(buf, ".backfs_version", NULL, 0);
    }

    int res;
    struct dirent *entry = malloc(offsetof(struct dirent, d_name) + pathconf(real, _PC_NAME_MAX) + 1);
    struct dirent *rp;
    while ((res = readdir_r(dir, entry, &rp) == 0) && (rp != NULL)) {
        filler(buf, rp->d_name, NULL, 0);
    }
    free(entry);

    closedir(dir);

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

    char cwd[PATH_MAX];
    getcwd(cwd, PATH_MAX);
    fprintf(stderr, "cwd: %s\n", cwd);

    if (fuse_opt_parse(&args, &backfs, backfs_opts, backfs_opt_proc) == -1) {
        fprintf(stderr, "BackFS: fuse_opt_parse failed\n");
        return 1;
    }

    if (backfs.real_root == NULL) {
        if ((strcmp(args.argv[1], "-o") == 0) ? args.argc != 5 : args.argc != 3) {
            fprintf(stderr, "BackFS: error: you need to specify a backing filesystem.\n");
            usage();
            fuse_opt_add_arg(&args, "-ho");
            backfs_fuse_main(args.argc, args.argv, &BackFS_Opers);
            return -1;
        } else {
            backfs.real_root = args.argv[ args.argc - 2 ];
            args.argv[ args.argc - 2 ] = args.argv[ args.argc - 1 ];
            args.argc--;
        }
    }

    if (backfs.real_root[0] != '/') {
        char *rel = backfs.real_root;

        backfs.real_root = (char*)malloc(strlen(cwd)+strlen(rel)+2);
        sprintf(backfs.real_root, "%s/%s", cwd, rel);
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

    if (backfs.cache_dir[0] != '/') {
        char *rel = backfs.cache_dir;

        backfs.cache_dir = (char*)malloc(strlen(cwd)+strlen(rel)+2);
        sprintf(backfs.cache_dir, "%s/%s", cwd, rel);
    }

#ifdef SYSLOG
    openlog("BackFS", 0, LOG_USER);
#endif

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
    snprintf(buf, PATH_MAX, "%s/buckets", backfs.cache_dir);
    if (mkdir(buf, 0700) == -1 && errno != EEXIST) {
        perror("BackFS ERROR: unable to create cache bucket directory");
        return 5;
    }

    snprintf(buf, PATH_MAX, "%s/map", backfs.cache_dir);
    if (mkdir(buf, 0700) == -1 && errno != EEXIST) {
        perror("BackFS ERROR: unable to create cache map directory");
        return 6;
    }
	
    unsigned long long cache_block_size = 0;
    snprintf(buf, PATH_MAX, "%s/buckets/bucket_size", backfs.cache_dir);
    bool has_block_size_marker = false;
    FILE *f = fopen(buf, "r");
    if (f == NULL) {
    if (errno != ENOENT) {
            perror("BackFS ERROR: unable to open cache block size marker");
            return 7;
        }
    } else {
        if (fscanf(f, "%llu", &cache_block_size) != 1) {
            perror("BackFS ERROR: unable to read cache block size marker");
            return 8;
        }
        has_block_size_marker = true;
		
        if (backfs.block_size == 0) {
            backfs.block_size = cache_block_size;
            fprintf(stderr, "BackFS: using previous cache block size of %llu\n", cache_block_size);
        } else if (backfs.block_size != cache_block_size) {
            fprintf(stderr, "BackFS ERROR: cache was made using different block size of %llu. Unable to use specified size of %llu\n",
                    cache_block_size, backfs.block_size);
            return 9;
        }
        fclose(f);
        f = NULL;
    }

    if (backfs.block_size == 0) {
        backfs.block_size = BACKFS_DEFAULT_BLOCK_SIZE;
    }
	
    if (!has_block_size_marker) {
        f = fopen(buf, "w");
        if (f == NULL) {
            perror("BackFS ERROR: unable to open cache block size marker");
            return 10;
        }
        fprintf(f, "%llu\n", backfs.block_size);
        fclose(f);
        f = NULL;
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

    printf("block size %llu bytes\n", backfs.block_size);

    printf("initializing cache and scanning existing cache dir...\n");
    cache_init(backfs.cache_dir, backfs.cache_size, backfs.block_size);

    printf("ready to go!\n");
    backfs_fuse_main(args.argc, args.argv, &BackFS_Opers);

    return 0;
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
