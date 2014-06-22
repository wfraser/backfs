/*
 * BackFS
 * Copyright (c) 2010-2014 William R. Fraser
 */

// this needs to be first
#include <fuse.h>
#include <fuse_opt.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <string.h>

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

#include <sys/statvfs.h>

#include <pthread.h>

#include "global.h"
#include "fscache.h"
#include "util.h"

#if FUSE_USE_VERSION > 25
#define backfs_fuse_main(argc, argv, opers) fuse_main(argc,argv,opers,NULL)
#else
#define backfs_fuse_main fuse_main
#endif

// default cache block size: 128 KiB
#define BACKFS_DEFAULT_BLOCK_SIZE 0x20000

struct backfs { 
    char *cache_dir;
    char *real_root;
    unsigned long long cache_size;
    unsigned long long block_size;
    bool rw;
    pthread_mutex_t lock;
};
static struct backfs backfs = {0};

int backfs_log_level;
bool backfs_log_stderr = false;

#define FREE(var) { free(var); var = NULL; }

void usage()
{
    fprintf(stderr, 
        "usage: backfs [-o <options>] <backing> <mount point>\n"
        "\n"
        "BackFS options:\n"
        "    -o cache               cache location (REQUIRED)\n"
        "    -o backing_fs          backing filesystem location (REQUIRED here or\n"
        "                               as the first non-option argument)\n"
        "    -o cache_size          maximum size for the cache (0)\n"
        "                           (default is for cache to grow to fill the device\n"
        "                              it is on)\n"
#ifdef BACKFS_RW
        "    -o rw                  be a read-write cache (default is read-only)\n"
#endif
        "    -o block_size          cache block size. defaults to 128K\n"
        "    -v --verbose           Enable informational messages.\n"
        "       -o verbose\n"
        "    -d --debug -o debug    Enable debugging mode. BackFS will not fork to\n"
        "                           background and enables all debugging messages.\n"
        "\n"
    );
}

const char BACKFS_CONTROL_FILE[] = "/.backfs_control";
const char BACKFS_VERSION_FILE[] = "/.backfs_version";

#define REALPATH(real, path) \
    do { \
        if (-1 == asprintf(&real, "%s%s", backfs.real_root, path)) { \
            ret = -ENOMEM; \
            goto exit; \
        } \
    } while (0)

int backfs_control_file_write(const char *path, const char *buf, size_t len, off_t offset,
        struct fuse_file_info *fi)
{
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

    DEBUG("BackFS: command(%s) data(%s)\n", command, data);

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

int backfs_open(const char *path, struct fuse_file_info *fi)
{
    DEBUG("BackFS: open %s\n", path);
    int ret = 0;
    char *real = NULL;

    if (strcmp(BACKFS_CONTROL_FILE, path) == 0) {
        if ((fi->flags & 3) != O_WRONLY)
            ret = -EACCES;
        goto exit;
    }

    if (strcmp(BACKFS_VERSION_FILE, path) == 0) {
        if ((fi->flags & 3) != O_RDONLY)
            ret = -EACCES;
        goto exit;
    }

    REALPATH(real, path);
    struct stat stbuf;
    ret = lstat(real, &stbuf);
    FREE(real);
    if (ret == -1) {
        ret = -errno;
        goto exit;
    }

    if (!backfs.rw && (fi->flags & 3) != O_RDONLY) {
        ret = -EACCES;
        goto exit;
    }

exit:
    FREE(real);
    return ret;
}

int backfs_write(const char *path, const char *buf, size_t len, off_t offset,
        struct fuse_file_info *fi)
{
    if (strcmp(path, BACKFS_CONTROL_FILE) == 0) {
        return backfs_control_file_write(path, buf, len, offset, fi);
    }

    if (!backfs.rw) {
        return -EACCES;
    }

    //TODO
    return -EIO;
}

int backfs_readlink(const char *path, char *buf, size_t bufsize)
{
    int ret = 0;
    char *real = NULL;

    REALPATH(real, path);

    ssize_t bytes_written = readlink(real, buf, bufsize);
    if (bytes_written == -1)
    {
        ret = -errno;
        goto exit;
    }

exit:
    FREE(real);
    return ret;
}

int backfs_getattr(const char *path, struct stat *stbuf)
{
    DEBUG("BackFS: getattr %s\n", path);
    int ret = 0;
    char *real = NULL;

    if (strcmp(path, BACKFS_CONTROL_FILE) == 0) {
        memset(stbuf, 0, sizeof(struct stat));
        stbuf->st_mode = S_IFREG | 0200;
        stbuf->st_nlink = 1;
        stbuf->st_size = 0;
        stbuf->st_uid = 0;
        stbuf->st_gid = 0;
        stbuf->st_atime = 0;
        stbuf->st_mtime = 0;
        stbuf->st_ctime = 0;
        goto exit;
    }

    if (strcmp(path, BACKFS_VERSION_FILE) == 0) {
        memset(stbuf, 0, sizeof(struct stat));
        stbuf->st_mode = S_IFREG | 0444;
        stbuf->st_nlink = 1;
        stbuf->st_size = strlen(BACKFS_VERSION);
        stbuf->st_uid = 0;
        stbuf->st_gid = 0;
        stbuf->st_atime = 0;
        stbuf->st_mtime = 0;
        stbuf->st_ctime = 0;
        goto exit;
    }

    REALPATH(real, path);
    ret = lstat(real, stbuf);
    
    // no write or exec perms
    stbuf->st_mode &= ~0333;

    if (ret == -1) {
        ret = -errno;
    } else {
        DEBUG("BackFS: mode: %o\n", stbuf->st_mode);
        ret = 0;
    }

exit:
    FREE(real);
    return ret;
}

int backfs_read(const char *path, char *rbuf, size_t size, off_t offset,
        struct fuse_file_info *fi)
{
    int ret = 0;
    bool locked = false;
    char *block_buf = NULL;
    char *real = NULL;

    if (strcmp(path, BACKFS_VERSION_FILE) == 0) {
        char *ver = BACKFS_VERSION;
        size_t len = strlen(ver);

        if (offset > len) {
            goto exit;
        }

        int bytes_out = ((len - offset) > size) ? size : (len - offset);

        memcpy(rbuf, ver+offset, bytes_out);

        ret = bytes_out;
        goto exit;
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
        locked = true;

        if (first) {
            DEBUG("reading from 0x%lx to 0x%lx, block size is 0x%lx\n",
                    (unsigned long) offset,
                    (unsigned long) offset+size,
                    (unsigned long) backfs.block_size);
            first = false;
        }

        DEBUG("reading block %lu, 0x%lx to 0x%lx\n",
                (unsigned long) block,
                (unsigned long) block_offset,
                (unsigned long) block_offset + block_size);
                
        REALPATH(real, path);
        
        struct stat real_stat;
        real_stat.st_mtime = 0;
        if (stat(real, &real_stat) == -1) {
            PERROR("stat on real file failed");
            ret = -1 * errno;
            goto exit;
        }

        uint64_t bread = 0;
        int result = cache_fetch(path, block, block_offset, 
                rbuf + buf_offset, block_size, &bread, real_stat.st_mtime);
        if (result == -1) {
            if (errno == ENOENT) {
                // not an error
            } else {
                PERROR("read from cache failed");
                ret = -EIO;
                goto exit;
            }

            //
            // need to do a real read
            //

            DEBUG("reading block %lu from real file: %s\n",
                    (unsigned long) block, real);
            int fd = open(real, O_RDONLY);
            if (fd == -1) {
                PERROR("error opening real file");
                ret = -EIO;
                goto exit;
            }
            
            // read the entire block
            block_buf = (char*)malloc(backfs.block_size);
            int nread = pread(fd, block_buf, backfs.block_size,
                    backfs.block_size * block);
            if (nread == -1) {
                PERROR("read error on real file");
                close(fd);
                ret = -EIO;
                goto exit;
            } else {
                close(fd);
                DEBUG("got %lu bytes from real file\n", (unsigned long) nread);
                DEBUG("adding to cache\n");
                cache_add(path, block, block_buf, nread, real_stat.st_mtime);

                memcpy(rbuf+buf_offset, block_buf+block_offset, 
                        ((nread < block_size) ? nread : block_size));

                if (nread < block_size) {
                    DEBUG("read less than requested, %lu instead of %lu\n", 
                            (unsigned long) nread, (unsigned long) block_size);
                    bytes_read += nread;
                    DEBUG("bytes_read=%lu\n", 
                            (unsigned long) bytes_read);
                    ret = bytes_read;
                    goto exit;
                } else {
                    DEBUG("%lu bytes for fuse buffer\n", 
                            (unsigned long) block_size);
                    bytes_read += block_size;
                    DEBUG("bytes_read=%lu\n", (unsigned long) bytes_read);
                }
            }
        } else {
            DEBUG("got %lu bytes from cache\n", (unsigned long) bread);
            bytes_read += bread;
            DEBUG("bytes_read=%lu\n", (unsigned long) bytes_read);

            if (bread < block_size) {
                // must have read the end of file
                DEBUG("fewer than requested\n");
                ret = bytes_read;
                goto exit;
            }
        }
        buf_offset += block_size;
    }

    ret = bytes_read;

exit:
    if (locked) {
        pthread_mutex_unlock(&backfs.lock);
    }
    FREE(real);
    FREE(block_buf);
    return ret;
}

int backfs_opendir(const char *path, struct fuse_file_info *fi)
{
    DEBUG("opendir %s\n", path);
    int ret = 0;
    char *real = NULL;
    REALPATH(real, path);

    DIR *dir = opendir(real);

    if (dir == NULL) {
        PERROR("opendir failed");
        ret = -errno;
        goto exit;
    }

    fi->fh = (uint64_t)(long)dir;

exit:
    FREE(real);
    return ret;
}

int backfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
        off_t offset, struct fuse_file_info *fi)
{
    DEBUG("readdir %s\n", path);
    int ret = 0;
    char *real = NULL;

    DIR *dir = (DIR*)(long)(fi->fh);

    if (dir == NULL) {
        ERROR("got null dir handle");
        ret = -EBADF;
        goto exit;
    }

    REALPATH(real, path);

    // fs control handle
    if (strcmp("/", path) == 0) {
        filler(buf, ".backfs_control", NULL, 0);
        filler(buf, ".backfs_version", NULL, 0);
    }

    int res;
    struct dirent *entry = malloc(offsetof(struct dirent, d_name) + max_filename_length(real) + 1);
    if (entry == NULL) {
        ret = -ENOMEM;
        goto exit;
    }
    struct dirent *rp;
    while ((res = readdir_r(dir, entry, &rp) == 0) && (rp != NULL)) {
        filler(buf, rp->d_name, NULL, 0);
    }
    free(entry);

    closedir(dir);

exit:
    FREE(real);
    return 0;
}

int backfs_access(const char *path, int mode)
{
    if (mode & W_OK) {
        return -EACCES;
    }

    return 0;
}

int backfs_truncate(const char *path, off_t offset)
{
    DEBUG("truncate %s, %u\n", path, offset);

    if (strcmp(path, BACKFS_CONTROL_FILE) == 0) {
        // Probably due to user doing 'echo foo > .backfs_control' instead of using '>>'.
        // Ignore it.
        return 0;
    }

    return -EACCES;
}

#define STUB_(func) \
int backfs_##func(const char *path) \
{ \
    DEBUG(#func ": %s\n", path); \
    return -ENOSYS; \
}

#define STUB(func, ...) \
int backfs_##func(const char *path, __VA_ARGS__) \
{ \
    DEBUG(#func ": %s\n", path); \
    return -ENOSYS; \
}

STUB(mkdir, mode_t mode)
STUB_(unlink)
STUB_(rmdir)
STUB(symlink, const char *other)
STUB(rename, const char *path_new)
STUB(link, const char *other)
STUB(chmod, mode_t mode)
STUB(chown, uid_t uid, gid_t gid)
STUB(statfs, struct statvfs *stat)
STUB(flush, struct fuse_file_info *ffi)
STUB(fsync, int n, struct fuse_file_info *ffi)
STUB(setxattr, const char *a, const char *b, size_t c, int d)
STUB(getxattr, const char *a, char *b, size_t c)
STUB(listxattr, char *a, size_t b)
STUB(removexattr, const char *a)
STUB(fsyncdir, int a, struct fuse_file_info *ffi)
STUB(create, mode_t mode, struct fuse_file_info *ffi)
STUB(lock, struct fuse_file_info *ffi, int cmd, struct flock *flock)
STUB(utimens, const struct timespec tv[2])
STUB(bmap, size_t blocksize, uint64_t *idx)
STUB(ioctl, int cmd, void *arg, struct fuse_file_info *ffi, unsigned int flags, void *data)
STUB(poll, struct fuse_file_info *ffi, struct fuse_pollhandle *ph, unsigned *reventsp)
STUB(flock, struct fuse_file_info *ffi, int op)
STUB(fallocate, int a, off_t b, off_t c, struct fuse_file_info *ffi)

#define IMPL(func) .func = backfs_##func

static struct fuse_operations BackFS_Opers = {
#ifdef BACKFS_RW
    IMPL(mkdir),
    IMPL(unlink),
    IMPL(rmdir),
    IMPL(symlink),
    IMPL(rename),
    IMPL(link),
    IMPL(chmod),
    IMPL(chown),
    IMPL(statfs),
    IMPL(flush),
    IMPL(fsync),
    IMPL(setxattr),
    IMPL(getxattr),
    IMPL(listxattr),
    IMPL(removexattr),
    IMPL(fsyncdir),
    IMPL(create),
    IMPL(lock),
    IMPL(utimens),
    IMPL(bmap),
    IMPL(ioctl),
    IMPL(poll),
    IMPL(flock),
    IMPL(fallocate),
#endif
    IMPL(open),
    IMPL(read),
    IMPL(opendir),
    IMPL(readdir),
    IMPL(getattr),
    IMPL(access),
    IMPL(write),
    IMPL(readlink),
    IMPL(truncate),
//  IMPL(release),      // not needed
//  IMPL(releasedir),   // not needed
//  IMPL(ftruncate)     // redundant, use truncate instead
//  IMPL(fgetattr),     // redundant, use getattr instead
//  IMPL(read_buf),     // use read instead
//  IMPL(write_buf),    // use write instead
};

enum {
    KEY_RW,
    KEY_VERBOSE,
    KEY_DEBUG,
    KEY_HELP,
    KEY_VERSION,
};

static struct fuse_opt backfs_opts[] = {
    {"cache=%s",        offsetof(struct backfs, cache_dir),     0},
    {"cache_size=%llu", offsetof(struct backfs, cache_size),    0},
    {"backing_fs=%s",   offsetof(struct backfs, real_root),     0},
    {"block_size=%llu", offsetof(struct backfs, block_size),    0},
    FUSE_OPT_KEY("rw",          KEY_RW),
    FUSE_OPT_KEY("verbose",     KEY_VERBOSE),
    FUSE_OPT_KEY("-v",          KEY_VERBOSE),
    FUSE_OPT_KEY("--verbose",   KEY_VERBOSE),
    FUSE_OPT_KEY("debug",       KEY_DEBUG),
    FUSE_OPT_KEY("-d",          KEY_DEBUG),
    FUSE_OPT_KEY("--debug",     KEY_DEBUG),
    FUSE_OPT_KEY("-h",          KEY_HELP),
    FUSE_OPT_KEY("--help",      KEY_HELP),
    FUSE_OPT_KEY("-V",          KEY_VERSION),
    FUSE_OPT_KEY("--version",   KEY_VERSION),
    FUSE_OPT_END
};

enum {
    FUSE_OPT_ERROR = -1,
    FUSE_OPT_DISCARD = 0,
    FUSE_OPT_KEEP = 1
};

int num_nonopt_args_read = 0;
char* nonopt_arguments[2] = {NULL, NULL};

int backfs_opt_proc(void *data, const char *arg, int key, 
        struct fuse_args *outargs)
{
    switch (key) {
    case FUSE_OPT_KEY_OPT:
        // Unknown option-argument. Pass it along to FUSE I guess?
        return FUSE_OPT_KEEP;

    case FUSE_OPT_KEY_NONOPT:
        // We take either 1 or 2 non-option arguments.
        // The last one is the mount-point. This needs to be tacked on to the outargs,
        // because FUSE handles it. But during parsing we don't know how many there are,
        // so just save them for later, and main() will fix it.
        if (num_nonopt_args_read < 2) {
            nonopt_arguments[num_nonopt_args_read++] = (char*)arg;
            return FUSE_OPT_DISCARD;
        }
        else {
            fprintf(stderr, "BackFS: too many arguments: "
                "don't know what to do with \"%s\"\n", arg);
            return FUSE_OPT_ERROR;
        }
        break;

    case KEY_RW:
#ifdef BACKFS_RW
        // Print a nasty warning to stdout
        printf("####################################\n"
               "#                                  #\n"
               "# ENABLING EXPERIMENTAL R/W MODE!! #\n"
               "#                                  #\n"
               "####################################\n");
        backfs.rw = true;
        return FUSE_OPT_DISCARD;
#else
        fprintf(stderr, "BackFS: mounting r/w is not supported in this build.\n");
        return FUSE_OPT_ERROR;
#endif

    case KEY_VERBOSE:
        backfs_log_level = LOG_LEVEL_INFO;
        return FUSE_OPT_DISCARD;

    case KEY_DEBUG:
        fuse_opt_add_arg(outargs, "-d");
        backfs_log_level = LOG_LEVEL_DEBUG;
        backfs_log_stderr = true;
        return FUSE_OPT_DISCARD;
    
    case KEY_HELP:
        fuse_opt_add_arg(outargs, "-h");
        usage();
        backfs_fuse_main(outargs->argc, outargs->argv, &BackFS_Opers);
        exit(1);

    case KEY_VERSION:
        fprintf(stderr, "BackFS: %s\n", BACKFS_VERSION);
        fuse_opt_add_arg(outargs, "--version");
        backfs_fuse_main(outargs->argc, outargs->argv, &BackFS_Opers);
        exit(0);
        
    default:
        // Shouldn't ever get here.
        fprintf(stderr, "BackFS: argument parsing error\n");
        abort();
    }
}

int main(int argc, char **argv)
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct statvfs cachedir_statvfs;

    backfs_log_level = LOG_LEVEL_WARN;

    if (fuse_opt_parse(&args, &backfs, backfs_opts, backfs_opt_proc) == -1) {
        fprintf(stderr, "BackFS: argument parsing failed.\n");
        return 1;
    }

    printf("%d read\n", num_nonopt_args_read);
    fuse_opt_add_arg(&args, nonopt_arguments[num_nonopt_args_read - 1]);
    if (num_nonopt_args_read == 2) {
        backfs.real_root = nonopt_arguments[0];
    }

#if 1
    char *cwd = getcwd(NULL, 0); // non-standard usage, Linux only
#else
    // standard POSIX, but ugly
    char cwd[PATH_MAX];
    getcwd(cwd, PATH_MAX);
#endif

    if (backfs.real_root == NULL) {
        fprintf(stderr, "BackFS: error: you need to specify a backing filesystem.\n");
        usage();
        fuse_opt_add_arg(&args, "-ho");
        backfs_fuse_main(args.argc, args.argv, &BackFS_Opers);
        return -1;
    }

    if (backfs.real_root[0] != '/') {
        const char *rel = backfs.real_root;
        asprintf(&backfs.real_root, "%s/%s", cwd, rel);
    }

    DIR *d;
    if ((d = opendir(backfs.real_root)) == NULL) {
        perror("BackFS ERROR: error checking backing filesystem");
        fprintf(stderr, "BackFS: specified as \"%s\"\n", backfs.real_root);
        return 2;
    }
    closedir(d);

    if (backfs.cache_dir == NULL) {
        fprintf(stderr, "BackFS: error: you need to specify a cache location with \"-o cache\"\n");
        return -1;
    }

    if (backfs.cache_dir[0] != '/') {
        char *rel = backfs.cache_dir;
        asprintf(&backfs.cache_dir, "%s/%s", cwd, rel);
    }

    FREE(cwd);

#ifndef NOSYSLOG
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

    char *buf = NULL;
    asprintf(&buf, "%s/buckets", backfs.cache_dir);
    if (mkdir(buf, 0700) == -1 && errno != EEXIST) {
        perror("BackFS ERROR: unable to create cache bucket directory");
        return 5;
    }
    FREE(buf);

    asprintf(&buf, "%s/map", backfs.cache_dir);
    if (mkdir(buf, 0700) == -1 && errno != EEXIST) {
        perror("BackFS ERROR: unable to create cache map directory");
        return 6;
    }
    FREE(buf);
	
    unsigned long long cache_block_size = 0;
    asprintf(&buf, "%s/buckets/bucket_size", backfs.cache_dir);
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
    FREE(buf);

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

    printf("cache size %.2lf %s%s\n"
        , cache_human
        , cache_units
        , use_whole_device ? " (using whole device)" : ""
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
