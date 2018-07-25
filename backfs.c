/*
 * BackFS
 * Copyright (c) 2010-2018 William R. Fraser
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
#include <sys/stat.h>
#include <sys/xattr.h>

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

// Comment this out if you're on an older system that doesn't have this call.
#define HAVE_UTIMENS

struct backfs { 
    char *cache_dir;
    char *real_root;
    bool real_root_alloc;
    unsigned long long cache_size;
    unsigned long long block_size;
    bool rw;
    pthread_mutex_t lock;
};
static struct backfs backfs = {0};

int backfs_log_level;
bool backfs_log_stderr = false;

#define FORWARD(func, ...) \
    do { \
        ret = func(__VA_ARGS__); \
        if (ret == -1) { \
            PERROR(#func); \
            ret = -errno; \
            goto exit; \
        } \
    } while (0)

#define RW_ONLY() \
    do { \
        if (!backfs.rw) { \
            ret = -EROFS; \
            goto exit; \
        } \
    } while (0)

#define REALPATH(real, path) \
    do { \
        if (-1 == asprintf(&real, "%s%s", backfs.real_root, path)) { \
            ret = -ENOMEM; \
            goto exit; \
        } \
    } while (0)

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

int backfs_control_file_write(const char *buf, size_t len)
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

    DEBUG("backfs_control: command(%s) data(%s)\n", command, data);

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

int backfs_access(const char *path, int mode)
{
    char modestr[4] = {0};
    size_t modestr_pos = 0;

    int checkmode = 0;
    if (mode & F_OK) {
        modestr[0] = 'f';
    }
    else {
        if (mode & R_OK) {
            modestr[modestr_pos++] = 'r';
            checkmode |= 4;
        }
        if (mode & W_OK) {
            modestr[modestr_pos++] = 'w';
            checkmode |= 2;
        }
        if (mode & X_OK) {
            modestr[modestr_pos++] = 'x';
            checkmode |= 1;
        }
    }
    DEBUG("access (%s) %s\n", modestr, path);

    int ret = 0;
    char *real = NULL;

    REALPATH(real, path);
    struct stat stbuf;
    ret = lstat(real, &stbuf);
    if (ret == -1) {
        ret = -errno;
        goto exit;
    }

    DEBUG("checkmode: 0%o\n", checkmode);

    if (checkmode > 0) {
        if (!backfs.rw && (mode & W_OK)) {
            ret = -EACCES;
            goto exit;
        }

        DEBUG("fullmode: 0%o\n", stbuf.st_mode);

        struct fuse_context *ctx = fuse_get_context();

        int shift = 0;
        if (ctx->uid == stbuf.st_uid) {
            shift = 6;
        }
        else if (ctx->gid == stbuf.st_gid) {
            shift = 3;
        }
        int mode = (stbuf.st_mode & (0x7 << shift)) >> shift;

        DEBUG("mode: 0%o\n", mode);

        if ((mode & checkmode) != checkmode) {
            ret = -EACCES;
            goto exit;
        }
    }

exit:
    FREE(real);
    return ret;
}

int backfs_open(const char *path, struct fuse_file_info *fi)
{
    DEBUG("open %s\n", path);
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
    int fd = open(real, fi->flags);
    if (fd == -1) {
        PERROR("open");
        ret = -errno;
        goto exit;
    }
    
    fi->fh = fd;

exit:
    FREE(real);
    return ret;
}

int backfs_write(const char *path, const char *buf, size_t size, off_t offset,
        struct fuse_file_info *fi)
{
    DEBUG("write %s %lx %lx\n", path, size, offset);

    if (strcmp(path, BACKFS_CONTROL_FILE) == 0) {
        return backfs_control_file_write(buf, size);
    }
    else if (strcmp(path, BACKFS_VERSION_FILE) == 0) {
        return -EACCES;
    }

    if (!backfs.rw) {
        return -EACCES;
    }

    int ret = 0;
    bool locked = false;
    bool first = true;
    
    int bytes_written = 0;
    uint32_t first_block = offset / backfs.block_size;
    uint32_t last_block = (offset+size) / backfs.block_size;
    uint32_t block;
    off_t buf_offset = 0;
    for (block = first_block; block <= last_block; block++) {
        size_t block_size;

        if (block == first_block)
            block_size = ((block + 1) * backfs.block_size) - offset;
        else if (block == last_block)
            block_size = size - buf_offset;
        else
            block_size = backfs.block_size;

        if (block_size > size)
            block_size = size;
        if (block_size == 0)
            continue;

        pthread_mutex_lock(&backfs.lock);
        locked = true;

        if (first) {
            DEBUG("writing to 0x%lx to 0x%lx, block size is 0x%lx\n",
                (unsigned long)offset,
                (unsigned long)offset+size,
                (unsigned long)backfs.block_size);
            first = false;
        }
        
        DEBUG("writing block %lu, 0x%lx to 0x%lx\n",
            (unsigned long)block,
            (unsigned long)offset + buf_offset,
            (unsigned long)offset + buf_offset + block_size);

        ssize_t nwritten = pwrite((int)fi->fh, buf + buf_offset, block_size, offset + buf_offset);

        bytes_written += nwritten;
        DEBUG("bytes_written=%lu\n",(unsigned long)bytes_written);
        if (nwritten < block_size) {
            DEBUG("wrote less than requested, %lu instead of %lu\n",
                (unsigned long)nwritten,
                (unsigned long)block_size);
            ret = bytes_written;
            goto exit;
        }
        
        if (block_size == backfs.block_size) {
            // a full block, save it to the cache
            for (int loop = 0; loop < 5; loop++) {
                if (0 == cache_add(
                            path,
                            block,
                            buf + buf_offset,
                            nwritten,
                            time(NULL))
                        || errno != EAGAIN) {
                    break;
                }
                DEBUG("cache retry #%d\n", loop+1);
            }
        }
        else {
            cache_try_invalidate_block(path, block);
        }

        pthread_mutex_unlock(&backfs.lock);
        locked = false;

        buf_offset += block_size;
    }

    ret = bytes_written;

exit:
    if (locked)
        pthread_mutex_unlock(&backfs.lock);
    return ret;
}

int backfs_readlink(const char *path, char *buf, size_t bufsize)
{
    int ret = 0;
    char *real = NULL;

    REALPATH(real, path);

    ssize_t bytes_written = readlink(real, buf, bufsize-1);
    if (bytes_written == -1)
    {
        ret = -errno;
        goto exit;
    }
    buf[bytes_written] = '\0';

exit:
    FREE(real);
    return ret;
}

int backfs_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi)
{
    DEBUG("getattr %s\n", path);
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

    if (fi == NULL) {
        REALPATH(real, path);
        ret = lstat(real, stbuf);
    }
    else {
        ret = fstat(fi->fh, stbuf);
    }
    
    // no write perms
    if (!backfs.rw) {
        stbuf->st_mode &= ~0222;
    }

    if (ret == -1) {
        ret = -errno;
    } else {
        DEBUG("mode: 0%o\n", stbuf->st_mode);
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
        ret = pthread_mutex_lock(&backfs.lock);
        if (ret) {
            DEBUG("Error locking mutex: %d!", ret);
            goto exit;
        }
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
            int fd = (int)fi->fh;
            
            // read the entire block
            block_buf = (char*)malloc(backfs.block_size);
            int nread = pread(fd, block_buf, backfs.block_size,
                    backfs.block_size * block);
            if (nread == -1) {
                PERROR("read error on real file");
                ret = -EIO;
                goto exit;
            } else {
                DEBUG("got %lu bytes from real file\n", (unsigned long) nread);
                DEBUG("adding to cache\n");
                
                for (int loop = 0; loop < 5; loop++) {
                    if (0 == cache_add(
                                path,
                                block,
                                block_buf,
                                nread,
                                real_stat.st_mtime)
                            || errno != EAGAIN) {
                        break;
                    }
                    DEBUG("cache retry #%d\n", loop+1);
                }

                memcpy(rbuf+buf_offset, block_buf+block_offset, 
                        ((nread < block_size) ? nread : block_size));
                FREE(block_buf);

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

        pthread_mutex_unlock(&backfs.lock);
        locked = false;

        buf_offset += block_size;
    }

    ret = bytes_read;

exit:
    FREE(real);
    FREE(block_buf);
    if (locked) {
        pthread_mutex_unlock(&backfs.lock);
    }
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

    fi->fh = (uint64_t)(intptr_t)dir;

exit:
    FREE(real);
    return ret;
}

int backfs_releasedir(const char *path, struct fuse_file_info *fi)
{
    DEBUG("releasedir %s\n", path);
    int ret = 0;

    if (fi->fh != 0) {
        DEBUG("releasedir closing directory");
        DIR *dir = (DIR*)(intptr_t)(fi->fh);
        fi->fh = 0;
        FORWARD(closedir, dir);
    }

exit:
    return ret;
}

// FUSE3 adds a flags parameter to the directory filler function, which we don't use at all.
// for compatibility with FUSE2, define a wrapper function that doesn't take the flags parameter,
// and calls the filler function for whichever version.

static int filler_compat(
        fuse_fill_dir_t real_filler,
        void *buf,
        const char *name,
        const struct stat *stbuf,
        off_t off)
{
#ifdef FUSE3
    return real_filler(buf, name, stbuf, off, 0);
#else
    return real_filler(buf, name, stbuf, off);
#endif
}

static int backfs_readdir_internal(
        const char *path,
        void *buf,
        fuse_fill_dir_t filler,
        off_t offset,
        struct fuse_file_info *fi)
{
    DEBUG("readdir %s\n", path);

    if (offset != 0) {
        return EINVAL;
    }

    int ret = 0;
    char *real = NULL;
    DIR *dir = (DIR*)(intptr_t)(fi->fh);
    struct dirent *entry = NULL;

    if (dir == NULL) {
        ERROR("got null dir handle");
        ret = -EBADF;
        goto exit;
    }

    REALPATH(real, path);

    // fs control handle
    if (strcmp("/", path) == 0) {
        filler_compat(filler, buf, ".backfs_control", NULL, 0);
        filler_compat(filler, buf, ".backfs_version", NULL, 0);
    }

    while ((entry = readdir(dir)) != NULL) {
        filler_compat(filler, buf, entry->d_name, NULL, 0);
    }

exit:
    FREE(real);
    return ret;
}

#ifdef FUSE3
int backfs_readdir(
        const char *path,
        void *buf,
        fuse_fill_dir_t filler,
        off_t offset,
        struct fuse_file_info *fi,
        enum fuse_readdir_flags flags)
{
    (void) flags; // we don't support the FUSE_READDIR_PLUS flag
    return backfs_readdir_internal(path, buf, filler, offset, fi);
}
#else
int backfs_readdir(
        const char *path,
        void *buf,
        fuse_fill_dir_t filler,
        off_t offset,
        struct fuse_file_info *fi)
{
    return backfs_readdir_internal(path, buf, filler, offset, fi);
}
#endif

int backfs_truncate(const char *path, off_t length, struct fuse_file_info *fi)
{
    DEBUG("truncate %s, %u\n", path, length);

    if (strcmp(path, BACKFS_CONTROL_FILE) == 0) {
        // Probably due to user doing 'echo foo > .backfs_control' instead of using '>>'.
        // Ignore it.
        return 0;
    }

    int ret = 0;
    char *real = NULL;

    RW_ONLY();

    if (fi == NULL) {
        REALPATH(real, path);
        FORWARD(truncate, real, length);
    }
    else {
        FORWARD(ftruncate, fi->fh, length);
    }

    uint32_t block = length / backfs.block_size;
    cache_try_invalidate_blocks_above(path, block);

exit:
    FREE(real);
    return ret;
}

int backfs_create(const char *path, mode_t mode, struct fuse_file_info *info)
{
    DEBUG("create (mode 0%o) %s\n", mode, path);
    int ret = 0;
    char *real = NULL;

    RW_ONLY();
    REALPATH(real, path);

    ret = open(real, info->flags | O_CREAT | O_EXCL); // not sure on the read/write mode here...
    if (ret == -1) {
        PERROR("error opening real file for create");
        ret = -errno;
        goto exit;
    }
    info->fh = ret;

    FORWARD(fchmod, info->fh, mode);

exit:
    FREE(real);
    return ret;
}

int backfs_unlink(const char *path)
{
    DEBUG("unlink %s\n", path);
    int ret = 0;
    char *real = NULL;

    RW_ONLY();
    REALPATH(real, path);
    FORWARD(unlink, real);

    if (0 == cache_try_invalidate_file(path)) {
        DEBUG("unlink: invalidated cache for the file\n");
    }
    // ignore its return value; don't care if it fails.

exit:
    FREE(real);
    return ret;
}

int backfs_release(const char *path, struct fuse_file_info *info)
{
    DEBUG("release: %s\n", path);

    if (info->fh != 0) {
        // If we saved a file handle here from 
        DEBUG("closing saved file handle\n");
        close((int)info->fh);
    }

    // FUSE ignores the return value here.
    return 0;
}

int backfs_mkdir(const char *path, mode_t mode)
{
    DEBUG("mkdir (mode 0%o) %s\n", mode, path);
    int ret = 0;
    char *real = NULL;

    RW_ONLY();
    REALPATH(real, path);
    FORWARD(mkdir, real, mode);

exit:
    FREE(real);
    return ret;
}

int backfs_rmdir(const char *path)
{
    DEBUG("rmdir %s\n", path);
    int ret = 0;
    char *real = NULL;

    RW_ONLY();
    REALPATH(real, path);
    FORWARD(rmdir, real);

exit:
    FREE(real);
    return ret;
}

int backfs_symlink(const char *target, const char *path)
{
    DEBUG("symlink %s -> %s\n", target, path);
    int ret = 0;
    char *real = NULL;

    RW_ONLY();
    REALPATH(real, path);
    FORWARD(symlink, target, real);

exit:
    FREE(real);
    return ret;
}

enum rename_or_link { RENAME, LINK };

static int rename_or_link_internal(
        const char *path,
        const char *path_new,
        enum rename_or_link which,
        unsigned int flags)
{
    int ret = 0;
    char *real = NULL;
    char *real_new = NULL;
    bool locked = false;

    RW_ONLY();

    REALPATH(real, path);
    REALPATH(real_new, path_new);

    if (which == RENAME) {
        pthread_mutex_lock(&backfs.lock);
        locked = true;
    }

    switch (which) {
        case RENAME:
            FORWARD(renameat2, 0, real, 0, real_new, flags);
            break;
        case LINK:
            FORWARD(link, real, real_new);
            break;
    }

    if (which == RENAME) {
        int cache_ret = cache_rename(path, path_new);
        if (cache_ret != 0) {
            FORWARD(rename, real_new, real); // undo the rename
            ret = cache_ret;
        }
    }

exit:
    if (locked)
        pthread_mutex_unlock(&backfs.lock);
    FREE(real);
    FREE(real_new);
    return ret;
}

int backfs_rename(const char *path, const char *path_new, unsigned int flags)
{
    DEBUG("rename %s -> %s (flags=%x)\n", path, path_new, flags);
    return rename_or_link_internal(path, path_new, RENAME, 0);
}

int backfs_link(const char *path, const char *path_new)
{
    DEBUG("link %s -> %s\n", path, path_new);
    return rename_or_link_internal(path, path_new, LINK, 0);
}

int backfs_chmod(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    DEBUG("chmod %s 0%o\n", path, mode);
    int ret = 0;
    char *real = NULL;

    RW_ONLY();

    if (fi == NULL) {
        REALPATH(real, path);
        FORWARD(chmod, real, mode);
    }
    else {
        FORWARD(fchmod, fi->fh, mode);
    }

exit:
    FREE(real);
    return ret;
}

int backfs_chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi)
{
    DEBUG("chown %s %d:%d\n", path, uid, gid);
    int ret = 0;
    char *real = NULL;

    RW_ONLY();

    if (fi == NULL) {
        REALPATH(real, path);
        FORWARD(chown, real, uid, gid);
    }
    else {
        FORWARD(fchown, fi->fh, uid, gid);
    }

exit:
    FREE(real);
    return ret;
}

#ifdef HAVE_UTIMENS
int backfs_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi)
{
    DEBUG("utimens %s\n", path);
    int ret = 0;
    char *real = NULL;

    RW_ONLY();

    if (fi == NULL) {
        REALPATH(real, path);
        FORWARD(utimensat, 0, real, tv, 0);
    }
    else {
        FORWARD(futimens, fi->fh, tv);
    }

exit:
    FREE(real);
    return ret;
}
#endif

//
// Support for custom attributes
//

enum
{
    ATTRIBUTE_INVALID_ACTION,
    ATTRIBUTE_READ,
    ATTRIBUTE_WRITE,
    ATTRIBUTE_WRITE_REPLACE,
    ATTRIBUTE_CREATE,
    ATTRIBUTE_REMOVE,
};

struct backfs_attribute_handler
{
    const char *attribute_name;
    int (*handler_fn)(const char *path, const char *name, char *value, size_t size,
                            int action);
};

#define BACKFS_ATTRIBUTE_HANDLER(attribute_name) \
int backfs_##attribute_name##_handler( \
    const char *path, \
    const char *name, \
    char *value, \
    size_t size, \
    int action \
    )

BACKFS_ATTRIBUTE_HANDLER(in_cache)
{
    (void)name;

    int ret = 0;
    char *out = NULL;

    if (action != ATTRIBUTE_READ) {
        ret = -EACCES;
        goto exit;
    }

    uint64_t cached_bytes = 0;
    ret = cache_has_file(path, &cached_bytes);
    if (ret != 0) {
        goto exit;
    }

    int required_space = asprintf(&out, "%llu", cached_bytes);

    if (size == 0) {
        ret = required_space;
    }
    else if (size < required_space) {
        ret = -ERANGE;
    }
    else {
        memcpy(value, out, required_space);
        ret = required_space;
    }

exit:
    FREE(out);
    return ret;
}

const struct backfs_attribute_handler backfs_attributes[] = {
    { "user.backfs.in_cache", &backfs_in_cache_handler }
};

int backfs_handle_attribute(const char *path, const char *name, char *value, size_t size,
                            int action)
{
    int ret = -ENOTSUP;
    bool handled = false;

    for (size_t i = 0; i < COUNTOF(backfs_attributes); i++) {
        if (strcmp(backfs_attributes[i].attribute_name, name) == 0) {
            ret = backfs_attributes[i].handler_fn(path, name, value, size, action);
            handled = true;
            break;
        }
    }

    const char prefix[] = "user.backfs.";
    if (!handled && strncmp(name, prefix, COUNTOF(prefix) - 1) == 0) {
        switch (action) {
        case ATTRIBUTE_READ:
        case ATTRIBUTE_REMOVE:
            ret = -ENODATA; // should be ENOATTR but this isn't as widely available
            break;
        case ATTRIBUTE_WRITE:
        case ATTRIBUTE_WRITE_REPLACE:
        case ATTRIBUTE_CREATE:
            ret = -EACCES;
            break;
        default:
            ret = -EINVAL;
        }
    }

    return ret;
}

int backfs_getxattr(const char *path, const char *name, char *value, size_t size)
{
    DEBUG("getxattr %s %s %d\n", path, name, size);
    int ret = 0;
    char *real = NULL;

    ret = backfs_handle_attribute(path, name, value, size, ATTRIBUTE_READ);

    if (ret == -ENOTSUP) {
        REALPATH(real, path);
        FORWARD(getxattr, real, name, value, size);
    }

exit:
    FREE(real);
    return ret;
}

int backfs_setxattr(const char *path, const char *name, const char *value, size_t size,
                    int flags)
{
    DEBUG("setxattr %s %s\n", path, name);
    int ret = 0;
    char *real = NULL;

    RW_ONLY();

    int action;
    if (flags == XATTR_CREATE)
        action = ATTRIBUTE_CREATE;
    else if (flags == XATTR_REPLACE)
        action = ATTRIBUTE_WRITE_REPLACE;
    else 
        action = ATTRIBUTE_WRITE;

    ret = backfs_handle_attribute(path, name, (char*)value, size, action);

    if (ret == -ENOTSUP) {
        REALPATH(real, path);
        FORWARD(setxattr, real, name, value, size, flags);
    }

exit:
    FREE(real);
    return ret;
}

int backfs_removexattr(const char *path, const char *name)
{
    DEBUG("removexattr %s %s\n", path, name);
    int ret = 0;
    char *real = NULL;

    RW_ONLY();

    ret = backfs_handle_attribute(path, name, NULL, 0, ATTRIBUTE_REMOVE);

    if (ret == -ENOTSUP) {
        REALPATH(real, path);
        FORWARD(removexattr, real, name);
    }

exit:
    FREE(real);
    return ret;
}

int backfs_listxattr(const char *path, char *list, size_t size)
{
    DEBUG("listxattr %s\n", path);
    int ret = 0;
    char *real = NULL;

    REALPATH(real, path);

    if (size == 0) {
        ret = listxattr(real, NULL, 0);
        
        for (size_t i = 0; i < COUNTOF(backfs_attributes); i++) {
            ret += strlen(backfs_attributes[i].attribute_name) + 1;
        }
    }
    else {
        for (size_t i = 0; i < COUNTOF(backfs_attributes); i++) {
            size_t name_len = strlen(backfs_attributes[i].attribute_name) + 1;
            if (name_len > size) {
                ret = ERANGE;
                goto exit;
            }

            memcpy(list, backfs_attributes[i].attribute_name, name_len);
            size -= name_len;
            list += name_len;
            ret += name_len;
        }

        ssize_t listsize = listxattr(real, list, size);
        DEBUG("forwarded: %d\n", listsize);
        if (listsize < 0) {
            goto exit;
        }
        else {
            ret += listsize;
        }
    }

exit:
    FREE(real);
    return ret;
}

#ifdef STUB_FUNCTIONS
//
// Stubs for the remaining syscalls
//

#define STUB(func, ...) \
int backfs_##func(const char *path, __VA_ARGS__) \
{ \
    DEBUG(#func ": %s\n", path); \
    return -ENOSYS; \
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
STUB(statfs, struct statvfs *stat)
STUB(flush, struct fuse_file_info *ffi)
STUB(fsync, int n, struct fuse_file_info *ffi)
STUB(fsyncdir, int a, struct fuse_file_info *ffi)
STUB(lock, struct fuse_file_info *ffi, int cmd, struct flock *flock)
STUB(bmap, size_t blocksize, uint64_t *idx)
STUB(ioctl, int cmd, void *arg, struct fuse_file_info *ffi, unsigned int flags, void *data)
STUB(poll, struct fuse_file_info *ffi, struct fuse_pollhandle *ph, unsigned *reventsp)
STUB(flock, struct fuse_file_info *ffi, int op)
STUB(fallocate, int a, off_t b, off_t c, struct fuse_file_info *ffi)
STUB(mknod, mode_t mode, dev_t dev)
#pragma GCC diagnostic pop
#endif

#define IMPL(func) .func = backfs_##func

#ifdef FUSE3
#   define IMPL_COMPAT(func) .func = backfs_##func
#   define COMPAT(func, ...)
#else
#   define IMPL_COMPAT(func) .func = backfs_##func##_compat

int backfs_rename_compat(const char *path, const char *path_new)
{
    return backfs_rename(path, path_new, 0);
}

int backfs_chmod_compat(const char *path, mode_t mode)
{
    return backfs_chmod(path, mode, NULL);
}

int backfs_chown_compat(const char *path, uid_t uid, gid_t gid)
{
    return backfs_chown(path, uid, gid, NULL);
}

#ifdef HAVE_UTIMENS
int backfs_utimens_compat(const char *path, const struct timespec tv[2])
{
    return backfs_utimens(path, tv, NULL);
}
#endif

int backfs_getattr_compat(const char *path, struct stat *stbuf)
{
    return backfs_getattr(path, stbuf, NULL);
}

int backfs_truncate_compat(const char *path, off_t length)
{
    return backfs_truncate(path, length, NULL);
}

#endif

static struct fuse_operations BackFS_Opers = {
#ifdef BACKFS_RW
    IMPL(mkdir),
    IMPL(unlink),
    IMPL(rmdir),
    IMPL(symlink),
    IMPL_COMPAT(rename),
    IMPL(link),
    IMPL_COMPAT(chmod),
    IMPL_COMPAT(chown),
    IMPL(setxattr),
    IMPL(removexattr),
    IMPL(create),
#ifdef HAVE_UTIMENS
    IMPL_COMPAT(utimens),
#endif
#ifdef STUB_FUNCTIONS
    IMPL(fsyncdir),
    IMPL(statfs),
    IMPL(flush),
    IMPL(fsync),
    IMPL(lock),
    IMPL(bmap),
    IMPL(ioctl),
    IMPL(poll),
    IMPL(flock),
    IMPL(fallocate),
    IMPL(mknod),
#endif
#endif
    IMPL(open),
    IMPL(read),
    IMPL(opendir),
    IMPL(readdir),
    IMPL(releasedir),
    IMPL_COMPAT(getattr),
    IMPL(access),
    IMPL(write),        // used in read-only builds for fake files
    IMPL(readlink),
    IMPL_COMPAT(truncate),  // used in read-only builds for fake files
    IMPL(release),
    IMPL(getxattr),
    IMPL(listxattr),
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
    (void)data;

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
        // Turn on big_writes so we get writes > 4K in size. Helps cache.
        fuse_opt_add_opt(outargs->argv, "big_writes");
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
    int exit_code = 0;
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct statvfs cachedir_statvfs;

    backfs_log_level = LOG_LEVEL_WARN;
    backfs.real_root_alloc = true;  // assume it comes from arg parsing.

    if (fuse_opt_parse(&args, &backfs, backfs_opts, backfs_opt_proc) == -1) {
        fprintf(stderr, "BackFS: argument parsing failed.\n");
        exit_code = 1;
        goto exit;
    }

    if (num_nonopt_args_read > 0) {
        fuse_opt_add_arg(&args, nonopt_arguments[num_nonopt_args_read - 1]);
        if (num_nonopt_args_read == 2) {
            backfs.real_root = nonopt_arguments[0];
            backfs.real_root_alloc = false; // straight from argv now.
        }
    }
    else {
        fprintf(stderr, "BackFS: error: you need to specify a mount point.\n");
        usage();
        fuse_opt_add_arg(&args, "-ho");
        backfs_fuse_main(args.argc, args.argv, &BackFS_Opers);
        exit_code = -1;
        goto exit;
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
        exit_code = -1;
        goto exit;
    }

    if (backfs.real_root[0] != '/') {
        const char *rel = backfs.real_root;
        char* temp;
        asprintf(&temp, "%s/%s", cwd, rel);
        if (backfs.real_root_alloc) {
            free(backfs.real_root);
        }
        backfs.real_root = temp;
        backfs.real_root_alloc = true;
    }

    DIR *d;
    if ((d = opendir(backfs.real_root)) == NULL) {
        perror("BackFS ERROR: error checking backing filesystem");
        fprintf(stderr, "BackFS: specified as \"%s\"\n", backfs.real_root);
        exit_code = 2;
        goto exit;
    }
    closedir(d);

    if (backfs.cache_dir == NULL) {
        fprintf(stderr, "BackFS: error: you need to specify a cache location with \"-o cache\"\n");
        exit_code = -1;
        goto exit;
    }

    if (backfs.cache_dir[0] != '/') {
        char *rel = backfs.cache_dir;
        char *temp;
        asprintf(&temp, "%s/%s", cwd, rel);
        free(backfs.cache_dir);
        backfs.cache_dir = temp;
    }

    FREE(cwd);

#ifndef NOSYSLOG
    openlog("BackFS", 0, LOG_USER);
#endif

    // TODO: move these to fscache.c?

    if (statvfs(backfs.cache_dir, &cachedir_statvfs) == -1) {
        perror("BackFS ERROR: error checking cache dir");
        exit_code = 3;
        goto exit;
    }

    if (access(backfs.cache_dir, W_OK) == -1) {
        perror("BackFS ERROR: unable to write to cache dir");
        exit_code = 4;
        goto exit;
    }

    char *buf = NULL;
    asprintf(&buf, "%s/buckets", backfs.cache_dir);
    if (mkdir(buf, 0700) == -1 && errno != EEXIST) {
        perror("BackFS ERROR: unable to create cache bucket directory");
        exit_code = 5;
        goto exit;
    }
    FREE(buf);

    asprintf(&buf, "%s/map", backfs.cache_dir);
    if (mkdir(buf, 0700) == -1 && errno != EEXIST) {
        perror("BackFS ERROR: unable to create cache map directory");
        exit_code = 6;
        goto exit;
    }
    FREE(buf);
	
    unsigned long long cache_block_size = 0;
    asprintf(&buf, "%s/buckets/bucket_size", backfs.cache_dir);
    bool has_block_size_marker = false;
    FILE *f = fopen(buf, "r");
    if (f == NULL) {
    if (errno != ENOENT) {
            perror("BackFS ERROR: unable to open cache block size marker");
            exit_code = 7;
            goto exit;
        }
    } else {
        if (fscanf(f, "%llu", &cache_block_size) != 1) {
            perror("BackFS ERROR: unable to read cache block size marker");
            exit_code = 8;
            goto exit;
        }
        has_block_size_marker = true;
		
        if (backfs.block_size == 0) {
            backfs.block_size = cache_block_size;
            fprintf(stderr, "BackFS: using previous cache block size of %llu\n", cache_block_size);
        } else if (backfs.block_size != cache_block_size) {
            fprintf(stderr, "BackFS ERROR: cache was made using different block size of %llu. Unable to use specified size of %llu\n",
                    cache_block_size, backfs.block_size);
            exit_code = 9;
            goto exit;
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
            exit_code = 10;
            goto exit;
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
        exit_code = -1;
        goto exit;
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

    // Initializing mutex
    pthread_mutex_init(&backfs.lock, NULL);
    
    printf("ready to go!\n");
    backfs_fuse_main(args.argc, args.argv, &BackFS_Opers);

exit:
    fuse_opt_free_args(&args);
    free(backfs.cache_dir);
    if (backfs.real_root_alloc) {
        free(backfs.real_root);
    }

    pthread_exit(NULL);

    return exit_code;
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
