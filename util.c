#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/statfs.h>
#include <unistd.h>
#include "util.h"
#include "global.h"

extern int backfs_log_level;
extern bool backfs_log_stderr;

size_t max_filename_length(const char* path)
{
    /* It's not safe to simply do:
     *   return pathconf(path, _PC_NAME_MAX);
     * because pathconf is broken when used with huge filesystems on 32-bit
     * machines. It looks like it's hardcoded to use the 32-bit version of
     * statfs, which can't fit some values (i.e. inode counts) into the 32-bit
     * struct statfs, so it fails with EOVERFLOW.
     * The solution is to manually call statfs, which is smart enough to use
     * 64-bit structs if necessary. */

    struct statfs sfs;
    if (-1 == statfs(path, &sfs)) {
        PERROR("statfs in max_filename_length failed");
        return 255; // Seems to be a safe default.
    }
    return (size_t)sfs.f_namelen;
}

/*
 * A version of readlink() that allocates a big enough buffer.
 * Make sure to free() it later.
 */
char* areadlink(const char* path)
{
    size_t size = 256;
    char* buf = NULL;
    for (;;) {
        buf = realloc(buf, size);
        memset(buf, 0, size);
        ssize_t result_len = readlink(path, buf, size);
        if (result_len == size) {
            size *= 2;
        }
        else if (result_len == -1) {
            free(buf);
            return NULL;
        }
        else {
            break;
        }
    }
    return buf;
}
