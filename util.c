/*
 * BackFS utility functions
 * Copyright (c) 2014-2018 William R. Fraser
 */

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>
#include "util.h"

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

/*
 * glibc does not provide a wrapper for renameat2, so we have to make one ourselves.
 */
int renameat2(
        int olddirfd,
        const char *oldpath,
        int newdirfd,
        const char *newpath,
        unsigned int flags)
{
    return (int) syscall(SYS_renameat2, olddirfd, oldpath, newdirfd, newpath, flags);
}
