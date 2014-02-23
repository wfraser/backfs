#include <stdio.h>
#include <stdbool.h>
#include <sys/statfs.h>
#include <unistd.h>
#include "util.h"
#include "global.h"

extern int backfs_log_level;
extern bool backfs_log_stderr;

size_t max_filename_length(const char* path)
{
    struct statfs sfs;
    if (-1 == statfs(path, &sfs)) {
        PERROR("statfs64 failed");
        return 255; // Seems to be a safe default.
    }

    return (size_t)sfs.f_namelen;
}
