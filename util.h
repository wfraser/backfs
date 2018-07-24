#ifndef BACKFS_UTIL_H
#define BACKFS_UTIL_H
/*
 * BackFS utility functions
 * Copyright (c) 2014 William R. Fraser
 */

char* areadlink(const char* path);
int renameat2(
        int olddirfd,
        const char *oldpath,
        int newdirfd,
        const char *newpath,
        unsigned int flags);

#define FREE(var) { free(var); var = NULL; }
#define COUNTOF(var) (sizeof(var) / sizeof(*var))

#endif //BACKFS_UTIL_H
