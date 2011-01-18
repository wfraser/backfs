/*
 * Filesystem Linked List
 * Copyright (c) 2010-2011 William R. Fraser
 */
#include "fsll.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <dirent.h>
#include <fcntl.h>
#include <libgen.h>

char * fsll_getlink(const char *base, const char *file)
{
    char path[PATH_MAX];
    char *result = (char*)malloc(PATH_MAX);
    snprintf(path, PATH_MAX, "%s/%s", base, file);
    ssize_t len;
    if ((len = readlink(path, result, PATH_MAX-1)) == -1) {
        if (errno == ENOENT || errno == ENOTDIR) {
            free(result);
            return NULL;
        } else {
            PERROR("readlink in fsll_getlink");
            free(result);
            return NULL;
        }
    } else {
        result[len] = '\0';
        return result;
    }
}

void fsll_makelink(const char *base, const char *file, const char *dest)
{
    char source[PATH_MAX];
    snprintf(source, PATH_MAX, "%s/%s", base, file);

    if (unlink(source) == -1) {
        if (errno != ENOENT && errno != ENOTDIR) {
            PERROR("unlink in fsll_makelink");
            ERROR("caused by unlink(%s)\n", source);
            return;
        }
    }

    if (dest != NULL) {
        if (symlink(dest, source) == -1) {
            PERROR("symlink in fsll_makelink");
            ERROR("caused by symlink(%s,%s)\n", dest, source);
        }
    }
}

bool fsll_file_exists(const char *base, const char *file)
{
    char path[PATH_MAX];
    if (file != NULL) {
        snprintf(path, PATH_MAX, "%s/%s", base, file);
        base = path;
    }

    if (access(base, F_OK) == -1) {
        return false;
    } else {
        return true;
    }
}

char fsll_base_buf[PATH_MAX];
const char * fsll_basename(const char *path)
{
    if (path == NULL) {
        strcpy(fsll_base_buf, "NULL");
        return fsll_base_buf;
    }

    char *copy = (char*)malloc(strlen(path)+1);
    strncpy(copy, path, strlen(path)+1);
    strncpy(fsll_base_buf, basename(copy), PATH_MAX);
    free(copy);

    return fsll_base_buf;
}

void fsll_dump(const char *base, const char *headfile, const char *tailfile)
{
#ifdef DEBUG
    char *entry = fsll_getlink(base, headfile);

    if (entry) {
        char *p, *n;
        do {
            p = fsll_getlink(entry, "prev");
            n = fsll_getlink(entry, "next");
            fprintf(stderr, "DUMP: %s <- ", fsll_basename(p));
            fprintf(stderr, "%s -> ", fsll_basename(entry));
            fprintf(stderr, "%s\n", fsll_basename(n));
            if (entry && n && strcmp(entry, n) == 0) {
                fprintf(stderr, "FSLL DUMP: ERROR: list has a loop!\n");
                break;
            }
            if (entry) free(entry);
            entry = NULL;
            if (p) free(p);
        } while ((n == NULL) ? false : (entry = n));

        char *tail = fsll_getlink(base, tailfile);

        if (n && tail && strcmp(n, tail) != 0) {
            fprintf(stderr, "FSLL DUMP: ERROR: list doesn't end with the tail!\n"
                    "\ttail is %s\n", fsll_basename(tail));
        }
        if (entry) free(entry);
        if (tail) free(tail);
        if (n) free(n);
    }
#else
    (void)base;
    (void)headfile;
    (void)tailfile;
#endif //DEBUG
}

/*
 * don't use this function directly.
 */
char * fsll_make_entry(const char *base, const char *dir, uint64_t number)
{
    char *path = (char*)malloc(PATH_MAX);
    if (dir != NULL) {
        snprintf(path, PATH_MAX, "%s/%s/%llu",
                base, dir, (unsigned long long) number);
    } else {
        snprintf(path, PATH_MAX, "%s/%llu", 
                base, (unsigned long long) number);
    }

    if (mkdir(path, 0700) == -1) {
        PERROR("mkdir in fsll_make_entry");
        return NULL;
    }

    return path;
}

/*
 * Move an existing element in the list to the head.
 */
void fsll_to_head(const char *base, const char *path, const char *head, const char *tail)
{
    char *h = fsll_getlink(base, head);
    char *t = fsll_getlink(base, tail);
    char *n = fsll_getlink(path, "next");
    char *p = fsll_getlink(path, "prev");

    if ((p == NULL) ^ (strcmp(h, path) == 0)) {
        if (p)
            ERROR("head entry has a prev: %s\n", path);
        else
            ERROR("entry has no prev but is not head: %s\n", path);
        fsll_dump(base, head, tail);
        // cowardly refusing to break things farther
        return;
    }

    if ((n == NULL) ^ (strcmp(t, path) == 0)) {
        if (n)
            ERROR("tail entry has a next: %s\n", path);
        else
            ERROR("entry has no next but is not tail: %s\n", path);
        fsll_dump(base, head, tail);
        return;
    }

    if ((n != NULL) && strcmp(n, path) == 0) {
        ERROR("entry points to itself as next: %s\n", path);
        return;
    }
    if ((p != NULL) && (strcmp(p, path) == 0)) {
        ERROR("entry points to itself as prev: %s\n", path);
        return;
    }

    // there must not be the situation where the list is empty (no head or 
    // tail yet set) because this function is only for promoting an existing
    // element to the head. Use fsll_insert_as_head() for the other case.
    if (h == NULL) {
        ERROR("fsll_to_head, no head found!\n");
        fsll_dump(base, head, tail);
        return;
    }
    if (t == NULL) {
        ERROR("in fsll_to_head, no tail found!\n");
        fsll_dump(base, head, tail);
        return;
    }

    if (p == NULL) {
        // already head; do nothing
        return ;
    } else {
        fsll_makelink(p, "next", n);
    }

    if (n) {
        fsll_makelink(n, "prev", p);
    } else {
        fsll_makelink(base, tail, p);
        // p->next is already NULL from above
    }

    // assuming h != NULL
    fsll_makelink(h, "prev", path);
    fsll_makelink(path, "next", h);
    fsll_makelink(path, "prev", NULL);
    fsll_makelink(base, head, path);

    if (h) free(h);
    if (t) free(t);
    if (n) free(n);
    if (p) free(p);

    fsll_dump(base, head, tail);
}

void fsll_insert_as_head(const char *base, const char *path, const char *head,
        const char *tail)
{
    // ignore the prev and next; assume the entry is disconnected from the
    // list.

    char *h = fsll_getlink(base, head);
    char *t = fsll_getlink(base, tail);

    if (h == NULL && t == NULL) {
        fsll_makelink(base, head, path);
        fsll_makelink(base, tail, path);
        fsll_makelink(path, "next", NULL);
        fsll_makelink(path, "prev", NULL);
    } else if (h != NULL && t != NULL) {
        fsll_makelink(path, "next", h);
        fsll_makelink(h, "prev", path);
        fsll_makelink(base, head, path);
    } else {
        if (h)
            ERROR("list has a head but no tail!\n");
        if (t)
            ERROR("list has a tail but no head!\n");
    }

    if (h) free(h);
    if (t) free(t);
}

void fsll_insert_as_tail(const char *base, const char *path, const char *head,
        const char *tail)
{
    char *h = fsll_getlink(base, head);
    char *t = fsll_getlink(base, tail);

    if (h == NULL && t == NULL) {
        fsll_makelink(base, head, path);
        fsll_makelink(base, tail, path);
        fsll_makelink(path, "next", NULL);
        fsll_makelink(path, "prev", NULL);
    } else if (h != NULL && t != NULL) {
        fsll_makelink(path, "prev", t);
        fsll_makelink(t, "next", path);
        fsll_makelink(base, tail, path);
    } else {
        if (h)
            ERROR("list has a head but no tail!\n");
        if (t)
            ERROR("list has a tail but no head!\n");
    }

    if (h) free(h);
    if (t) free(t);
}

void fsll_disconnect(const char *base, const char *path, const char *head,
        const char *tail)
{
    char *h = fsll_getlink(base, head);
    char *t = fsll_getlink(base, tail);
    char *n = fsll_getlink(path, "next");
    char *p = fsll_getlink(path, "prev");

    if (strcmp(h, path) == 0) {
        if (n == NULL) {
            if (strcmp(t, path) == 0) {
                fsll_makelink(base, tail, NULL);
            } else {
                ERROR("entry has no next but is not tail: %s\n", path);
            }
        } else {
            fsll_makelink(base, head, n);
            fsll_makelink(n, "prev", NULL);
        }
    }

    if (strcmp(t, path) == 0) {
        if (p == NULL) {
            if (strcmp(h, path) == 0) {
                fsll_makelink(base, head, NULL);
            } else {
                ERROR("entry has no prev but is not head: %s\n", path);
            }
        } else {
            fsll_makelink(base, tail, p);
            fsll_makelink(p, "next", NULL);
        }
    }

    if (n && p) {
        fsll_makelink(n, "prev", p);
        fsll_makelink(p, "next", n);
    }

    fsll_makelink(path, "next", NULL);
    fsll_makelink(path, "prev", NULL);

    if (h) free(h);
    if (t) free(t);
    if (n) free(n);
    if (p) free(p);
}

