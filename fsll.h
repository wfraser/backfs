#ifndef WRF_FSLL_H
#define WRF_FSLL_H
/*
 * Filesystem Linked List
 * Copyright (c) 2010-2014 William R. Fraser
 */

#include <stdint.h>
#include <stdbool.h>

char * fsll_getlink(const char *base, const char *file);
void fsll_makelink(const char *base, const char *file, const char *dest);
bool fsll_file_exists(const char *base, const char *file);
void fsll_dump(const char *base, const char *head, const char *tail);
const char * fsll_basename(const char *path);
char * fsll_make_entry(const char *base, const char *dir, uint64_t number);
void fsll_to_head(const char *base, const char *path, const char *head,
                    const char *tail);
void fsll_insert_as_head(const char *base, const char *path, const char *head,
                            const char *tail);
void fsll_insert_as_tail(const char *base, const char *path, const char *head,
                            const char *tail);
void fsll_disconnect(const char *base, const char *path, const char *head,
                        const char *tail);

#endif //WRF_FSLL_H
