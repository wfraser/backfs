#ifndef WRF_BACKFS_GLOBAL_H
#define WRF_BACKFS_GLOBAL_H

#ifndef BACKFS_LOG_SUBSYS
#define _BACKFS_LOG_SUBSYS /* empty */
#define BACKFS_LOG_SUBSYS_ /* empty */
#define BACKFS_LOG_SUBSYS__ /* empty */
#else
#define _BACKFS_LOG_SUBSYS " " BACKFS_LOG_SUBSYS
#define BACKFS_LOG_SUBSYS_ BACKFS_LOG_SUBSYS " "
#define BACKFS_LOG_SUBSYS__ BACKFS_LOG_SUBSYS ": "
#endif

enum {
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_INFO,
    LOG_LEVEL_WARN,
    LOG_LEVEL_ERROR,
};

#ifndef NODEBUG
#ifndef NOSYSLOG
#include <syslog.h>
#define ERROR(...)  if (backfs_log_level >= LOG_LEVEL_ERROR) \
                        syslog(LOG_ERR, BACKFS_LOG_SUBSYS_ "ERROR: " __VA_ARGS__)
#define WARN(...)   if (backfs_log_level >= LOG_LEVEL_WARN) \
                        syslog(LOG_WARNING, BACKFS_LOG_SUBSYS_ "WARNING: " __VA_ARGS__)
#define INFO(...)   if (backfs_log_level >= LOG_LEVEL_INFO) \
                        syslog(LOG_INFO, BACKFS_LOG_SUBSYS__ __VA_ARGS__)
#define DEBUG(...)  if (backfs_log_level >= LOG_LEVEL_DEBUG) \
                        syslog(LOG_DEBUG, BACKFS_LOG_SUBSYS__ __VA_ARGS__)
#define PERROR(msg) if (backfs_log_level >= LOG_LEVEL_ERROR) \
                        syslog(LOG_ERR, BACKFS_LOG_SUBSYS_ "ERROR: " msg ": %m")
#else
#define ERROR(...)  if (backfs_log_level >= LOG_LEVEL_ERROR) \
                        fprintf(stderr, "BackFS" _BACKFS_LOG_SUBSYS " ERROR: " __VA_ARGS__)
#define WARN(...)   if (backfs_log_level >= LOG_LEVEL_WARN) \
                        fprintf(stderr, "BackFS" _BACKFS_LOG_SUBSYS " WARNING: " __VA_ARGS__)
#define INFO(...)   if (backfs_log_level >= LOG_LEVEL_INFO) \
                        fprintf(stderr, "BackFS" _BACKFS_LOG_SUBSYS ": " __VA_ARGS__)
#define DEBUG(...)  if (backfs_log_level >= LOG_LEVEL_DEBUG) \
                        fprintf(stderr, "BackFS" _BACKFS_LOG_SUBSYS ": " __VA_ARGS__)
#define PERROR(msg) if (backfs_log_level >= LOG_LEVEL_ERROR) \
                        perror("BackFS" _BACKFS_LOG_SUBSYS " ERROR: " msg)
#endif //NOSYSLOG
#else
#define ERROR(...) /* nothing */
#define WARN(...) /* nothing */
#define INFO(...) /* nothing */
#define PERROR(msg) /* nothing */
#endif //NODEBUG

#endif //WRF_BACKFS_GLOBAL_H
