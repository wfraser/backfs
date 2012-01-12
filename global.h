#ifndef WRF_BACKFS_GLOBAL_H
#define WRF_BACKFS_GLOBAL_H

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

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
    LOG_LEVEL_ERROR,
    LOG_LEVEL_WARN,
    LOG_LEVEL_INFO,
    LOG_LEVEL_DEBUG
};

#ifndef NODEBUG
#define CONERROR(...)  if (backfs_log_level >= LOG_LEVEL_ERROR) \
                            fprintf(stderr, "BackFS" _BACKFS_LOG_SUBSYS " ERROR: " __VA_ARGS__)
#define CONWARN(...)   if (backfs_log_level >= LOG_LEVEL_WARN) \
                            fprintf(stderr, "BackFS" _BACKFS_LOG_SUBSYS " WARNING: " __VA_ARGS__)
#define CONINFO(...)   if (backfs_log_level >= LOG_LEVEL_INFO) \
                            fprintf(stderr, "BackFS" _BACKFS_LOG_SUBSYS ": " __VA_ARGS__)
#define CONDEBUG(...)  if (backfs_log_level >= LOG_LEVEL_DEBUG) \
                            fprintf(stderr, "BackFS" _BACKFS_LOG_SUBSYS ": " __VA_ARGS__)
#define CONPERROR(msg) if (backfs_log_level >= LOG_LEVEL_ERROR) \
                            perror("BackFS" _BACKFS_LOG_SUBSYS " ERROR: " msg)

#ifndef NOSYSLOG
#include <syslog.h>
#define ERROR(...)  if (backfs_log_stderr) {                        \
                        CONERROR(__VA_ARGS__);                      \
                    } else if (backfs_log_level >= LOG_LEVEL_ERROR) \
                        syslog(LOG_ERR, BACKFS_LOG_SUBSYS_ "ERROR: " __VA_ARGS__)

#define WARN(...)   if (backfs_log_stderr) {                        \
                        CONWARN(__VA_ARGS__);                       \
                    } else if (backfs_log_level >= LOG_LEVEL_WARN)   \
                        syslog(LOG_WARNING, BACKFS_LOG_SUBSYS_ "WARNING: " __VA_ARGS__)

#define INFO(...)   if (backfs_log_stderr) {                        \
                        CONINFO(__VA_ARGS__);                       \
                    } else if (backfs_log_level >= LOG_LEVEL_INFO)  \
                        syslog(LOG_INFO, BACKFS_LOG_SUBSYS__ __VA_ARGS__)

#define DEBUG(...)  if (backfs_log_stderr) {                        \
                        CONDEBUG(__VA_ARGS__);                      \
                    } else if (backfs_log_level >= LOG_LEVEL_DEBUG) \
                        syslog(LOG_DEBUG, BACKFS_LOG_SUBSYS__ __VA_ARGS__)

#define PERROR(msg) if (backfs_log_stderr) {                        \
                        CONPERROR(msg);                             \
                    } else if (backfs_log_level >= LOG_LEVEL_ERROR) \
                        syslog(LOG_ERR, BACKFS_LOG_SUBSYS_ "ERROR: " msg ": %m")
#else
#define ERROR   CONERROR
#define WARN    CONWARN
#define INFO    CONINFO
#define DEBUG   CONDEBUG
#define PERROR  CONPERROR
#endif //!NOSYSLOG

#else //NODEBUG
#define ERROR(...) /* nothing */
#define WARN(...) /* nothing */
#define INFO(...) /* nothing */
#define PERROR(msg) /* nothing */
#endif //NODEBUG

#endif //WRF_BACKFS_GLOBAL_H
