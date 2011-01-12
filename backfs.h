#ifndef WRF_BACKFS_H
#define WRF_BACKFS_H

#ifndef ERROR
#ifdef SYSLOG
#include <syslog.h>
#define ERROR(...) syslog(LOG_ERR, "ERROR: " __VA_ARGS__)
#define WARN(...) syslog(LOG_WARNING, "WARNING: " __VA_ARGS__)
#define INFO(...) syslog(LOG_INFO, __VA_ARGS__)
#define PERROR(msg) syslog(LOG_ERR, "ERROR: " msg ": %m")
#else
#define ERROR(...) fprintf(stderr, "BackFS ERROR: " __VA_ARGS__)
#define WARN(...) fprintf(stderr, "BackFS WARNING: " __VA_ARGS__)
#define INFO(...) fprintf(stderr, "BackFS: " __VA_ARGS__)
#define PERROR(msg) perror("BackFS ERROR: " msg)
#endif //SYSLOG
#endif //ERROR

#endif //WRF_BACKFS_H
