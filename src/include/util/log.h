// util/log.h
// CS165 Fall 2015
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdio.h>

#include "util/message.h"

#define LOG
#define LOG_ERR
#define LOG_INFO

// Usage: cs165_log(stderr, "%s: error at line: %d", __func__, __LINE__);
void cs165_log(FILE* out, const char *format, ...);
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
void log_err(const char *format, ...);
// Usage: log_info("Command received: %s", command_string);
void log_info(const char *format, ...);

#endif /* __UTILS_H__ */
