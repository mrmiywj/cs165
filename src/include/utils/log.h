// utils/log.h
// CS165 Fall 2015
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdarg.h>
#include <stdio.h>

#include "message.h"

#define LOG
#define LOG_ERR
#define LOG_INFO

char* trim_newline(char *str);
char* trim_parenthesis(char *str);
char* trim_whitespace(char *str);
char* trim_quotes(char *str);

// Usage: cs165_log(stderr, "%s: error at line: %d", __func__, __LINE__);
void cs165_log(FILE* out, const char *format, ...);
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
void log_err(const char *format, ...);
// Usage: log_info("Command received: %s", command_string);
void log_info(const char *format, ...);

char* next_token(char** tokenizer, message_status* status);

#endif /* __UTILS_H__ */
