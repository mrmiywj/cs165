#ifndef PARSE_MATH_H
#define PARSE_MATH_H

#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>

#include "api/cs165.h"
#include "util/message.h"

DbOperator* parse_math(char* arguments, message* response, char* handle, MathType type);

#endif
