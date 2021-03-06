#ifndef PARSE_JOIN_H
#define PARSE_JOIN_H

#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>

#include "api/cs165.h"
#include "util/message.h"

DbOperator* parse_join(char* arguments, message* response, char* handles);

#endif
