#ifndef PARSE_CREATE_H
#define PARSE_CREATE_H

#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>

#include "api/cs165.h"
#include "util/message.h"

DbOperator* parse_create(char* arguments, message* response);
DbOperator* parse_create_db(char* arguments, message* response);
DbOperator* parse_create_tbl(char* arguments, message* response);
DbOperator* parse_create_col(char* arguments, message* response);
DbOperator* parse_create_idx(char* arguments, message* response);

#endif
