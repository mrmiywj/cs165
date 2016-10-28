#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>

#include "cs165_api.h"
#include "message.h"

DbOperator* create(char* arguments, message* response);
DbOperator* parse_create_db(char* arguments, message* response);
DbOperator* parse_create_tbl(char* arguments, message* response);
DbOperator* parse_create_col(char* arguments, message* response);
