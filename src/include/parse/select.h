#ifndef PARSE_SELECT_H
#define PARSE_SELECT_H

#include "api/cs165.h"
#include "util/message.h"

DbOperator* parse_select(char* arguments, message* response, char* handle);

#endif
