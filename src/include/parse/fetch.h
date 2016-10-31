#ifndef PARSE_FETCH_H
#define PARSE_FETCH_H

#include "api/cs165.h"
#include "util/message.h"

DbOperator* parse_fetch(char* arguments, message* response, char* handle);

#endif
