#ifndef PARSE_BATCH_H
#define PARSE_BATCH_H

#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>

#include "api/cs165.h"
#include "util/message.h"

DbOperator* parse_batch(char* arguments, message* response);

#endif
