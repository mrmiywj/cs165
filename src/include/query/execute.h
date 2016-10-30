#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "util/const.h"
#include "parse/parse.h"
#include "api/cs165.h"
#include "util/message.h"
#include "util/log.h"

#define COL_INITIAL_SIZE 10
#define COL_RESIZE_FACTOR 2

Db *current_db;

char* executeDbOperator(DbOperator* query, message* send_message);
char* handleCreateQuery(DbOperator* query, message* send_message);
char* handleInsertQuery(DbOperator* query, message* send_message);
char* handleLoaderQuery(DbOperator* query, message* send_message);
char* handleSelectQuery(DbOperator* query, message* send_message);
char* handlePrintQuery(DbOperator* query, message* send_message);
