#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "parse.h"
#include "api/cs165.h"
#include "message.h"
#include "utils/log.h"
#include "client_context.h"

char* executeDbOperator(DbOperator* query);
char* handleCreateQuery(DbOperator* query);
char* handleInsertQuery(DbOperator* query);
char* handleLoaderQuery(DbOperator* query);
