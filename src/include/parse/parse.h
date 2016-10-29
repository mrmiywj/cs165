#ifndef PARSE_H__
#define PARSE_H__
#include "api/cs165.h"
#include "util/message.h"

DbOperator* process_query(char* query, message* send_message);
DbOperator* parse_command(char* query_command, message* send_message, int client, ClientContext* context);

#endif
