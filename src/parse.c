#define _BSD_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>

#include "parse/parse.h"
#include "util/log.h"
#include "util/strmanip.h"
#include "parse/create.h"
#include "parse/insert.h"
#include "parse/select.h"

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
DbOperator* parse_command(
    char* query_command, 
    message* send_message, 
    int client_socket, 
    ClientContext* context
) {
    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE; 
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle file table
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    log_info("QUERY: %s", query_command);
    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);

    DbOperator* dbo = process_query(query_command, send_message);
    if (dbo != NULL) {
        dbo->client_fd = client_socket;
        dbo->context = context;
    }
    return dbo;
}

DbOperator* process_query(char* query, message* send_message) {
    if (strncmp(query, "create", 6) == 0) {
        query += 6;
        return parse_create(query, send_message);
    }
    if (strncmp(query, "relational_insert", 17) == 0) {
        query += 17;
        return parse_insert(query, send_message);
    }
    if (strncmp(query, "select", 6) == 0) {
        query += 6;
        return parse_select(query, send_message);
    }
    return NULL;
}
