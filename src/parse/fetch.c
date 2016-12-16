#include <string.h>
#include <stdio.h>

#include "parse/create.h"
#include "util/log.h"
#include "util/strmanip.h"

DbOperator* parse_fetch(char* arguments, message* response, char* handle) {
    if (response == NULL)
        return NULL;
    if (arguments == NULL || *arguments != '(') {
        response->status = UNKNOWN_COMMAND;
        return NULL;
    }
    arguments++;

    // create a copy of string
    size_t space = strlen(arguments) + 1;
    char* copy = malloc(space * sizeof(char));
    strcpy(copy, arguments);
    size_t len = strlen(copy);
    if (copy[len - 1] != ')') {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    copy[len - 1] = '\0';
    
    // parse arguments
    char* token = strsep(&copy, ",");
    if (token == NULL) {
        // invalid query format
        response->status = INCORRECT_FORMAT;
        return NULL;
    }

    // create fetch operator object
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = FETCH;
    dbo->fields.fetch.col_name = token;
    dbo->fields.fetch.db_name = strsep(&dbo->fields.fetch.col_name, ".");
    dbo->fields.fetch.tbl_name = strsep(&dbo->fields.fetch.col_name, ".");
    dbo->fields.fetch.source = copy;
    dbo->fields.fetch.target = handle;
    return dbo;
}
