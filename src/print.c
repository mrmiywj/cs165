#include <string.h>
#include <stdio.h>

#include "parse/create.h"
#include "util/log.h"
#include "util/strmanip.h"

DbOperator* parse_print(char* arguments, message* response) {
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

    // create print operator object
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = PRINT;
    dbo->fields.print.col_name = token;
    dbo->fields.print.db_name = strsep(&dbo->fields.select.col_name, ".");
    dbo->fields.print.tbl_name = strsep(&dbo->fields.select.col_name, ".");
    dbo->fields.print.handle = copy;
    return dbo;
}
