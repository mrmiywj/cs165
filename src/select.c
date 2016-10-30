#include <string.h>
#include <stdio.h>
#include <limits.h>

#include "parse/select.h"

DbOperator* parse_select(char* arguments, message* response) {
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

    // create select operator object
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SELECT;
    dbo->fields.select.col_name = token;
    dbo->fields.select.db_name = strsep(&dbo->fields.select.col_name, ".");
    dbo->fields.select.tbl_name = strsep(&dbo->fields.select.col_name, ".");
    char* lower = strsep(&copy, ",");
    char* upper = copy;
    if (strcmp("null", lower) == 0) {
        dbo->fields.select.minimum = INT_MIN;
    } else {
        dbo->fields.select.minimum = atoi(lower);
    }
    if (strcmp("null", upper) == 0) {
        dbo->fields.select.maximum = INT_MAX;
    } else {
        dbo->fields.select.maximum = atoi(upper);
    }
    return dbo;
}
