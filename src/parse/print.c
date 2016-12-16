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

    // search for number of arguments
    size_t num_args = 1;
    for (size_t i = 0; i < strlen(copy); i++)
        num_args += copy[i] == ',';
    
    // allocate enough space and place tokens in array
    char** handles = malloc(sizeof(char*) * num_args);
    for (size_t i = 0; i < num_args; i++)
        handles[i] = strsep(&copy, ",");

    // create print operator object
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = PRINT;
    dbo->fields.print.handles = handles;
    dbo->fields.print.num_params = num_args;
    return dbo;
}
