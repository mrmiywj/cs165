#include <string.h>
#include <stdio.h>
#include <limits.h>

#include "parse/select.h"

DbOperator* parse_select(char* arguments, message* response, char* handle) {
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
    char* arg1 = (char*) strsep(&copy, ",");
    if (arg1 == NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    char* arg2 = (char*) strsep(&copy, ",");
    if (arg2 == NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    char* arg3 = (char*) strsep(&copy, ",");
    if (arg3 == NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    char* arg4 = copy;

    // create select operator object
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = OP_SELECT;
    if (arg4 == NULL) {
        char** params = malloc(sizeof(char*) * 3);
        params[0] = (char*) strsep(&arg1, ".");
        params[1] = (char*) strsep(&arg1, ".");
        params[2] = arg1;
        dbo->fields.select = (SelectOperator) {
            .handle = handle,
            .params = params,
            .minimum = (strcmp("null", arg2) == 0) ? INT_MIN : atoi(arg2),
            .maximum = (strcmp("null", arg3) == 0) ? INT_MAX : atoi(arg3),
            .src_is_var = false
        };
    } else {
        char** params = malloc(sizeof(char*) * 2);
        params[0] = arg1;
        params[1] = arg2;
        dbo->fields.select = (SelectOperator) {
            .handle = handle,
            .params = params,
            .minimum = (strcmp("null", arg3) == 0) ? INT_MIN : atoi(arg3),
            .maximum = (strcmp("null", arg4) == 0) ? INT_MAX : atoi(arg4),
            .src_is_var = true
        };
    }
    return dbo;
}
