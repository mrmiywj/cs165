#include <string.h>
#include <stdio.h>

#include "parse/math.h"
#include "util/log.h"
#include "util/strmanip.h"

DbOperator* parse_math(char* arguments, message* response, char* handle, MathType type) {
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
    
    // possible to have two arguments to the operator
    char* first;
    char* second = copy;

    // parse arguments, look for two if necessary
    if (type > MIN) {
        first = (char*) strsep(&second, ",");
        if (first == NULL) {
            response->status = INCORRECT_FORMAT;
            return NULL;
        }
    } else {
        first = second;
        second = NULL;
    }
    char** params = malloc(sizeof(char*) * 7);
    bool is_var = false;
    int num_tokens = 0;
    if (first == NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    // parse the first argument
    params[num_tokens] = (char*) strsep(&first, ".");
    if (first == NULL) {
        num_tokens += 1;
        is_var = true;
    } else {
        params[num_tokens + 1] = (char*) strsep(&first, ".");
        if (first == NULL) {
            response->status = INCORRECT_FORMAT;
            free(params);
            return NULL;
        } else {
            params[num_tokens + 2] = first;
        }
        num_tokens += 3;
    }
    // parse the second argument
    if (second != NULL) {
        params[num_tokens] = (char*) strsep(&second, ".");
        if (second == NULL) {
            params[num_tokens] = second;
            num_tokens += 1;
        } else {
            params[num_tokens + 1] = (char*) strsep(&second, ".");
            if (second == NULL) {
                response->status = INCORRECT_FORMAT;
                free(params);
                return NULL;
            } else {
                params[num_tokens + 2] = second;
            }
            num_tokens += 3;
        }
    }

    // create select operator object
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = MATH;
    dbo->fields.math.type = type;
    dbo->fields.math.handle = handle;
    dbo->fields.math.params = params;
    dbo->fields.math.num_params = num_tokens;
    dbo->fields.math.is_var = is_var;
    return dbo;
}
