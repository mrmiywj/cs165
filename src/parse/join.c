#include <string.h>
#include <stdio.h>

#include "parse/join.h"
#include "util/log.h"
#include "util/strmanip.h"

DbOperator* parse_join(char* arguments, message* response, char* handles) {
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
    
    // pull first and second handles out
    char* handle1 = handles;
    char* handle2 = handle1;
    while (*handle2 != ',' && *handle2 != '\0')
        handle2++;
    if (*handle2 == '\0') {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    *handle2 = '\0';

    // parse select and fetch handles
    char* fetch1 = strsep(&copy, ",");
    if (fetch1 == NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    char* select1 = strsep(&copy, ",");
    if (select1 == NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    char* fetch2 = strsep(&copy, ",");
    if (fetch2 == NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    char* select2 = strsep(&copy, ",");
    if (select2 == NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    char* type = copy;
    if (type == NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }

    // create select operator object
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = OP_JOIN;
    dbo->fields.join = (JoinOperator) {
        .type = strcmp(type, "hash") == 0 ? HASH : NESTED,
        .fetch1 = fetch1,
        .select1 = select1,
        .fetch2 = fetch2,
        .select2 = select2,
        .handle1 = handle1,
        .handle2 = handle2
    };
    return dbo;
}
