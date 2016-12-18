#include <string.h>
#include <stdio.h>

#include "parse/create.h"
#include "util/log.h"
#include "util/strmanip.h"

// handle a create query
DbOperator* parse_create(char* arguments, message* response) {
    if (response == NULL)
        return NULL;
    if (arguments == NULL || *arguments != '(') {
        response->status = UNKNOWN_COMMAND;
        return NULL;
    }
    
    // create a copy of string
    size_t space = strlen(arguments) + 1;
    char *copy = malloc(space * sizeof(char));
    strcpy(copy, arguments);
    
    // parse arguments
    copy++;
    char* token = strsep(&copy, ",");
    if (token == NULL) {
        // invalid query format
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    
    // check for possible create queries
    if (strcmp(token, "db") == 0)
        return parse_create_db(copy, response);
    if (strcmp(token, "tbl") == 0)
        return parse_create_tbl(copy, response);
    if (strcmp(token, "col") == 0)
        return parse_create_col(copy, response);
    if (strcmp(token, "idx") == 0)
        return parse_create_idx(copy, response);
    
    // fall-through; if no valid operator has been returned, return NULL
    response->status = UNKNOWN_COMMAND;
    return NULL;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/

DbOperator* parse_create_db(char* arguments, message* response) {
    response->status = OK_DONE;
    char *db_name = strsep(&arguments, ",");
    
    // check for database name
    if (db_name == NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    
    // create the database with given name
    db_name = trim_quotes(db_name);
    int last_char = strlen(db_name) - 1;
    if (last_char < 0 || db_name[last_char] != ')') {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    db_name[last_char] = '\0';

    // ensure no more arguments
    if (strsep(&arguments, ",") != NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    
    // create DbOperator and return
    DbOperator* result = malloc(sizeof(DbOperator));
    result->type = OP_CREATE;
    char** params = malloc(sizeof(char*));
    params[0] = db_name;
    result->fields.create = (CreateOperator) {
        .type = CREATE_DB, 
        .params = params, 
        .num_params = 1
    };
    return result;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/

DbOperator* parse_create_tbl(char* arguments, message* response) {
    response->status = OK_DONE;
    char** arguments_index = &arguments;
    char* table_name = next_token(arguments_index, &(response->status));
    char* db_name = next_token(arguments_index, &(response->status));
    char* col_cnt = next_token(arguments_index, &(response->status));

    table_name = trim_quotes(table_name);
    // not enough arguments
    if (response->status == INCORRECT_FORMAT) {
        return NULL;
    }

    // read and chop off last char
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    col_cnt[last_char] = '\0';

    // get column count for table
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    
    // create DbOperator and return
    DbOperator* result = malloc(sizeof(DbOperator));
    result->type = OP_CREATE;
    char** params = malloc(3 * sizeof(char*));
    params[0] = db_name;
    params[1] = table_name;
    params[2] = col_cnt;
    result->fields.create = (CreateOperator) {
        .type = CREATE_TBL, 
        .params = params,
        .num_params = 3
    };
    return result;
}

/**
 * This method takes in a string representing the arguments to create a column.
 * It parses those arguments, checks that they are valid, and creates a column.
 **/

DbOperator* parse_create_col(char* arguments, message* response) {
    response->status = OK_DONE;
    char** arguments_index = &arguments;
    char* col_name = next_token(arguments_index, &(response->status));
    char* table_path = next_token(arguments_index, &(response->status));

    col_name = trim_quotes(col_name);
    // not enough arguments
    if (response->status == INCORRECT_FORMAT) {
        return NULL;
    }

    // read and chop off last char
    int last_char = strlen(table_path) - 1;
    if (table_path[last_char] != ')') {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    table_path[last_char] = '\0';

    // pull out database and table from table_path
    char* db_name = trim_quotes(table_path);
    char* tbl_name = table_path;
    while (*tbl_name != '\0') {
        if (*tbl_name == '.') {
            *tbl_name++ = '\0';
            break;
        }
        tbl_name++;
    }

    // create DbOperator and return
    DbOperator* result = malloc(sizeof(DbOperator));
    result->type = OP_CREATE;
    char** params = malloc(3 * sizeof(char*));
    params[0] = db_name;
    params[1] = tbl_name;
    params[2] = col_name;
    result->fields.create = (CreateOperator) {
        .type = CREATE_COL, 
        .params = params,
        .num_params = 3
    };
    return result;
}

/**
 * This method takes in a string representing the arguments to create an index.
 * It parses those arguments, checks that they are valid, and creates an index.
 **/

DbOperator* parse_create_idx(char* arguments, message* response) {
    response->status = OK_DONE;
    char** arguments_index = &arguments;
    char* col_path = next_token(arguments_index, &(response->status));
    char* type = next_token(arguments_index, &(response->status));
    char* cluster = next_token(arguments_index, &(response->status));
    
    // check for # of arguments
    if (response->status == INCORRECT_FORMAT)
        return NULL;
    if (*arguments_index != NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    // check and chop off ending parenthesis
    int last_char = strlen(cluster) - 1;
    if (cluster[last_char] != ')') {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    cluster[last_char] = '\0';

    // pull out database and table from table_path
    char* db_name = strsep(&col_path, ".");
    if (db_name == NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    char* tbl_name = strsep(&col_path, ".");
    if (tbl_name == NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }
    char* col_name = strsep(&col_path, ".");
    if (col_name == NULL || col_path != NULL) {
        response->status = INCORRECT_FORMAT;
        return NULL;
    }

    // create DbOperator and return
    DbOperator* result = malloc(sizeof(DbOperator));
    result->type = OP_CREATE;
    char** params = malloc(5 * sizeof(char*));
    params[0] = db_name;
    params[1] = tbl_name;
    params[2] = col_name;
    params[3] = type;
    params[4] = cluster;
    result->fields.create = (CreateOperator) {
        .type = CREATE_IDX, 
        .params = params,
        .num_params = 5
    };
    return result;
}
