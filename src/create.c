#include "cs165_api.h"
#include "utils.h"

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL)
        *status = INCORRECT_FORMAT;
    return token;
}

// handle a create query
DbOperator* create(char* arguments, message* response) {
    size_t space = strlen(arguments) + 1;
    char *copy = malloc(space * sizeof(char));
    strcpy(copy, arguments);
    
    // move past (
    if (strncmp(copy, "(", 1) == 0)
        copy++;
    else {
        response->status = UNKNOWN_COMMAND;
        return NULL;
    }

    char* token = strsep(&copy, ",");
    if (token == NULL) {
        // invalid query format
        response->status = INCORRECT_FORMAT;
        return NULL;
    } else {
        // check for possible create queries
        if (strcmp(token, "db") == 0)
            return parse_create_db(copy, response);
        if (strcmp(token, "tbl") == 0)
            return parse_create_tbl(copy, response);
        if (strcmp(token, "col") == 0)
            return parse_create_col(copy, response);
        
        // fall-through; if nothing works yet, return UNKNOWN
        response->status = UNKNOWN_COMMAND; 
    }
    
    // fall-through; if no valid operator has been returned, return NULL
    return NULL;
}

DbOperator* parse_create_db(char* arguments, message* response) {
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
    
    if (add_db(db_name, true).code == OK) {
        return OK_DONE;
    } else {
        return EXECUTION_ERROR;
    }
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/

message_status parse_create_tbl(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    table_name = trim_quotes(table_name);
    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }

    // read and chop off last char
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    col_cnt[last_char] = '\0';

    // check that the database argument is the current active database
    if (strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return QUERY_UNSUPPORTED;
    }

    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    Status create_status;
    create_table(current_db, table_name, column_cnt, &create_status);
    if (create_status.code != OK) {
        cs165_log(stdout, "adding a table failed.");
        return EXECUTION_ERROR;
    }

    return status;
}

/**
 * This method takes in a string representing the arguments to create a column.
 * It parses those arguments, checks that they are valid, and creates a column.
 **/

message_status parse_create_col(char* create_arguments) {
    message_status status = OK_DONE;

    printf("Arguments: %s\n", create_arguments);

    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &status);
    char* table_path = next_token(create_arguments_index, &status);

    col_name = trim_quotes(col_name);
    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }

    // read and chop off last char
    int last_char = strlen(table_path) - 1;
    if (table_path[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    table_path[last_char] = '\0';

    // pull out database and table from table_path
    char* db_name = table_path;
    char* tbl_name = table_path;
    while (*tbl_name != '\0') {
        if (*tbl_name == '.') {
            *tbl_name++ = '\0';
            break;
        }
        tbl_name++;
    }

    // check that the database argument is the current active database
    // if (strcmp(current_db->name, db_name) != 0) {
    //     cs165_log(stdout, "query unsupported. Bad db name");
    //     return QUERY_UNSUPPORTED;
    // }

    Status create_status;
    printf("tbl_name: %s, col_name: %s\n", tbl_name, col_name);
    create_column(col_name, tbl_name, false, &create_status);
    if (create_status.code != OK) {
        cs165_log(stdout, "adding a column failed.");
        return EXECUTION_ERROR;
    }

    return status;
}