#include "parse/insert.h"
#include "util/log.h"
#include "util/strmanip.h"

DbOperator* parse_insert(char* arguments, message* response) {
    if (response == NULL)
        return NULL;
    if (arguments == NULL || *arguments != '(') {
        response->status = UNKNOWN_COMMAND;
        return NULL;
    }
    
    // parse arguments
    arguments++;
    char** command_index = &arguments;
    char* table_name = next_token(command_index, &(response->status));
    if (response->status == INCORRECT_FORMAT) {
        return NULL;
    }
    while (*table_name != '\0' && *table_name != '.')
        table_name++;
    table_name++;
    
    // create insert operator object
    DbOperator* dbo = calloc(1, sizeof(DbOperator));
    dbo->type = INSERT;
    dbo->fields.insert.tbl_name = table_name;
    
    // get values from query
    size_t columns_inserted = 1;
    char* cpy = arguments;
    while (*cpy != '\0') {
        if (*cpy == ',')
            columns_inserted++;
        cpy++;
    }
    dbo->fields.insert.num_values = columns_inserted;
    if (dbo->fields.insert.values == NULL) {
        dbo->fields.insert.values = calloc(columns_inserted, sizeof(int));
    } else {
        int* new_data = realloc(dbo->fields.insert.values, sizeof(int) * columns_inserted);
        if (new_data == NULL) {
            response->status = EXECUTION_ERROR;
            return NULL;
        }
        dbo->fields.insert.values = new_data;
    }   
 
    // iterate down values and insert one by one
    int index = 0;
    char* token = NULL;
    while ((token = strsep(command_index, ",")) != NULL) {
        int insert_val = atoi(token);
        dbo->fields.insert.values[index] = insert_val;
        index++;
    }

    return dbo;
}
