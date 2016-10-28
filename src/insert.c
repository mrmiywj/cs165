#include "insert.h"
#include "client_context.h"
#include "utils.h"

DbOperator* insert(char* arguments, message* response) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    if (strncmp(arguments, "(", 1) == 0) {
        arguments++;
        char** command_index = &arguments;
        char* table_name = next_token(command_index, &response->status);
        if (response->status == INCORRECT_FORMAT) {
            return NULL;
        }
        // lookup the table and make sure it exists. 
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            response->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->fields.insert.table = insert_table;
        dbo->fields.insert.values = malloc(sizeof(int) * insert_table->col_count);
        while ((token = strsep(command_index, ",")) != NULL) {
            // NOT ERROR CHECKED. COULD WRITE YOUR OWN ATOI. (ATOI RETURNS 0 ON NON-INTEGER STRING)
            int insert_val = atoi(token);
            dbo->fields.insert.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            response->status = INCORRECT_FORMAT;
            free(dbo);
            return NULL;
        } 
        return dbo;
    } else {
        response->status = UNKNOWN_COMMAND;
        return NULL;
    }
}
