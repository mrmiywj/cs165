#include "parse/insert.h"
#include "util/log.h"
#include "util/strmanip.h"

DbOperator* parse_insert(char* arguments, message* response) {
    char* token = NULL;
    if (strncmp(arguments, "(", 1) == 0) {
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
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->fields.insert.tbl_name = table_name;
        
        // get values from query
        size_t columns_inserted = 1;
        char* cpy = arguments;
        while (*cpy != '\0') {
            printf("%c", *cpy);
            if (*cpy == ',')
                columns_inserted++;
            cpy++;
        }
        dbo->fields.insert.num_values = columns_inserted;
        dbo->fields.insert.values = malloc(sizeof(int) * columns_inserted);
        int index = 0;
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->fields.insert.values[index] = insert_val;
            index++;
        }

        return dbo;
    } else {
        response->status = UNKNOWN_COMMAND;
        return NULL;
    }
}
