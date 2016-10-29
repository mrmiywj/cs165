#include "api/db_io.h"
#include "query/execute.h"
#include "util/debug.h"
#include "util/cleanup.h"

Db* current_db = NULL;

Table* findTable(char* tbl_name) {
    if (current_db == NULL || tbl_name == NULL)
        return NULL;
    for (size_t i = 0; i < current_db->num_tables; i++)
        if (strcmp(current_db->tables[i]->name, tbl_name) == 0)
            return current_db->tables[i];
    return NULL;
}

/** execute_DbOperator takes as input the DbOperator and executes the query. **/
char* executeDbOperator(DbOperator* query, message* send_message) {
    if (query == NULL) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query.";
    }

    printDbOperator(query);

    char* res = NULL;
    switch(query->type) {
    case CREATE:
        res = handleCreateQuery(query, send_message);
        break;
    case INSERT:
        res = handleInsertQuery(query, send_message);
        break;
    case LOADER:
        res = handleLoaderQuery(query, send_message);
        break;
    default:
        break;
    }

    printDatabase(current_db);

    if (res != NULL)
        return res;

    free(query);
    
    return "Executed query successfully.";
}

// ================ HANDLERS ================
char* handleCreateQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != CREATE) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query.";
    }
    char* db_name;
    char* tbl_name;
    char* col_name;
    
    OperatorFields fields = query->fields;
    switch (fields.create.type) {
    case CREATE_DATABASE:
        // check for params
        if (fields.create.num_params != 1) {
            send_message->status = INCORRECT_FORMAT;
            return "-- Incorrect number of arguments when creating db.";
        }

        // extract params and validate
        db_name = fields.create.params[0];
        if (current_db != NULL && strcmp(current_db->name, db_name) == 0) {
            send_message->status = EXECUTION_ERROR;
            return "-- Error creating db.";
        }

        // create the actual database directory
        if (createDatabase(db_name) != 0) {
            send_message->status = EXECUTION_ERROR;
            return "-- Error creating db.";   
        }

        // destroy current db and replace with new db object
        freeDb(current_db);
        current_db = malloc(sizeof(Db));
        strcpy(current_db->name, db_name);
        current_db->tables = NULL;
        current_db->num_tables = 0;
        current_db->tables_size = 0;
        current_db->tables_capacity = 0;

        // finished successfully
        send_message->status = OK_DONE;
        return "-- Successfully created db.";
    case CREATE_TABLE:
        log_info("Creating table...\n");
        // check for params
        if (fields.create.num_params != 3) {
            send_message->status = INCORRECT_FORMAT;
            return "-- Incorrect number of arguments when creating db.";
        }

        // extract params and validate
        db_name = fields.create.params[0];
        tbl_name = fields.create.params[1];
        int column_count = atoi(fields.create.params[2]);
        if (column_count <= 0) {
            send_message->status = INCORRECT_FORMAT;
            return "-- At least one column required.";
        }

        // create the table directory
        if (createTable(db_name, tbl_name) != 0) {
            send_message->status = EXECUTION_ERROR;
            return "-- Error creating table.";
        }

        // update the db index
        if (current_db->tables == NULL) {
            current_db->tables = malloc(sizeof(Table*));
        } else {
            size_t new_num = current_db->num_tables + 1;
            Table** new_tables = realloc(current_db->tables, new_num * sizeof(Table*));
            if (new_tables == NULL) {
                send_message->status = EXECUTION_ERROR;
                return "-- Unable to create new table.";
            }
            current_db->tables = new_tables;
        }
        Table* new_table = malloc(sizeof(Table));
        strcpy(new_table->name, tbl_name);
        new_table->columns = NULL;
        new_table->col_count = 0;
        new_table->num_rows = 0;
        new_table->capacity = 0;
        current_db->tables[current_db->num_tables] = new_table;
        current_db->num_tables++;

        // finished successfully
        send_message->status = OK_DONE;
        return "-- Successfully created table.";
    case CREATE_COLUMN:
        // check for params
        if (fields.create.num_params != 3) {
            send_message->status = INCORRECT_FORMAT;
            return "-- Incorrect number of arguments when creating db.";
        }
        
        // extract params and validate
        db_name = fields.create.params[0];
        tbl_name = fields.create.params[1];
        col_name = fields.create.params[2];
        if (strcmp(current_db->name, db_name) != 0) {
            send_message->status = QUERY_UNSUPPORTED;
            return "-- Cannot create column in inactive db.";
        }

        // create a column file
        if (createColumn(db_name, tbl_name, col_name) != 0) {
            send_message->status = EXECUTION_ERROR;
            return "-- Error creating column.";
        }

        // find table
        Table* table = findTable(tbl_name);
        
        // if we didn't manage to find a table
        if (table == NULL) {
            send_message->status = INCORRECT_FORMAT;
            return "-- Unable to find specified table.";
        } 
        
        // found the correct table, insert new column
        if (table->columns == NULL) {
            table->columns = malloc(sizeof(Column*));
        } else {
            size_t new_num = table->col_count + 1;
            Column** new_cols = realloc(table->columns, new_num * sizeof(Column*)); 
            if (new_cols == NULL) {
                send_message->status = EXECUTION_ERROR;
                return "-- Unable to create new column.";
            }
            table->columns = new_cols;
        }
        Column* new_col = malloc(sizeof(Column*));
        strcpy(new_col->name, col_name);
        new_col->data = NULL;
        new_col->index = NULL;
        table->columns[table->col_count] = new_col;
        table->col_count++;

        // finished successfully
        send_message->status = OK_DONE;
        return "-- Successfully created column.";
    default:
        break;
    }
    return "Not implemented yet.";
}
char* handleInsertQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != INSERT) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query."; 
    }

    // retrieve params
    char* db_name = current_db->name;
    // TODO: USE DB_NAME SOMEHOW
    (void) db_name;
    char* tbl_name = query->fields.insert.tbl_name;
    int* values = query->fields.insert.values;

    // find table
    Table* table = findTable(tbl_name);

    // if we didn't manage to find a table
    if (table == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified table.";
    }

    // check to make sure number of values and columns match
    if (table->col_count != query->fields.insert.num_values) {
        send_message->status = INCORRECT_FORMAT;
        return "-- Mismatched number of values inserted.";
    }

    // found the correct table, insert new row
    size_t num_rows = table->num_rows;
    for (size_t i = 0; i < table->col_count; i++) {
        Column* col = table->columns[i];
        if (col->data == NULL) {
            col->data = malloc(sizeof(int) * COL_INITIAL_SIZE);
            table->capacity = COL_INITIAL_SIZE;
        }
        if (num_rows == table->capacity) {
            int* new_data = realloc(col->data, table->capacity * COL_RESIZE_FACTOR * sizeof(int));
            if (new_data == NULL) {
                send_message->status = EXECUTION_ERROR;
                return "-- Unable to insert a new row.";
            }
            col->data = new_data;
            table->capacity *= COL_RESIZE_FACTOR;
        }
        col->data[num_rows] = values[i];
    }
    table->num_rows++;

    send_message->status = OK_DONE;
    return "Successfully inserted new row.";
}
char* handleLoaderQuery(DbOperator* query, message* send_message) {
    (void) query;
    (void) send_message;
    return "Not implemented yet.";
}
