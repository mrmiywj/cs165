#include "api/db_io.h"
#include "query/execute.h"
#include "util/debug.h"
#include "util/cleanup.h"

Db* current_db = NULL;

/** execute_DbOperator takes as input the DbOperator and executes the query. **/
char* executeDbOperator(DbOperator* query, message* send_message) {
    if (query == NULL)
        return "Invalid query.";

    printDbOperator(query);

    char* res = NULL;
    switch(query->type) {
    case CREATE:
        res = handleCreateQuery(query, send_message);
    case INSERT:
        res = handleInsertQuery(query, send_message);
    case LOADER:
        res = handleLoaderQuery(query, send_message);
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
            current_db->tables = realloc(current_db->tables, new_num * sizeof(Table*));
        }
        Table* new_table = malloc(sizeof(Table*));
        strcpy(new_table->name, tbl_name);
        new_table->columns = NULL;
        new_table->col_count = 0;
        new_table->table_length = 0;
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

        // find table and update column
        bool updated = false;
        for (size_t i = 0; i < current_db->num_tables; i++) {
            if (strcmp(current_db->tables[i]->name, tbl_name) != 0) { 
                continue;
            }
            
            // found the correct table
            Table* tbl = current_db->tables[i];
            if (tbl->columns == NULL) {
                tbl->columns = malloc(sizeof(Column*));
            } else {
                size_t new_num = tbl->col_count + 1;
                tbl->columns = realloc(tbl->columns, new_num * sizeof(Column*));
            }
            Column* new_col = malloc(sizeof(Column*));
            strcpy(new_col->name, col_name);
            new_col->data = NULL;
            new_col->index = NULL;
            tbl->columns[tbl->col_count] = new_col;
            tbl->col_count++;
            updated = true;
            break;
        }

        // if we didn't manage to find a table
        if (updated == false) {
            send_message->status = INCORRECT_FORMAT;
            return "-- Unable to find specified table.";
        }

        // finished successfully
        send_message->status = OK_DONE;
        return "-- Successfully created column.";
    default:
        break;
    }
    return "Not implemented yet.";
}
char* handleInsertQuery(DbOperator* query, message* send_message) {
    (void) query;
    (void) send_message;
    return "Not implemented yet.";
}
char* handleLoaderQuery(DbOperator* query, message* send_message) {
    (void) query;
    (void) send_message;
    return "Not implemented yet.";
}
