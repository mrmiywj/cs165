#include "api/db_io.h"
#include "api/context.h"
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

Column* findColumn(Table* table, char* col_name) {
    if (current_db == NULL || table == NULL || col_name == NULL)
        return NULL;
    for (size_t i = 0; i < table->col_count; i++) {
        if (strcmp(table->columns[i]->name, col_name) == 0)
            return table->columns[i];
    }
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
    case SELECT:
        res = handleSelectQuery(query, send_message);
        break;
    case FETCH:
        res = handleFetchQuery(query, send_message);
        break;
    case PRINT:
        res = handlePrintQuery(query, send_message);
        break;
    default:
        break;
    }

    printDatabase(current_db);
    
    free(query);
    if (res != NULL)
        return res;
    return NULL;
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

        // if we didn't manage to find a table
        Table* table = findTable(tbl_name);
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
        Column* new_col = malloc(sizeof(Column));
        strcpy(new_col->name, col_name);
        new_col->data = NULL;
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

    // if we didn't manage to find a table
    Table* table = findTable(tbl_name);
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
    if (query == NULL || query->type != LOADER) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query."; 
    }
    return "Not implemented yet.";
}

char* handleSelectQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != SELECT) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query."; 
    }

    // retrieve params
    SelectOperator select = query->fields.select;
    char* db_name = select.db_name;
    char* tbl_name = select.tbl_name;
    char* col_name = select.col_name;
    char* var_name = select.var_name;
    int minimum = select.minimum;
    int maximum = select.maximum;
    
    // check database
    if (strcmp(db_name, current_db->name) != 0) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Database not found.";
    }

    // if we didn't manage to find a table
    Table* table = findTable(tbl_name);
    if (table == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified table.";
    }

    // if we didn't manage to find a column
    Column* column = findColumn(table, col_name);
    if (column == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified column.";
    }

    // create a new GeneralizedColumnHandle
    GeneralizedColumnHandle new_handle;
    GeneralizedColumnPointer new_pointer;
    new_pointer.result = malloc(sizeof(Result));
    new_pointer.result->data_type = INT;
    new_pointer.result->num_tuples = 0;
    new_pointer.result->payload = NULL;
    GeneralizedColumn gen_column = {
        .column_type = RESULT,
        .column_pointer = new_pointer
    };
    new_handle.generalized_column = gen_column;
    strcpy(new_handle.name, var_name);

    // scan through column and store all data in tuples
    int capacity = 0;
    int num_inserted = 0;
    int* data = NULL;
    for (size_t i = 0; i < table->num_rows; i++) {
        if (column->data[i] < minimum || column->data[i] > maximum)
            continue;
        if (num_inserted == capacity) {
            capacity = (capacity == 0) ? 1 : 2 * capacity;
            int* new_data = realloc(data, sizeof(int) * capacity);
            if (new_data == NULL) {
                free(new_data);
                free(new_pointer.result);
                send_message->status = EXECUTION_ERROR;
                return "-- Error calculating result array.";
            }
            data = new_data;
        }
        data[num_inserted] = i;
        num_inserted++;
    }
    new_pointer.result->payload = data;
    new_pointer.result->num_tuples = num_inserted;

    // search for context and add to the list of variables
    ClientContext* context = searchContext(query->client_fd);
    if (context == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Error finding client context for search.";
    }
    if (context->chandles_in_use == context->chandle_slots) {
        int new_size = (context->chandle_slots == 0) ? 1 : 2 * context->chandle_slots;
        GeneralizedColumnHandle* new_table = realloc(context->chandle_table, new_size * sizeof(GeneralizedColumnHandle));
        if (new_table == NULL) {
            send_message->status = EXECUTION_ERROR;
            return "-- Problem inserting new handle into client context.";
        }
        context->chandle_table = new_table;
        context->chandle_slots = new_size;
    }
    context->chandle_table[context->chandles_in_use++] = new_handle;

    send_message->status = OK_DONE;
    return "Successfully inserted new row.";
}

char* handleFetchQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != FETCH) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query."; 
    }

    // retrieve params
    FetchOperator fetch = query->fields.fetch;
    char* db_name = fetch.db_name;
    char* tbl_name = fetch.tbl_name;
    char* col_name = fetch.col_name;
    char* source = fetch.source;
    char* target = fetch.target;
    
    // check database
    if (strcmp(db_name, current_db->name) != 0) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Database not found.";
    }

    // if we didn't manage to find a table
    Table* table = findTable(tbl_name);
    if (table == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified table.";
    }

    // if we didn't manage to find a column
    Column* column = findColumn(table, col_name);
    if (column == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified column.";
    }

    // get context for current client
    ClientContext* context = searchContext(query->client_fd);
    if (context == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find context for current client.";
    }

    // search for source indices in context
    GeneralizedColumnHandle* src_handle = findHandle(context, source);
    if (src_handle == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified select source.";
    }

    // create a new GeneralizedColumnHandle
    GeneralizedColumnHandle new_handle;
    GeneralizedColumnPointer new_pointer;
    new_pointer.result = malloc(sizeof(Result));
    new_pointer.result->data_type = INT;
    new_pointer.result->num_tuples = 0;
    new_pointer.result->payload = NULL;
    GeneralizedColumn gen_column = {
        .column_type = RESULT,
        .column_pointer = new_pointer
    };
    new_handle.generalized_column = gen_column;
    strcpy(new_handle.name, target);

    // scan through column and store all data in tuples
    int num_tuples = src_handle->generalized_column.column_pointer.result->num_tuples;
    int* indices = (int*) src_handle->generalized_column.column_pointer.result->payload;
    int* data = malloc(sizeof(int) * num_tuples);
    for (int i = 0; i < num_tuples; i++) {
        data[i] = column->data[indices[i]];
    }
    new_pointer.result->payload = data;
    new_pointer.result->num_tuples = num_tuples;

    // search for context and add to the list of variables
    if (checkContextSize(context) != true) {
        send_message->status = EXECUTION_ERROR;
        return "-- Problem inserting new handle into client context.";
    }
    context->chandle_table[context->chandles_in_use++] = new_handle;

    send_message->status = OK_DONE;
    return "Successfully fetched data from select query.";
}

char* handlePrintQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != PRINT) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query."; 
    }

    // // retrieve params
    PrintOperator print = query->fields.print;
    char* handle = print.handle;
    
    // get context for current client
    ClientContext* context = searchContext(query->client_fd);
    if (context == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find context for current client.";
    }

    // search for result in context
    Result* result = findHandle(context, handle)->generalized_column.column_pointer.result;
    if (result == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified select source.";
    }

    // create string payload to return
    int length = 0;
    char buf[64];
    switch (result->data_type) {
        case INT: {
            int* data = (int*) result->payload;
            for (size_t i = 0; i < result->num_tuples; i++) {
                sprintf(buf, "%i", data[i]);
                length += strlen(buf) + 1;
            }
            break;
        }
        case LONG: {
            long* data = (long*) result->payload;
            for (size_t i = 0; i < result->num_tuples; i++) {
                sprintf(buf, "%ld", data[i]);
                length += strlen(buf) + 1;
            }
            break;
        }
        case FLOAT: {
            float* data = (float*) result->payload;
            for (size_t i = 0; i < result->num_tuples; i++) {
                sprintf(buf, "%f", data[i]);
                length += strlen(buf) + 1;
            }
            break;
        }
        default:
            break;
    }
    char* values = malloc(sizeof(char) * (length + 1));
    switch (result->data_type) {
        case INT: {
            int* data = (int*) result->payload;
            for (size_t i = 0; i < result->num_tuples; i++) {
                sprintf(values, "%s%i\n", values, data[i]);
            }
            break;
        }
        case LONG: {
            long* data = (long*) result->payload;
            for (size_t i = 0; i < result->num_tuples; i++) {
                sprintf(values, "%s%ld\n", values, data[i]);
            }
            break;
        }
        case FLOAT: {
            float* data = (float*) result->payload;
            for (size_t i = 0; i < result->num_tuples; i++) {
                sprintf(values, "%s%f\n", values, data[i]);
            }
            break;
        }
    }
    values[length] = '\0';
    printf("result printf: %s\n", values);

    send_message->status = OK_WAIT_FOR_RESPONSE;
    send_message->length = length;
    return values;
}
