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
    case OP_CREATE:
        res = handleCreateQuery(query, send_message);
        break;
    case OP_INSERT:
        res = handleInsertQuery(query, send_message);
        break;
    case OP_SELECT:
        res = handleSelectQuery(query, send_message);
        break;
    case OP_FETCH:
        res = handleFetchQuery(query, send_message);
        break;
    case OP_PRINT:
        res = handlePrintQuery(query, send_message);
        break;
    case OP_MATH:
        res = handleMathQuery(query, send_message);
        break;
    default:
        break;
    }

    printDatabase(current_db);
    
    free(query);
    if (res != NULL)
        return res;
    return "";
}

// ================ HANDLERS ================
char* handleCreateQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != OP_CREATE) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query.";
    }
    char* db_name;
    char* tbl_name;
    char* col_name;
    
    OperatorFields fields = query->fields;
    switch (fields.create.type) {
    case CREATE_DB:
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
    
    case CREATE_TBL:
        log_info("Creating table...\n");
        // check for params
        if (fields.create.num_params != 3) {
            send_message->status = INCORRECT_FORMAT;
            return "-- Incorrect number of arguments when creating table.";
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
            current_db->tables = calloc(1, sizeof(Table*));
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
        new_table->indexes = NULL;
        new_table->col_count = 0;
        new_table->num_rows = 0;
        new_table->capacity = 0;
        current_db->tables[current_db->num_tables++] = new_table;

        // finished successfully
        send_message->status = OK_DONE;
        return "-- Successfully created table.";
    
    case CREATE_COL:
        // check for params
        if (fields.create.num_params != 3) {
            send_message->status = INCORRECT_FORMAT;
            return "-- Incorrect number of arguments when creating column.";
        }
        
        // extract params and validate
        db_name = fields.create.params[0];
        tbl_name = fields.create.params[1];
        col_name = fields.create.params[2];
        if (strcmp(current_db->name, db_name) != 0) {
            send_message->status = QUERY_UNSUPPORTED;
            return "-- Cannot create index in inactive db.";
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
            table->columns = calloc(1, sizeof(Column*));
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
    
    case CREATE_IDX:
        // check for params
        if (fields.create.num_params != 5) {
            send_message->status = INCORRECT_FORMAT;
            return "-- Incorrect number of arguments when creating index.";
        }
        
        // extract params and validate
        db_name = fields.create.params[0];
        tbl_name = fields.create.params[1];
        col_name = fields.create.params[2];
        // if (strcmp(current_db->name, db_name) != 0) {
        //     send_message->status = QUERY_UNSUPPORTED;
        //     return "-- Cannot create column in inactive db.";
        // }

        // // create a column file
        // if (createColumn(db_name, tbl_name, col_name) != 0) {
        //     send_message->status = EXECUTION_ERROR;
        //     return "-- Error creating column.";
        // }

        // // if we didn't manage to find a table
        // Table* table = findTable(tbl_name);
        // if (table == NULL) {
        //     send_message->status = INCORRECT_FORMAT;
        //     return "-- Unable to find specified table.";
        // } 
        
        // // found the correct table, insert new column
        // if (table->columns == NULL) {
        //     table->columns = calloc(1, sizeof(Column*));
        // } else {
        //     size_t new_num = table->col_count + 1;
        //     Column** new_cols = realloc(table->columns, new_num * sizeof(Column*)); 
        //     if (new_cols == NULL) {
        //         send_message->status = EXECUTION_ERROR;
        //         return "-- Unable to create new column.";
        //     }
        //     table->columns = new_cols;
        // }
        // Column* new_col = malloc(sizeof(Column));
        // strcpy(new_col->name, col_name);
        // new_col->data = NULL;
        // table->columns[table->col_count] = new_col;
        // table->col_count++;

        // // finished successfully
        // send_message->status = OK_DONE;
        // return "-- Successfully created column.";
    
    default:
        break;
    }
    return "Not implemented yet.";
}

char* handleInsertQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != OP_INSERT) {
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
        if (num_rows == table->capacity) {
            log_info("-- Resizing table columns...\n");
            bool was_empty = false;
            for (size_t j = 0; j < table->col_count; j++) {
                Column* curr_col = table->columns[j];
                if (curr_col->data == NULL) {
                    curr_col->data = calloc(COL_INITIAL_SIZE, sizeof(int));
                    was_empty = true;
                } else {
                    int* new_data = realloc(curr_col->data, table->capacity * COL_RESIZE_FACTOR * sizeof(int));
                    if (new_data == NULL) {
                        send_message->status = EXECUTION_ERROR;
                        return "-- Unable to insert a new row.";
                    }
                    curr_col->data = new_data;
                }
            }
            if (was_empty == true) {
                table->capacity = COL_INITIAL_SIZE;
            } else {
                table->capacity *= COL_RESIZE_FACTOR;
            }
        }
        col->data[num_rows] = values[i];
    }
    table->num_rows++;

    send_message->status = OK_DONE;
    return "Successfully inserted new row.";
}

char* handleSelectQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != OP_SELECT) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query."; 
    }
    
    // retrieve params
    SelectOperator select = query->fields.select;
    char* handle = select.handle;
    int minimum = select.minimum;
    int maximum = select.maximum;

    // search for context and add to the list of variables
    ClientContext* context = searchContext(query->client_fd);
    if (context == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Error finding client context for search.";
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
    strcpy(new_handle.name, handle);

    // handle variable select sources separately from database sources
    if (select.src_is_var) {
        GeneralizedColumnHandle* src_handle = findHandle(context, select.params[0]);
        GeneralizedColumnHandle* val_handle = findHandle(context, select.params[1]);
        if (src_handle == NULL || val_handle == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return "-- Unable to find specified select source.";
        }
        Result* src_result = src_handle->generalized_column.column_pointer.result;
        Result* val_result = val_handle->generalized_column.column_pointer.result;
        if (src_result == NULL || val_result == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return "-- Unable to find specified select source.";
        }

        // scan through column and store all data in tuples
        int capacity = 0;
        int num_inserted = 0;
        int* data = NULL;
        int* indexes = (int*) src_result->payload;
        int* values = (int*) val_result->payload;
        for (size_t i = 0; i < src_result->num_tuples; i++) {
            if (values[i] < minimum || values[i] >= maximum) 
                continue;
            if (num_inserted == capacity) {
                capacity = (capacity == 0) ? 1 : 2 * capacity;
                if (data == NULL) {
                    data = calloc(capacity, sizeof(int));
                } else {
                    int* new_data = realloc(data, sizeof(int) * capacity);
                    if (new_data == NULL) {
                        free(new_data);
                        free(new_pointer.result);
                        send_message->status = EXECUTION_ERROR;
                        return "-- Error calculating result array.";
                    }
                    data = new_data;   
                }
            }
            data[num_inserted] = indexes[i];
            num_inserted++;
        }
        new_pointer.result->payload = data;
        new_pointer.result->num_tuples = num_inserted;
    } else {
        char* db_name = select.params[0];
        char* tbl_name = select.params[1];
        char* col_name = select.params[2];

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

        // scan through column and store all data in tuples
        int capacity = 0;
        int num_inserted = 0;
        int* data = NULL;
        for (size_t i = 0; i < table->num_rows; i++) {
            if (column->data[i] < minimum || column->data[i] >= maximum)
                continue;
            if (num_inserted == capacity) {
                capacity = (capacity == 0) ? 1 : 2 * capacity;
                if (data == NULL) {
                    data = calloc(capacity, sizeof(int));
                } else {
                    int* new_data = realloc(data, sizeof(int) * capacity);
                    if (new_data == NULL) {
                        free(new_data);
                        free(new_pointer.result);
                        send_message->status = EXECUTION_ERROR;
                        return "-- Error calculating result array.";
                    }
                    data = new_data;   
                }
            }
            data[num_inserted] = i;
            num_inserted++;
        }
        new_pointer.result->payload = data;
        new_pointer.result->num_tuples = num_inserted;
    }

    if (context->chandles_in_use == context->chandle_slots) {
        int new_size = (context->chandle_slots == 0) ? 1 : 2 * context->chandle_slots;
        if (context->chandle_table == NULL) {
            context->chandle_table = calloc(new_size, sizeof(GeneralizedColumnHandle));
            context->chandle_slots = new_size;
        } else {
            GeneralizedColumnHandle* new_table = realloc(context->chandle_table, new_size * sizeof(GeneralizedColumnHandle));
            if (new_table == NULL) {
                send_message->status = EXECUTION_ERROR;
                return "-- Problem inserting new handle into client context.";
            }
            context->chandle_table = new_table;
            context->chandle_slots = new_size;
        }
    }
    // check for duplicate handle names
    int dupIndex = findDuplicateHandle(context, handle);
    if (dupIndex < 0) {
        context->chandle_table[context->chandles_in_use++] = new_handle;
    } else {
        context->chandle_table[dupIndex] = new_handle;
    }

    send_message->status = OK_DONE;
    return "Successfully inserted new row.";
}

char* handleFetchQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != OP_FETCH) {
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
    // check for duplicate handle names
    int dupIndex = findDuplicateHandle(context, target);
    if (dupIndex < 0) {
        context->chandle_table[context->chandles_in_use++] = new_handle;
    } else {
        context->chandle_table[dupIndex] = new_handle;
    }

    send_message->status = OK_DONE;
    return "Successfully fetched data from select query.";
}

char* handlePrintQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != OP_PRINT) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query."; 
    }

    // retrieve params
    PrintOperator print = query->fields.print;
    char** handles = print.handles;
    size_t num_handles = print.num_params;
    
    // get context for current client
    ClientContext* context = searchContext(query->client_fd);
    if (context == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find context for current client.";
    }

    // search for handles in context
    Result* results[num_handles];
    for (size_t i = 0; i < num_handles; i++) {
        GeneralizedColumnHandle* columnHandle = findHandle(context, handles[i]);
        if (columnHandle == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return "-- Unable to find specified select source.";
        }
        Result* result = columnHandle->generalized_column.column_pointer.result;
        if (result == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return "-- Unable to find specified select source.";
        }
        results[i] = result;
    }

    // create string payload to return
    int length = 0;
    char buf[64];
    size_t num_tuples = results[0]->num_tuples;
    for (size_t i = 0; i < num_handles; i++) {
        switch(results[i]->data_type) {
            case INT: {
                int* data = (int*) results[i]->payload;
                for (size_t i = 0; i < num_tuples; i++) {
                    sprintf(buf, "%i", data[i]);
                    length += strlen(buf) + 1;
                }
                break;
            }
            case LONG: {
                long* data = (long*) results[i]->payload;
                for (size_t i = 0; i < num_tuples; i++) {
                    sprintf(buf, "%ld", data[i]);
                    length += strlen(buf) + 1;
                }
                break;
            }
            case FLOAT: {
                float* data = (float*) results[i]->payload;
                for (size_t i = 0; i < num_tuples; i++) {
                    sprintf(buf, "%f", data[i]);
                    length += strlen(buf) + 1;
                }
                break;
            }
            case DOUBLE: {
                double* data = (double*) results[i]->payload;
                for (size_t i = 0; i < num_tuples; i++) {
                    sprintf(buf, "%f", data[i]);
                    length += strlen(buf) + 1;
                }
                break;
            }
            default:
                break;
        }
    }
    
    char* values = malloc(sizeof(char) * (length + 1));
    values[0] = '\0';
    
    // TODO: really inefficient because it iterates across results
    for (size_t i = 0; i < num_tuples; i++) {
        for (size_t j = 0; j < num_handles; j++) {
            char delim = (j + 1 == num_handles) ? '\n' : ',';
            switch (results[j]->data_type) {
                case INT: {
                    int* data = (int*) results[j]->payload;
                    sprintf(values, "%s%i%c", values, data[i], delim);
                    break;
                }
                case LONG: {
                    long* data = (long*) results[j]->payload;
                    sprintf(values, "%s%ld%c", values, data[i], delim);
                    break;
                }
                case FLOAT: {
                    float* data = (float*) results[j]->payload;
                    sprintf(values, "%s%f%c", values, data[i], delim);
                    break;
                }
                case DOUBLE: {
                    double* data = (double*) results[j]->payload;
                    sprintf(values, "%s%f%c", values, data[i], delim);
                    break;
                }
                default:
                    break;
            }
        }
    }
    values[length] = '\0';
    // log_info("-- result printf: %s\n", values);

    send_message->status = OK_WAIT_FOR_RESPONSE;
    send_message->length = length;
    return values;
}

char* handleMathQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != OP_MATH) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query."; 
    }

    // retrieve params
    MathOperator math = query->fields.math;
    char* handle = math.handle;
    
    // get context for current client
    ClientContext* context = searchContext(query->client_fd);
    if (context == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find context for current client.";
    }
    
    // handle one and two argument cases separately
    if (math.type <= 3) {
        size_t num_tuples;
        int* payload;
        
        // handle variable vs. database queries separately
        if (math.is_var == true) {
            // search for result in context
            GeneralizedColumnHandle* src_handle = findHandle(context, math.params[0]);
            if (src_handle == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return "-- Unable to find specified result source.";
            }
            Result* result = src_handle->generalized_column.column_pointer.result;
            if (result == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return "-- Unable to find specified result source.";
            }
            num_tuples = result->num_tuples;
            payload = (int*) result->payload;
        } else {
            // check database
            if (strcmp(math.params[0], current_db->name) != 0) {
                send_message->status = OBJECT_NOT_FOUND;
                return "-- Database not found.";
            }

            // if we didn't manage to find a table
            Table* table = findTable(math.params[1]);
            if (table == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return "-- Unable to find specified table.";
            }

            // if we didn't manage to find a column
            Column* column = findColumn(table, math.params[2]);
            if (column == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return "-- Unable to find specified column.";
            }

            num_tuples = table->num_rows;
            payload = column->data;
        }

        double result = 0;
        switch (math.type) {
            case AVG: {
                for (size_t i = 0; i < num_tuples; i++) {
                    result += payload[i];
                }
                result /= num_tuples;
                break;
            }
            case SUM: {
                for (size_t i = 0; i < num_tuples; i++) {
                    result += payload[i];
                }
                break;
            }
            case MIN: {
                result = payload[0];
                for (size_t i = 1; i < num_tuples; i++) {
                    if (payload[i] < result) {
                        result = payload[i];
                    }
                }
                break;
            }
            case MAX: {
                result = payload[0];
                for (size_t i = 1; i < num_tuples; i++) {
                    if (payload[i] > result) {
                        result = payload[i];
                    }
                }
                break;
            }
            default:
                break;
        }
        double* toSave = malloc(sizeof(double));
        toSave[0] = result;

        // create a new GeneralizedColumnHandle
        GeneralizedColumnHandle new_handle;
        GeneralizedColumnPointer new_pointer;
        new_pointer.result = malloc(sizeof(Result));
        new_pointer.result->data_type = DOUBLE;
        new_pointer.result->num_tuples = 1;
        new_pointer.result->payload = (void*) toSave;
        GeneralizedColumn gen_column = {
            .column_type = RESULT,
            .column_pointer = new_pointer
        };
        new_handle.generalized_column = gen_column;
        strcpy(new_handle.name, handle);

        // search for context and add to the list of variables
        if (checkContextSize(context) != true) {
            send_message->status = EXECUTION_ERROR;
            return "-- Problem inserting new handle into client context.";
        }
        // check for duplicate handle names
        int dupIndex = findDuplicateHandle(context, handle);
        if (dupIndex < 0) {
            context->chandle_table[context->chandles_in_use++] = new_handle;
        } else {
            context->chandle_table[dupIndex] = new_handle;
        }

        send_message->status = OK_DONE;
        return "Successfully completed computation in math query.";
    } else {
        size_t num_tuples;
        int* payload1;
        int* payload2;
        
        // handle variable vs. database queries separately for first argument
        if (math.is_var == true) {
            // search for result in context
            GeneralizedColumnHandle* src_handle = findHandle(context, math.params[0]);
            if (src_handle == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return "-- Unable to find specified result source.";
            }
            Result* result = src_handle->generalized_column.column_pointer.result;
            if (result == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return "-- Unable to find specified result source.";
            }
            num_tuples = result->num_tuples;
            payload1 = (int*) result->payload;
            
            // handle variable vs. database queries separately for second argument
            if (math.num_params == 2) {
                // search for result in context
                src_handle = findHandle(context, math.params[1]);
                if (src_handle == NULL) {
                    send_message->status = OBJECT_NOT_FOUND;
                    return "-- Unable to find specified result source.";
                }
                result = src_handle->generalized_column.column_pointer.result;
                if (result == NULL) {
                    send_message->status = OBJECT_NOT_FOUND;
                    return "-- Unable to find specified result source.";
                }
                payload2 = (int*) result->payload;
            } else {
                // check database
                if (strcmp(math.params[1], current_db->name) != 0) {
                    send_message->status = OBJECT_NOT_FOUND;
                    return "-- Database not found.";
                }

                // if we didn't manage to find a table
                Table* table = findTable(math.params[2]);
                if (table == NULL) {
                    send_message->status = OBJECT_NOT_FOUND;
                    return "-- Unable to find specified table.";
                }

                // if we didn't manage to find a column
                Column* column = findColumn(table, math.params[3]);
                if (column == NULL) {
                    send_message->status = OBJECT_NOT_FOUND;
                    return "-- Unable to find specified column.";
                }

                payload2 = column->data;
            }
        } else {
            // check database
            if (strcmp(math.params[0], current_db->name) != 0) {
                send_message->status = OBJECT_NOT_FOUND;
                return "-- Database not found.";
            }

            // if we didn't manage to find a table
            Table* table = findTable(math.params[1]);
            if (table == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return "-- Unable to find specified table.";
            }

            // if we didn't manage to find a column
            Column* column = findColumn(table, math.params[2]);
            if (column == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return "-- Unable to find specified column.";
            }

            num_tuples = table->num_rows;
            payload1 = column->data;
            
            // handle variable vs. database queries separately for second argument
            if (math.num_params == 4) {
                // search for result in context
                GeneralizedColumnHandle* src_handle = findHandle(context, math.params[3]);
                if (src_handle == NULL) {
                    send_message->status = OBJECT_NOT_FOUND;
                    return "-- Unable to find specified result source.";
                }
                Result* result = src_handle->generalized_column.column_pointer.result;
                if (result == NULL) {
                    send_message->status = OBJECT_NOT_FOUND;
                    return "-- Unable to find specified result source.";
                }
                payload2 = (int*) result->payload;
            } else {
                // check database
                if (strcmp(math.params[3], current_db->name) != 0) {
                    send_message->status = OBJECT_NOT_FOUND;
                    return "-- Database not found.";
                }

                // if we didn't manage to find a table
                Table* table = findTable(math.params[4]);
                if (table == NULL) {
                    send_message->status = OBJECT_NOT_FOUND;
                    return "-- Unable to find specified table.";
                }

                // if we didn't manage to find a column
                Column* column = findColumn(table, math.params[5]);
                if (column == NULL) {
                    send_message->status = OBJECT_NOT_FOUND;
                    return "-- Unable to find specified column.";
                }

                payload2 = column->data;
            }
        }

        int* result = malloc(sizeof(int) * num_tuples);
        switch (math.type) {
            case ADD: {
                for (size_t i = 0; i < num_tuples; i++) {
                    result[i] = payload1[i] + payload2[i];
                }
                break;
            }
            case SUB: {
                for (size_t i = 0; i < num_tuples; i++) {
                    result[i] = payload1[i] - payload2[i];
                }
                break;
            }
            default:
                break;
        }

        // create a new GeneralizedColumnHandle
        GeneralizedColumnHandle new_handle;
        GeneralizedColumnPointer new_pointer;
        new_pointer.result = malloc(sizeof(Result));
        new_pointer.result->data_type = INT;
        new_pointer.result->num_tuples = num_tuples;
        new_pointer.result->payload = (void*) result;
        GeneralizedColumn gen_column = {
            .column_type = RESULT,
            .column_pointer = new_pointer
        };
        new_handle.generalized_column = gen_column;
        strcpy(new_handle.name, handle);

        // search for context and add to the list of variables
        if (checkContextSize(context) != true) {
            send_message->status = EXECUTION_ERROR;
            return "-- Problem inserting new handle into client context.";
        }
        // check for duplicate handle names
        int dupIndex = findDuplicateHandle(context, handle);
        if (dupIndex < 0) {
            context->chandle_table[context->chandles_in_use++] = new_handle;
        } else {
            context->chandle_table[dupIndex] = new_handle;
        }

        send_message->status = OK_DONE;
        return "Successfully completed computation in math query.";
    }    
}
