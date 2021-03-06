#include <sys/time.h>

#include "api/db_io.h"
#include "api/context.h"
#include "api/sorted.h"
#include "api/hashtable.h"
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
    case OP_BATCH:
        res = handleBatchQuery(query, send_message);
        break;
    case OP_MATH:
        res = handleMathQuery(query, send_message);
        break;
    case OP_JOIN:
        res = handleJoinQuery(query, send_message);
        break;
    }

    // printDatabase(current_db);
    
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
        case CREATE_DB: {
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
        }
        case CREATE_TBL: {
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

            // check to make sure table doesn't already existence
            Table* existing_table = findTable(tbl_name);
            if (existing_table != NULL) {
                send_message->status = EXECUTION_ERROR;
                return "-- Table already exists.";
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
            new_table->num_indexes = 0;
            current_db->tables[current_db->num_tables++] = new_table;

            // finished successfully
            send_message->status = OK_DONE;
            return "-- Successfully created table.";
        }
        case CREATE_COL: {
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

            // if we didn't manage to find a table
            Table* table = findTable(tbl_name);
            if (table == NULL) {
                send_message->status = INCORRECT_FORMAT;
                return "-- Unable to find specified table.";
            } 

            // check to make sure column doesn't already exist
            Column* existing_column = findColumn(table, col_name);
            if (existing_column != NULL) {
                send_message->status = EXECUTION_ERROR;
                return "-- Column already exists.";
            }

            // create a column file
            if (createColumn(db_name, tbl_name, col_name) != 0) {
                send_message->status = EXECUTION_ERROR;
                return "-- Error creating column.";
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
        }
        case CREATE_IDX: {
            // check for params
            if (fields.create.num_params != 5) {
                send_message->status = INCORRECT_FORMAT;
                return "-- Incorrect number of arguments when creating index.";
            }
            
            // extract params and validate
            db_name = fields.create.params[0];
            tbl_name = fields.create.params[1];
            col_name = fields.create.params[2];
            
            if (strcmp(current_db->name, db_name) != 0) {
                send_message->status = QUERY_UNSUPPORTED;
                return "-- Cannot create index in inactive db.";
            }

            // if we didn't manage to find a table
            Table* table = findTable(tbl_name);
            if (table == NULL) {
                send_message->status = INCORRECT_FORMAT;
                return "-- Unable to find specified table.";
            } 
            
            // search for column to ensure existence
            Column* column = findColumn(table, col_name);
            if (column == NULL) {
                send_message->status = INCORRECT_FORMAT;
                return "-- Unable to find specified column.";
            }

            // resize table indexes array if necessary
            if (table->num_indexes == 0)
                table->indexes = malloc(sizeof(Index*) * 2);
            if (table->num_indexes > 2 && table->num_indexes % 2 == 0) {
                Index** new_list = realloc(table->indexes, sizeof(Index*) * (2 + table->num_indexes));
                if (new_list != NULL) {
                    table->indexes = new_list;
                } else {
                    send_message->status = EXECUTION_ERROR;
                    return "-- Unable to insert a new index; no space in table.";
                }
            }
            
            // create an index object and insert into table now
            Index* new_index = malloc(sizeof(Index));
            new_index->column = column;
            new_index->type = strcmp("btree", fields.create.params[3]) == 0 ? BTREE : SORTED;
            new_index->clustered = (strcmp("clustered", fields.create.params[4]) == 0);
            switch (new_index->type) {
                case BTREE:
                    new_index->object = malloc(sizeof(IndexObject));
                    if (new_index->clustered) {
                        new_index->object->btreec = createBTreeC();
                        for (size_t i = 0; i < table->num_rows; i++) {
                            insertValueC(&(new_index->object->btreec), column->data[i]);
                        }
                    } else {
                        new_index->object->btreeu = createBTreeU();
                        for (size_t i = 0; i < table->num_rows; i++)
                            insertValueU(&(new_index->object->btreeu), column->data[i], i);
                    }
                    break;
                case SORTED:
                    new_index->column = column;
                    if (!new_index->clustered) {
                        new_index->object = malloc(sizeof(IndexObject));
                        initializeColumnIndex(&(new_index->object->column), table->capacity * sizeof(int));
                        for (size_t i = 0; i < table->num_rows; i++)
                            insertIndex(new_index->object->column, column->data[i], i, i);
                    } else {
                        new_index->object = NULL;
                    }
                    break;
            }
            table->indexes[table->num_indexes++] = new_index;
            
            // finished successfully
            send_message->status = OK_DONE;
            return "-- Successfully created index.";
        }
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

    // resize the table if necessary
    size_t num_rows = table->num_rows;
    bool must_resize = num_rows == table->capacity;
    if (must_resize) {
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

    // check for a clustered index
    Index* cluster_index = NULL;
    for (size_t i = 0; i < table->num_indexes; i++)
        cluster_index = (table->indexes[i]->clustered) ? table->indexes[i] : cluster_index;
    if (cluster_index != NULL) {
        // find corresponding column index and value
        size_t col_index = -1;
        for (size_t i = 0; i < table->col_count; i++)
            if (table->columns[i] == cluster_index->column)
                col_index = i;

        // insert new value and get back corresponding index
        size_t insert_index = 0;
        switch (cluster_index->type) {
            case BTREE:
                insert_index = insertValueC(&(cluster_index->object->btreec), values[col_index]);
                break;
            case SORTED:
                insert_index = insertSorted(cluster_index->column->data, values[col_index], table->num_rows);
                break;
        }
        
        // now shift all values in all columns starting from insert_index onwards
        for (size_t j = 0; j < table->col_count; j++) {
            if (j == col_index)
                continue;
            shiftValues(table->columns[j]->data, insert_index, table->num_rows, 0);
            table->columns[j]->data[insert_index] = values[j];
        }

        // check the other indexes to see if any need adjustment
        for (size_t j = 0; j < table->num_indexes; j++) {
            if (table->columns[j] == cluster_index->column)
                continue;
            // type should always be unclustered here
            if (table->indexes[j]->clustered && table->indexes[j] != cluster_index) {
                send_message->status = EXECUTION_ERROR;
                return "-- Indexing error; found multiple clustered indices.";
            }
            // find corresponding column index
            size_t col_index = 0;
            for (size_t i = 0; i < table->col_count; i++)
                if (table->columns[i] == table->indexes[j]->column)
                    col_index = i;
            
            // insert value appropriately
            switch (table->indexes[j]->type) {
                case BTREE:
                    insertValueU(&(table->indexes[j]->object->btreeu), values[col_index], insert_index);
                    break;
                case SORTED:
                    // resize sorted column index array if necessary
                    if (table->indexes[j]->object == NULL) {
                        table->indexes[j]->object = malloc(sizeof(IndexObject));
                        table->indexes[j]->object->column = malloc(sizeof(ColumnIndex));
                    }
                    if (table->indexes[j]->object->column->values == NULL) {
                        table->indexes[j]->object->column->values = malloc(sizeof(int) * table->capacity);
                        table->indexes[j]->object->column->indexes = malloc(sizeof(int) * table->capacity);
                    }
                    if (must_resize) {
                        int* new_array1 = realloc(table->indexes[j]->object->column->values, sizeof(int) * table->capacity);
                        int* new_array2 = realloc(table->indexes[j]->object->column->indexes, sizeof(int) * table->capacity);
                        if (new_array1 != NULL) {
                            table->indexes[j]->object->column->values = new_array1;
                        } else {
                            send_message->status = EXECUTION_ERROR;
                            return "-- Re-allocation of unclustered sorted index values failed.";
                        }
                        if (new_array2 != NULL) {
                            table->indexes[j]->object->column->indexes = new_array2;
                        } else {
                            send_message->status = EXECUTION_ERROR;
                            return "-- Re-allocation of unclustered sorted index values failed.";
                        }
                    }
                    // insert value into unclustered column index array
                    int unclustered_index = insertSorted(table->indexes[j]->object->column->values, values[col_index], table->num_rows);
                    table->indexes[j]->object->column->indexes[unclustered_index] = insert_index;
                    break;
            }
        }

        table->num_rows++;
        send_message->status = OK_DONE;
        return "Successfully inserted new row.";
    }

    // unclustered indices only, simply insert and update indices as necessary
    for (size_t i = 0; i < table->col_count; i++)
        table->columns[i]->data[table->num_rows] = values[i];

    // update indices
    for (size_t i = 0; i < table->num_indexes; i++) {
        // type should always be unclustered here
        if (table->indexes[i]->clustered) {
            send_message->status = EXECUTION_ERROR;
            return "-- Indexing error; found unexpected clustered indices.";
        }
        // find corresponding column index
        size_t col_index = 0;
        for (size_t j = 0; j < table->col_count; j++)
            if (table->columns[j] == table->indexes[i]->column)
                col_index = j;
        
        // insert value appropriately
        switch (table->indexes[i]->type) {
            case BTREE:
                insertValueU(&(table->indexes[i]->object->btreeu), values[col_index], table->num_rows);
                break;
            case SORTED:
                // resize sorted column index array if necessary
                if (table->indexes[i]->object->column->values == NULL) {
                    table->indexes[i]->object->column->values = malloc(sizeof(int) * table->capacity);
                    table->indexes[i]->object->column->indexes = malloc(sizeof(int) * table->capacity);
                }
                if (must_resize) {
                    int* new_array1 = realloc(table->indexes[i]->object->column->values, sizeof(int) * table->capacity);
                    int* new_array2 = realloc(table->indexes[i]->object->column->indexes, sizeof(int) * table->capacity);
                    if (new_array1 != NULL) {
                        table->indexes[i]->object->column->values = new_array1;
                    } else {
                        send_message->status = EXECUTION_ERROR;
                        return "-- Re-allocation of unclustered sorted index values failed.";
                    }
                    if (new_array2 != NULL) {
                        table->indexes[i]->object->column->indexes = new_array2;
                    } else {
                        send_message->status = EXECUTION_ERROR;
                        return "-- Re-allocation of unclustered sorted index values failed.";
                    }
                }
                // insert value into unsorted column index array
                int unclustered_index = insertSorted(table->indexes[i]->object->column->values, values[col_index], table->num_rows);
                table->indexes[i]->object->column->indexes[unclustered_index] = table->num_rows;
                break;
        }
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

    // make space for more context handles if necessary
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

    // add to batched queries if necessary
    if (context->queries != NULL) {
        BatchedQueries* queries = context->queries;
        if (queries->num_queries == 0) {
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

            queries->table = table;
            queries->column = column;

            queries->minimum = malloc(sizeof(int));
            queries->maximum = malloc(sizeof(int));
            queries->results = malloc(sizeof(Result*));
        } else {
            int* new_mins = realloc(queries->minimum, sizeof(int) * (queries->num_queries + 1));
            int* new_maxs = realloc(queries->maximum, sizeof(int) * (queries->num_queries + 1));
            Result** new_results = realloc(queries->results, sizeof(Result*) * (queries->num_queries + 1));
            if (new_mins != NULL) {
                queries->minimum = new_mins;
            } else {
                return "-- Failed to insert new select query into batch.";
            }
            if (new_maxs != NULL) {
                queries->maximum = new_maxs;
            } else {
                return "-- Failed to insert new select query into batch.";
            }
            if (new_results != NULL) {
                queries->results = new_results;
            } else {
                return "-- Failed to insert new select query into batch.";
            }
        }
        queries->minimum[queries->num_queries] = minimum;
        queries->maximum[queries->num_queries] = maximum;
        queries->results[queries->num_queries] = new_pointer.result;
        queries->num_queries++;
        
        send_message->status = OK_DONE;
        return "-- Successfully inserted select query into batch.";
    }

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

        // check for an index we can use
        Index* index = NULL;
        for (size_t i = 0; i < table->num_indexes; i++)
            index = table->indexes[i]->column == column ? table->indexes[i] : index;
        
        if (index != NULL) {
            struct timeval stop, start;
            gettimeofday(&start, NULL);
            
            // use index to search for valid values
            switch (index->type) {
                case BTREE:
                    if (index->clustered) {
                        int* int_payload;
                        new_pointer.result->num_tuples = findRangeC(&int_payload, index->object->btreec, minimum, maximum);
                        new_pointer.result->payload = (void*) int_payload;
                    } else {
                        int* int_payload;
                        new_pointer.result->num_tuples = findRangeU(&int_payload, index->object->btreeu, minimum, maximum);
                        new_pointer.result->payload = (void*) int_payload;
                    }
                    break;
                case SORTED:
                    if (index->clustered) {
                        int low = 0;
                        int high = table->num_rows;
                        while (low < high) {
                            int current = (low + high) / 2;
                            if (column->data[current] < minimum)
                                low = current + 1;
                            else
                                high = current;
                        }
                        // low now contains the smallest element >= minimum
                        size_t minIndex = low;
                        low = 0;
                        high = table->num_rows;
                        while (low < high) {
                            int current = (low + high) / 2;
                            if (column->data[current] >= maximum)
                                high = current - 1;
                            else
                                low = current + 1;
                        }
                        // high now contains the greatest element <= minimum
                        size_t maxIndex = high;
                        if (minIndex >= table->num_rows || maxIndex <= 0) {
                            new_pointer.result->num_tuples = 0;
                            new_pointer.result->payload = NULL;
                        } else {
                            new_pointer.result->num_tuples = maxIndex - minIndex + 1;
                            int* results = malloc(sizeof(int) * (maxIndex - minIndex + 1));
                            for (size_t i = minIndex; i <= maxIndex && i < table->num_rows; i++) {
                                results[i - minIndex] = i;
                            }
                            new_pointer.result->payload = (void*) results;
                        }
                    } else {
                        int* int_payload;
                        new_pointer.result->num_tuples = findRangeS(&int_payload, index->object->column, table->num_rows, minimum, maximum);
                        new_pointer.result->payload = (void*) int_payload;
                    }
                    break;
            }

            gettimeofday(&stop, NULL);
            printf("-- Select query using %s %s index took %lu milliseconds.  %i out of %i tuples.\n", 
                index->clustered ? "clustered" : "unclustered",
                index->type == BTREE ? "BTREE" : "SORTED",
                1000000 * (stop.tv_sec - start.tv_sec) + stop.tv_usec - start.tv_usec,
                new_pointer.result->num_tuples,
                table->num_rows);
        } else {
            struct timeval stop, start;
            gettimeofday(&start, NULL);
            
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

            gettimeofday(&stop, NULL);
            printf("-- Select query using scan took %lu milliseconds.  %i out of %i tuples.\n", 
                1000000 * (stop.tv_sec - start.tv_sec) + stop.tv_usec - start.tv_usec, 
                num_inserted,
                table->num_rows);
        }
    }

    send_message->status = OK_DONE;
    return "Successfully selected data from column.";
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
                    sprintf(buf, "%.2f", data[i]);
                    length += strlen(buf) + 1;
                }
                break;
            }
            case DOUBLE: {
                double* data = (double*) results[i]->payload;
                for (size_t i = 0; i < num_tuples; i++) {
                    sprintf(buf, "%.2f", data[i]);
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
                    sprintf(values, "%s%.2f%c", values, data[i], delim);
                    break;
                }
                case DOUBLE: {
                    double* data = (double*) results[j]->payload;
                    sprintf(values, "%s%.2f%c", values, data[i], delim);
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

char* handleBatchQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != OP_BATCH) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query."; 
    }
    
    // search for context and add to the list of variables
    ClientContext* context = searchContext(query->client_fd);
    if (context == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Error finding client context for batch request.";
    }

    // validate batch request
    if ((context->queries == NULL && query->fields.batch.start == false) ||
        (context->queries != NULL && query->fields.batch.start == true)) {
        send_message->status = EXECUTION_ERROR;
        return "-- Invalid batch request; not in the correct state.";
    }

    if (query->fields.batch.start) {
        // start a batch of queries
        context->queries = malloc(sizeof(BatchedQueries));
        context->queries->table = NULL;
        context->queries->column = NULL;
        context->queries->minimum = NULL;
        context->queries->maximum = NULL;
        context->queries->results = NULL;
        context->queries->num_queries = 0;

        send_message->status = OK_DONE;
        return "-- Successfully processed batch request.";
    } else {
        // execute a batch of queries
        return handleBatchSelectQuery(context->queries, send_message);
    }
}

void findRangeCBatchHelper(BTreeCNode* node, BatchedQueries* queries) {
    int num_queries = queries->num_queries;
    if (num_queries <= 0)
        return;
    
    int min_overall = queries->minimum[0];
    int max_overall = queries->maximum[0];
    int* minimum = queries->minimum;
    int* maximum = queries->maximum;
    for (int i = 0; i < num_queries; i++) {
        min_overall = min_overall < queries->minimum[i] ? min_overall : queries->minimum[i];
        max_overall = max_overall > queries->maximum[i] ? max_overall : queries->maximum[i];
    }

    // find closest leaf
    BTreeCNode* ptr = node;
    while (ptr->type != LEAF) {
        size_t i;
        for (i = 0; i < ptr->object.parent.num_children - 1; i++)
            if (ptr->object.parent.dividers[i] > min_overall)
                break;
        ptr = ptr->object.parent.children[i];
    }

    // iterate horizontally and retrieve values from tree
    int num_tuples[num_queries];
    int capacities[num_queries];
    int* results[num_queries];
    for (int i = 0; i < num_queries; i++) {
        num_tuples[i] = 0;
        capacities[i] = 0;
        results[i] = NULL;
    }

    BTreeCLeaf* leaf = &(ptr->object.leaf);
    
    // TODO: optimize further by dynamically calculating which queries need to be updated
    while (leaf != NULL) {
        // iterate over all values in the leaf
        for (size_t i = 0; i < leaf->num_elements; i++) {
            // look for values >= min_overall only
            if (leaf->values[i] < min_overall)
                continue;
            // stop iterating if we find a value >= max_overall
            if (leaf->values[i] >= max_overall) {
                leaf = NULL;
                break;
            }
            // iterate over all queries and insert appropriately
            for (int j = 0; j < num_queries; j++) {
                // skip if not in the needed range
                if (leaf->values[i] < minimum[j] || leaf->values[i] >= maximum[j])
                    continue;
                // resize if needed
                if (capacities[j] == num_tuples[j]) {
                    int new_size = (capacities[j] == 0) ? 1 : 2 * capacities[j];
                    if (new_size == 1) {
                        results[j] = malloc(sizeof(int));
                    } else {
                        int* new_data = realloc(results[j], sizeof(int) * new_size);
                        if (new_data != NULL)
                            results[j] = new_data;
                    }
                    capacities[j] = new_size;
                }
                results[j][num_tuples[j]++] = leaf->indexes[i];
            }
        }
        if (leaf != NULL)
            leaf = leaf->next;
    }

    // store values in Results array
    for (int i = 0; i < num_queries; i++) {
        queries->results[i]->payload = (void*) results[i];
        queries->results[i]->data_type = INT;
        queries->results[i]->num_tuples = num_tuples[i];
    }
}

void findRangeUBatchHelper(BTreeUNode* node, BatchedQueries* queries) {
    int num_queries = queries->num_queries;
    if (num_queries <= 0)
        return;
    
    int min_overall = queries->minimum[0];
    int max_overall = queries->maximum[0];
    int* minimum = queries->minimum;
    int* maximum = queries->maximum;
    for (int i = 0; i < num_queries; i++) {
        min_overall = min_overall < queries->minimum[i] ? min_overall : queries->minimum[i];
        max_overall = max_overall > queries->maximum[i] ? max_overall : queries->maximum[i];
    }

    // find closest leaf
    BTreeUNode* ptr = node;
    while (ptr->type != LEAF) {
        size_t i;
        for (i = 0; i < ptr->object.parent.num_children - 1; i++)
            if (ptr->object.parent.dividers[i] > min_overall)
                break;
        ptr = ptr->object.parent.children[i];
    }

    // iterate horizontally and retrieve values from tree
    int num_tuples[num_queries];
    int capacities[num_queries];
    int* results[num_queries];
    for (int i = 0; i < num_queries; i++) {
        num_tuples[i] = 0;
        capacities[i] = 0;
        results[i] = NULL;
    }

    BTreeULeaf* leaf = &(ptr->object.leaf);
    
    // TODO: optimize further by dynamically calculating which queries need to be updated
    while (leaf != NULL) {
        // iterate over all values in the leaf
        for (size_t i = 0; i < leaf->num_elements; i++) {
            // look for values >= min_overall only
            if (leaf->values[i] < min_overall)
                continue;
            // stop iterating if we find a value >= max_overall
            if (leaf->values[i] >= max_overall) {
                leaf = NULL;
                break;
            }
            // iterate over all queries and insert appropriately
            for (int j = 0; j < num_queries; j++) {
                // skip if not in the needed range
                if (leaf->values[i] < minimum[j] || leaf->values[i] >= maximum[j])
                    continue;
                // resize if needed
                if (capacities[j] == num_tuples[j]) {
                    int new_size = (capacities[j] == 0) ? 1 : 2 * capacities[j];
                    if (new_size == 1) {
                        results[j] = malloc(sizeof(int));
                    } else {
                        int* new_data = realloc(results[j], sizeof(int) * new_size);
                        if (new_data != NULL)
                            results[j] = new_data;
                    }
                    capacities[j] = new_size;
                }
                results[j][num_tuples[j]++] = leaf->indexes[i];
            }
        }
        if (leaf != NULL)
            leaf = leaf->next;
    }

    // store values in Results array
    for (int i = 0; i < num_queries; i++) {
        queries->results[i]->payload = (void*) results[i];
        queries->results[i]->data_type = INT;
        queries->results[i]->num_tuples = num_tuples[i];
    }
}

void findRangeSBatchHelper(Column* column, int total, BatchedQueries* queries) {
    int num_queries = queries->num_queries;
    if (num_queries <= 0)
        return;
    
    int min_overall = queries->minimum[0];
    int max_overall = queries->maximum[0];
    int* minimum = queries->minimum;
    int* maximum = queries->maximum;
    for (int i = 0; i < num_queries; i++) {
        min_overall = min_overall < queries->minimum[i] ? min_overall : queries->minimum[i];
        max_overall = max_overall > queries->maximum[i] ? max_overall : queries->maximum[i];
    }
    
    int minIndex, maxIndex;
    // find starting point in data array
    int low = 0;
    int high = total;
    while (low < high) {
        int current = (low + high) / 2;
        if (column->data[current] < min_overall)
            low = current + 1;
        else
            high = current;
    }
    minIndex = low;

    // find highest point in data array
    low = 0;
    high = total;
    while (low < high) {
        int current = (low + high) / 2;
        if (column->data[current] >= max_overall)
            high = current - 1;
        else
            low = current + 1;
    }
    maxIndex = high;

    // iterate and retrieve values
    int num_tuples[num_queries];
    int capacities[num_queries];
    int* results[num_queries];
    for (int i = 0; i < num_queries; i++) {
        num_tuples[i] = 0;
        capacities[i] = 0;
        results[i] = NULL;
    }

    if (minIndex >= total || maxIndex <= 0) {
        for (int i = 0; i < num_queries; i++) {
            queries->results[i]->num_tuples = 0;
            queries->results[i]->payload = NULL;
        }
    } else {
        for (int i = minIndex; i <= maxIndex; i++) {
            // iterate over all queries and insert appropriately
            for (int j = 0; j < num_queries; j++) {
                // skip if not in the needed range
                if (column->data[i] < minimum[j] || column->data[i] >= maximum[j])
                    continue;
                // resize if needed
                if (capacities[j] == num_tuples[j]) {
                    int new_size = (capacities[j] == 0) ? 1 : 2 * capacities[j];
                    if (new_size == 1) {
                        results[j] = malloc(sizeof(int));
                    } else {
                        int* new_data = realloc(results[j], sizeof(int) * new_size);
                        if (new_data != NULL)
                            results[j] = new_data;
                    }
                    capacities[j] = new_size;
                }
                results[j][num_tuples[j]++] = i;
            }
        }
    }

    // store values in Results array
    for (int i = 0; i < num_queries; i++) {
        queries->results[i]->payload = (void*) results[i];
        queries->results[i]->data_type = INT;
        queries->results[i]->num_tuples = num_tuples[i];
    }
}

// need to modify this to batch queries
char* handleBatchSelectQuery(BatchedQueries* queries, message* send_message) {
    
    Column* column = queries->column;
    Index* index = NULL;
    for (size_t i = 0; i < queries->table->num_indexes; i++)
        index = queries->table->indexes[i]->column == column ? queries->table->indexes[i] : index;
    
    // if (index != NULL) {
    if (false) {
        // use index to search for valid values
        switch (index->type) {
            case BTREE:
                if (index->clustered) {
                    findRangeCBatchHelper(index->object->btreec, queries);
                } else {
                    findRangeUBatchHelper(index->object->btreeu, queries);
                }
                break;
            case SORTED:
                if (index->clustered) {
                    findRangeSBatchHelper(column, queries->table->num_rows, queries);
                } else {
                    for (int i = 0; i < queries->num_queries; i++) {
                        int* payload;
                        queries->results[i]->num_tuples = findRangeS(
                            &payload, 
                            index->object->column, 
                            queries->table->num_rows, 
                            queries->minimum[i],
                            queries->maximum[i]
                        );
                        queries->results[i]->payload = (void*) payload;
                    }
                }
                break;
        }
    } else {
        struct timeval start, stop;
        gettimeofday(&start, NULL);

        // scan through column and store all data in tuples
        int num_tuples[queries->num_queries];
        int capacities[queries->num_queries];
        int* results[queries->num_queries];
        for (int i = 0; i < queries->num_queries; i++) {
            num_tuples[i] = 0;
            capacities[i] = 0;
            results[i] = NULL;
        }

        for (size_t i = 0; i < queries->table->num_rows; i++) {
            // iterate over all queries and insert appropriately
            for (int j = 0; j < queries->num_queries; j++) {
                // skip if not in the needed range
                if (column->data[i] < queries->minimum[j] || column->data[i] >= queries->maximum[j])
                    continue;
                // resize if needed
                if (capacities[j] == num_tuples[j]) {
                    int new_size = (capacities[j] == 0) ? 1 : 2 * capacities[j];
                    if (new_size == 1) {
                        results[j] = malloc(sizeof(int));
                    } else {
                        int* new_data = realloc(results[j], sizeof(int) * new_size);
                        if (new_data != NULL)
                            results[j] = new_data;
                    }
                    capacities[j] = new_size;
                }
                results[j][num_tuples[j]++] = column->data[i];
            }
        }

        // store values in Results array
        for (int i = 0; i < queries->num_queries; i++) {
            queries->results[i]->payload = (void*) results[i];
            queries->results[i]->data_type = INT;
            queries->results[i]->num_tuples = num_tuples[i];
        }

        gettimeofday(&stop, NULL);
        printf("-- Batch select scan query took %lu milliseconds.\n", 
            1000000 * (stop.tv_sec - start.tv_sec) + stop.tv_usec - start.tv_usec);
    }

    send_message->status = OK_DONE;
    return "Successfully selected data from column.";
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

        // create a new GeneralizedColumnHandle
        GeneralizedColumnHandle new_handle;
        GeneralizedColumnPointer new_pointer;
        new_pointer.result = malloc(sizeof(Result));
        new_pointer.result->num_tuples = 1;
        GeneralizedColumn gen_column = {
            .column_type = RESULT,
            .column_pointer = new_pointer
        };
        new_handle.generalized_column = gen_column;
        strcpy(new_handle.name, handle);

        // calculate values to store
        switch (math.type) {
            case AVG: {
                double result = 0;
                for (size_t i = 0; i < num_tuples; i++) {
                    result += payload[i];
                }
                result /= num_tuples;
                
                double* toSave = malloc(sizeof(double));
                toSave[0] = result;
                new_pointer.result->data_type = DOUBLE;
                new_pointer.result->payload = (void*) toSave;
                break;
            }
            case SUM: {
                int result = 0;
                for (size_t i = 0; i < num_tuples; i++) {
                    result += payload[i];
                }
                int* toSave = malloc(sizeof(double));
                toSave[0] = result;
                new_pointer.result->data_type = INT;
                new_pointer.result->payload = (void*) toSave;
                break;
            }
            case MIN: {
                int result = payload[0];
                for (size_t i = 1; i < num_tuples; i++) {
                    if (payload[i] < result) {
                        result = payload[i];
                    }
                }
                int* toSave = malloc(sizeof(double));
                toSave[0] = result;
                new_pointer.result->data_type = INT;
                new_pointer.result->payload = (void*) toSave;
                break;
            }
            case MAX: {
                int result = payload[0];
                for (size_t i = 1; i < num_tuples; i++) {
                    if (payload[i] > result) {
                        result = payload[i];
                    }
                }
                int* toSave = malloc(sizeof(double));
                toSave[0] = result;
                new_pointer.result->data_type = INT;
                new_pointer.result->payload = (void*) toSave;
                break;
            }
            default:
                break;
        }

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

char* handleJoinQuery(DbOperator* query, message* send_message) {
    if (query == NULL || query->type != OP_JOIN) {
        send_message->status = QUERY_UNSUPPORTED;
        return "Invalid query."; 
    }

    // get context for current client
    ClientContext* context = searchContext(query->client_fd);
    if (context == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find context for current client.";
    }

    // search for handles in context
    JoinOperator join = query->fields.join;
    Result* fetch_r1;
    Result* fetch_r2;
    Result* select_r1;
    Result* select_r2;
    GeneralizedColumnHandle* columnHandle;
    
    columnHandle = findHandle(context, join.fetch1);
    if (columnHandle == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified fetch source.";
    }
    fetch_r1 = columnHandle->generalized_column.column_pointer.result;
    if (fetch_r1 == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified fetch source.";
    }
    columnHandle = findHandle(context, join.fetch2);
    if (columnHandle == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified fetch source.";
    }
    fetch_r2 = columnHandle->generalized_column.column_pointer.result;
    if (fetch_r2 == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified fetch source.";
    }
    columnHandle = findHandle(context, join.select1);
    if (columnHandle == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified select source.";
    }
    select_r1 = columnHandle->generalized_column.column_pointer.result;
    if (select_r1 == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified select source.";
    }
    columnHandle = findHandle(context, join.select2);
    if (columnHandle == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified select source.";
    }
    select_r2 = columnHandle->generalized_column.column_pointer.result;
    if (select_r2 == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return "-- Unable to find specified select source.";
    }

    // create two new Result objects in context// create a new GeneralizedColumnHandle
    GeneralizedColumnHandle join_r1;
    GeneralizedColumnPointer join_r1p;
    join_r1p.result = malloc(sizeof(Result));
    join_r1p.result->data_type = INT;
    join_r1p.result->num_tuples = 0;
    join_r1p.result->payload = NULL;
    GeneralizedColumn join_r1c = {
        .column_type = RESULT,
        .column_pointer = join_r1p
    };
    join_r1.generalized_column = join_r1c;
    strcpy(join_r1.name, join.handle1);
    GeneralizedColumnHandle join_r2;
    GeneralizedColumnPointer join_r2p;
    join_r2p.result = malloc(sizeof(Result));
    join_r2p.result->data_type = INT;
    join_r2p.result->num_tuples = 0;
    join_r2p.result->payload = NULL;
    GeneralizedColumn join_r2c = {
        .column_type = RESULT,
        .column_pointer = join_r2p
    };
    join_r2.generalized_column = join_r2c;
    strcpy(join_r2.name, join.handle2);

    // check size of context and resize if necessary
    if (checkContextSize(context) != true) {
        send_message->status = EXECUTION_ERROR;
        return "-- Problem inserting new handle into client context.";
    }
    // check for duplicate handle names
    int dupIndex = findDuplicateHandle(context, join.handle1);
    if (dupIndex < 0)
        context->chandle_table[context->chandles_in_use++] = join_r1;
    else
        context->chandle_table[dupIndex] = join_r1;
    // check size of context and resize if necessary
    if (checkContextSize(context) != true) {
        send_message->status = EXECUTION_ERROR;
        return "-- Problem inserting new handle into client context.";
    }
    // check for duplicate handle names
    dupIndex = findDuplicateHandle(context, join.handle2);
    if (dupIndex < 0)
        context->chandle_table[context->chandles_in_use++] = join_r2;
    else
        context->chandle_table[dupIndex] = join_r2;

    // holds results from hashtable searches
    size_t size = 1024;
    int intermediate[size];
    int* result1 = NULL;
    int* result2 = NULL;
    int count = 0;
    int capacity = 0;

    // initialize hashtable
    HashTable* ht;
    init(&ht);
    
    // values and indexes
    int* values1 = (int*) fetch_r1->payload;
    int* indexes1 = (int*) select_r1->payload;
    int* values2 = (int*) fetch_r2->payload;
    int* indexes2 = (int*) select_r2->payload;
    
    // insert all values from the first set into the hashtable
    for (size_t i = 0; i < fetch_r1->num_tuples; i++)
        put(ht, values1[i], indexes1[i]);

    // compare against the second array
    for (size_t i = 0; i < fetch_r2->num_tuples; i++) {
        int found = get(ht, values2[i], intermediate, size);
        if (found <= 0)
            continue;
        if (result1 == NULL || result2 == NULL) {
            result1 = malloc(sizeof(int) * size);
            result2 = malloc(sizeof(int) * size);
            capacity = size;
        }
        if (count + found > capacity) {
            int* new_res1 = realloc(result1, sizeof(int) * (capacity + size));
            if (new_res1 != NULL) {
                result1 = new_res1;
            } else {
                send_message->status = EXECUTION_ERROR;
                return "-- Unable to retrieve all values from join.";
            }
            int* new_res2 = realloc(result2, sizeof(int) * (capacity + size));
            if (new_res2 != NULL) {
                result2 = new_res2;
            } else {
                send_message->status = EXECUTION_ERROR;
                return "-- Unable to retrieve all values from join.";
            }
            capacity += size;
        }
        for (int j = count; j < count + found; j++) {
            result1[j] = intermediate[j - count];
            result2[j] = indexes2[i];
        }
        count += found;
    }

    // save results
    join_r1p.result->payload = (void*) result1;
    join_r1p.result->num_tuples = count;
    join_r2p.result->payload = (void*) result2;
    join_r2p.result->num_tuples = count;

    send_message->status = OK_DONE;
    return "-- Successfully completed join.";
}
