#include "util/debug.h"
#include "util/log.h"

/* Prints a description of a DbOperator object. */
void printDbOperator(DbOperator* query) {
    if (query == NULL) {
        log_info("DbOperator: NULL\n");
        return;
    }
    log_info("DbOperator at %p:\n", query);

    /* DbOperator.client_fd */
    log_info("\tClient FD: %i\n", query->client_fd);

    OperatorFields fields = query->fields;
    /* DbOperator.type */
    switch (query->type) {
        case CREATE:
            log_info("\tType: CREATE\n");
            // DbOperator.fields.create.type
            CreateType type = fields.create.type;
            switch (type) {
            case CREATE_DATABASE:
                log_info("\tCreate Type: DATABASE\n");
                break;
            case CREATE_TABLE:
                log_info("\tCreate Type: TABLE\n");
                break;
            case CREATE_COLUMN:
                log_info("\tCreate Type: COLUMN\n");
                break;
            default:
                break;
            }

            // DbOperator.fields.create.params
            log_info("\tParameters: \n");
            for (size_t i = 0; i < fields.create.num_params; i++) {
                log_info("\t    %4i: %s\n", i, fields.create.params[i]);
            }
            break;
        case INSERT:
            log_info("\tType: INSERT\n");
            /* DbOperator.fields.insert.table.name */
            if (fields.insert.tbl_name == NULL) {
                log_info("\tNo table object\n");
            } else {
                log_info("\tTable name: %s\n", fields.insert.tbl_name);
            }

            /* DbOperator.fields.insert.values */
            log_info("\tInsert values: [ ");
            if (fields.insert.values != NULL) {
                for (size_t i = 0; i < fields.insert.num_values; i++) {
                    log_info("%i ", fields.insert.values[i]);
                }
            }
            log_info("]\n");

            /* DbOperator.fields.insert.num_values */
            log_info("\t# values: %i\n", fields.insert.num_values);
            break;
        case LOADER:
            log_info("\tType: LOADER\n");
            /* DbOperator.fields.loader.db_name */
            if (fields.loader.file_name == NULL) {
                log_info("\tNo database name\n");
            } else {
                log_info("\tLoader: database %s\n", fields.loader.file_name);
            }
            break;
        case SELECT:
            log_info("\tType: SELECT\n");
            log_info("\t    DB: %s\n", fields.select.db_name);
            log_info("\t    TBL: %s\n", fields.select.tbl_name);
            log_info("\t    COL: %s\n", fields.select.col_name);
            log_info("\t    MIN: %i\n", fields.select.minimum);
            log_info("\t    MAX: %i\n", fields.select.maximum);
            break;
        case PRINT:
            log_info("\tType: PRINT\n");
            log_info("\t    HANDLE: %s\n", fields.print.handle);
            break;
        case FETCH:
            log_info("\tType: FETCH\n");
            log_info("\t    DB: %s\n", fields.fetch.db_name);
            log_info("\t    TBL: %s\n", fields.fetch.tbl_name);
            log_info("\t    COL: %s\n", fields.fetch.col_name);
            log_info("\t    SOURCE: %s\n", fields.fetch.source);
            log_info("\t    TARGET: %s\n", fields.fetch.target);
            break;
        case MATH:
            log_info("\tType: MATH\n");
            log_info("\t    MathType: %i\n", fields.math.type);
            for (size_t i = 0; i < fields.math.num_params; i++) {
                log_info("\t    PARAM%i:%s\n", i, fields.math.params[i]);
            }
            log_info("\t    First arg is var: %i\n", fields.math.is_var);
        default:
            break;
    }
    log_info("\n");
}

/* Prints a description of a Db object. */
void printDatabase(Db* db) {
    if (db == NULL) {
        log_info("Db object: NULL\n");
        return;
    }
    log_info("Db object at %p\n", db);

    // Db.(name, num_tables, tables_size, tables_capacity)
    log_info("    Name: %s\n", db->name);
    log_info("    Num tables: %i\n", db->num_tables);
    log_info("    Table maxsize: %i\n", db->tables_capacity);

    // print each of the tables
    for (size_t i = 0; i < db->num_tables; i++) {
        log_info("    Table %i at %p:\n", i, db->tables[i]);
        printTable(db->tables[i], "\t");
    }
    log_info("\n");
}

/* Prints a description of a Table object. */
void printTable(Table* tbl, char* prefix) {
    // Table.(name, col_count, num_rows)
    log_info("%sName: %s\n", prefix, tbl->name);
    log_info("%s# Columns: %i\n", prefix, tbl->col_count);
    log_info("%s# Rows: %i\n", prefix, tbl->num_rows);
    log_info("%sCapacity: %i\n", prefix, tbl->capacity);
    
    // print each of the columns
    char next_prefix[16];
    sprintf(next_prefix, "%s%s", prefix, "    ");
    for (size_t i = 0; i < tbl->col_count; i++) {
        log_info("%sColumn %i at %p:\n", prefix, i, tbl->columns[i]);
        printColumn(tbl->columns[i], next_prefix, tbl->num_rows);
    }
}

/* Prints a description of a Column object. */
void printColumn(Column* col, char* prefix, size_t nvals) {
    log_info("%sName: %s\n", prefix, col->name);
    if (nvals == 0) {
        log_info("%sNo values\n", prefix);
    }
    log_info("%sValues at %p: [ ", prefix, col->data);
    for (size_t i = 0; i < nvals; i++) {
        log_info("%i ", col->data[i]);
    }
    log_info("]\n");
}

/* Prints a description of a ClientContext object. */
void printContext(ClientContext* context) {
    if (context == NULL) {
        log_info("Client context: NULL\n");
        return;
    }
    log_info("Client context at %p\n", context);
    log_info("    # handles: %i\n", context->chandles_in_use);
    log_info("    capacity:  %i\n", context->chandle_slots);
    log_info("    client fd: %i\n", context->client_fd);
    for (int i = 0; i < context->chandles_in_use; i++) {
        GeneralizedColumnHandle handle = context->chandle_table[i];
        log_info("    -> handle %i (%s):\n", i, handle.name);
        GeneralizedColumn gcol = handle.generalized_column;
        switch (gcol.column_type) {
            case RESULT: {
                Result* result = gcol.column_pointer.result;
                log_info("         Type: RESULT\n");
                log_info("         # tuples: %i\n", result->num_tuples);
                switch (result->data_type) {
                    case INT: {
                        log_info("         Data type: INT\n");
                        log_info("         Values: [ ");
                        int* data = (int*) result->payload;
                        for (size_t j = 0; j < result->num_tuples; j++) {
                            log_info("%i ", data[j]);
                        }
                        log_info("]\n");
                        break; 
                    }           
                    case LONG: {
                        log_info("         Data type: INT\n");
                        log_info("         Values: [ ");
                        long* data = (long*) result->payload;
                        for (size_t j = 0; j < result->num_tuples; j++) {
                            log_info("%l ", data[j]);
                        }
                        log_info("]\n");
                        break;
                    }
                    case FLOAT: {
                        log_info("         Data type: INT\n");
                        log_info("         Values: [ ");
                        float* data = (float*) result->payload;
                        for (size_t j = 0; j < result->num_tuples; j++) {
                            log_info("%f ", data[j]);
                        }
                        log_info("]\n");
                        break;
                    }
                    case DOUBLE: {
                        log_info("         Data type: DOUBLE\n");
                        log_info("         VALUES: [ ");
                        double* data = (double*) result->payload;
                        for (size_t j = 0; j < result->num_tuples; j++) {
                            log_info("%f ", data[j]);
                        }
                        log_info("]\n");
                        break;
                    }
                    default: 
                        break;
                }
                break;
            }
            case COLUMN: {
                Column* column = gcol.column_pointer.column;
                log_info("        Type: COLUMN\n");
                log_info("        Col name: %s\n", column->name);
                break;
            }
            default:
                break;
        }
    }
    log_info("\n");
}
