#include "util/log.h"
#include "util/debug.h"

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
        default:
            break;
    }

    /* DbOperator.context */
    // log_info("Context: ");
    // log_info("         %i handles in use\n", query->context->chandles_in_use);
    // log_info("         %i handle slots\n", query->context->chandle_slots);

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
    log_info("    Table size: %i\n", db->tables_size);
    log_info("    Table maxsize: %i\n", db->tables_capacity);

    // print each of the tables
    for (size_t i = 0; i < db->num_tables; i++) {
        log_info("    Table %i:\n", i);
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
        log_info("%sColumn %i:\n", prefix, i);
        printColumn(tbl->columns[i], next_prefix, tbl->num_rows);
    }
}

/* Prints a description of a Column object. */
void printColumn(Column* col, char* prefix, size_t nvals) {
    log_info("%sName: %s\n", prefix, col->name);
    if (nvals == 0) {
        log_info("%sNo values\n", prefix);
    }
    log_info("%sValues: [ ", prefix);
    for (size_t i = 0; i < nvals; i++) {
        log_info("%i ", col->data[i]);
    }
    log_info("]\n");
}
