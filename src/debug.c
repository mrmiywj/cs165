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
            if (fields.insert.table == NULL) {
                log_info("\tNo table object\n");
            } else {
                log_info("\tInsert table: %s\n", fields.insert.table->name);
                log_info("\t    %i columns\n", fields.insert.table->col_count);
                log_info("\t    %i table length\n", fields.insert.table->table_length);
            }

            /* DbOperator.fields.insert.values */
            log_info("\tInsert values: [ ");
            if (fields.insert.values != NULL) {
                for (size_t i = 0; i < sizeof(fields.insert.values) / sizeof(int); i++) {
                    log_info("%i ", fields.insert.values[i]);
                }
            }
            log_info("]\n");
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
    log_info("\tName: %s\n", db->name);
    log_info("\tNum tables: %i\n", db->num_tables);
    log_info("\tTable size: %i\n", db->tables_size);
    log_info("\tTable maxsize: %i\n", db->tables_capacity);

    // print each of the tables
    for (size_t i = 0; i < db->num_tables; i++) {
        log_info("\tTable %i:\n", i);
        printTable(db->tables[i], "\t    ");
    }
    log_info("\n");
}

/* Prints a description of a Table object. */
void printTable(Table* tbl, char* prefix) {
    // Table.(name, col_count, table_length)
    log_info("%sName: %s\n", prefix, tbl->name);
    log_info("%s# Columns: %i\n", prefix, tbl->col_count);
    log_info("%sTable length: %i\n", prefix, tbl->table_length);
    
    // print each of the columns
    char next_prefix[16];
    sprintf(next_prefix, "%s%s", prefix, "    ");
    for (size_t i = 0; i < tbl->col_count; i++) {
        log_info("%sColumn %i:\n", prefix, i);
        printColumn(tbl->columns[i], next_prefix, tbl->table_length);
    }
}

/* Prints a description of a Column object. */
void printColumn(Column* col, char* prefix, size_t nvals) {
    log_info("%sName: %s\n", prefix, col->name);
    if (nvals == 0) {
        log_info("%sNo values\n", prefix);
    }
    for (size_t i = 0; i < nvals; i++) {
        log_info("%s    Value %i: %i\n", prefix, col->data[i]);
    }
}
