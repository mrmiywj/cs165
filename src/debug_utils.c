#include "debug_utils.h"

/* Prints a DbOperator object. */
void printDbOperator(DbOperator* query) {
    #ifdef LOG_INFO
        if (query == NULL) {
            log_info("<null DbOperator object>\n");
            return;
        }

        /* DbOperator.client_fd */
        log_info("Client FD: %i\n", query->client_fd);

        OperatorFields fields = query->fields;
        /* DbOperator.type */
        switch (query->type) {
            case CREATE:
                log_info("Create:\n");
                break;
            case INSERT:
                log_info("Insert:\n");
                /* DbOperator.fields.insert.table.name */
                if (fields.insert.table == NULL) {
                    log_info("\tInsert: No table object\n");
                } else {
                    log_info("\tInsert table: %s\n", fields.insert.table->name);
                    log_info("\t            : %i columns\n", fields.insert.table->col_count);
                    log_info("\t            : %i table length\n", fields.insert.table->table_length);
                }

                /* DbOperator.fields.insert.values */
                log_info("\tInsert Values: [ ");
                if (fields.insert.values != NULL) {
                    for (size_t i = 0; i < sizeof(fields.insert.values) / sizeof(int); i++) {
                        log_info("%i ", fields.insert.values[i]);
                    }
                }
                log_info("]\n");
                break;
            case LOADER:
                log_info("Loader:\n");
                /* DbOperator.fields.loader.db_name */
                if (fields.loader.file_name == NULL) {
                    log_info("\tLoader: No database name\n");
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
    #else 
        (void) query;
    #endif
}
