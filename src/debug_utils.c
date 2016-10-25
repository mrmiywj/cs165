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

        OperatorFields fields = query->operator_fields;
        /* DbOperator.type */
        switch (query->type) {
            case CREATE:
                log_info("Create:\n");
                break;
            case INSERT:
                log_info("Insert:\n");
                /* DbOperator.operator_fields.insert_operator.table.name */
                if (fields.insert_operator.table == NULL) {
                    log_info("\tInsert: No table object\n");
                } else {
                    log_info("\tInsert table: %s\n", fields.insert_operator.table->name);
                    log_info("\t            : %i columns\n", fields.insert_operator.table->col_count);
                    log_info("\t            : %i table length\n", fields.insert_operator.table->table_length);
                }

                /* DbOperator.operator_fields.insert_operator.values */
                log_info("\tInsert Values: [ ");
                if (fields.insert_operator.values != NULL) {
                    for (int i = 0; i < sizeof(fields.insert_operator.values) / sizeof(int); i++) {
                        log_info("%i ", fields.insert_operator.values[i]);
                    }
                }
                log_info("]\n");
                break;
            case OPEN:
                log_info("Open:\n");
                /* DbOperator.operator_fields.open_operator.db_name */
                if (fields.open_operator.db_name == NULL) {
                    log_info("\tOpen: No database name\n");
                } else {
                    log_info("\tOpen: database %s\n", fields.open_operator.db_name);
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
