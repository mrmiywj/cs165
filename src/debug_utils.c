#include "debug_utils.h"

/* Prints a DbOperator object. */
void printDbOperator(DbOperator* query) {
    #ifdef LOG_INFO
        if (query == NULL) {
            printf("<null DbOperator object>\n");
            return;
        }

        /* DbOperator.client_fd */
        printf("Client FD: %i\n", query->client_fd);

        OperatorFields fields = query->operator_fields;
        /* DbOperator.type */
        switch (query->type) {
            case CREATE:
                printf("Create:\n");
                break;
            case INSERT:
                printf("Insert:\n");
                /* DbOperator.operator_fields.insert_operator.table.name */
                if (fields.insert_operator.table == NULL) {
                    printf("\tInsert: No table object\n");
                } else {
                    printf("\tInsert table: %s\n", fields.insert_operator.table->name);
                    printf("\t            : %i columns\n", fields.insert_operator.table->col_count);
                    printf("\t            : %i table length\n", fields.insert_operator.table->table_length);
                }

                /* DbOperator.operator_fields.insert_operator.values */
                printf("\tInsert Values: [ ");
                if (fields.insert_operator.values != NULL) {
                    for (int i = 0; i < sizeof(fields.insert_operator.values) / sizeof(int); i++) {
                        printf("%i ", fields.insert_operator.values[i]);
                    }
                }
                printf("]\n");
                break;
            case OPEN:
                printf("Open:\n");
                /* DbOperator.operator_fields.open_operator.db_name */
                if (fields.open_operator.db_name == NULL) {
                    printf("\tOpen: No database name\n");
                } else {
                    printf("\tOpen: database %s\n", fields.open_operator.db_name);
                }
                break;
            default:
                break;
        }

        /* DbOperator.context */
        // printf("Context: ");
        // printf("         %i handles in use\n", query->context->chandles_in_use);
        // printf("         %i handle slots\n", query->context->chandle_slots);

        printf("\n");
    #endif
}