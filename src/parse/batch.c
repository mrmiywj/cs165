#include "parse/batch.h"

DbOperator* parse_batch(char* arguments, message* response) {
    response->status = OK_DONE;
    arguments[7] = '\0';
    
    // create DbOperator and return
    DbOperator* result = malloc(sizeof(DbOperator));
    result->type = OP_BATCH;
    result->fields.batch = (BatchOperator) {
        .start = strcmp(arguments, "queries") == 0
    };
    return result;
}
