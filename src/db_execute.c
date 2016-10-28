#include "db_execute.h"
#include "debug_utils.h"

/** execute_DbOperator takes as input the DbOperator and executes the query. **/
char* executeDbOperator(DbOperator* query) {
    if (query == NULL)
        return "Invalid query.";

    printDbOperator(query);

    switch(query->type) {
    case CREATE:
        return handleCreateQuery(query);
    case INSERT:
        return handleInsertQuery(query);
    case LOADER:
        return handleLoaderQuery(query);
    default:
        break;
    }

    free(query);
    
    return "Executed query successfully.";
}

// ================ HANDLERS ================
char* handleCreateQuery(DbOperator* query) {
    (void) query;
    return "Not implemented yet.";
}
char* handleInsertQuery(DbOperator* query) {
    (void) query;
    return "Not implemented yet.";
}
char* handleLoaderQuery(DbOperator* query) {
    (void) query;
    return "Not implemented yet.";
}
