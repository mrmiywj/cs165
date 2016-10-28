#include "db_execute.h"
#include "debug_utils.h"

/** execute_DbOperator takes as input the DbOperator and executes the query. **/
char* execute_DbOperator(DbOperator* query) {
    if (query == NULL) {
        return "We couldn't parse your request; please try again!";
    }
    printDbOperator(query);
    free(query);
    return "Parsed query...";
}