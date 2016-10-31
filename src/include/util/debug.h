#ifndef __DEBUG_UTILS__
#define __DEBUG_UTILS__

#include "api/cs165.h"
#include "util/log.h"

/* Prints a DbOperator object. */
void printDbOperator(DbOperator* query);
void printDatabase(Db* db);
void printTable(Table* tbl, char* prefix);
void printColumn(Column* col, char* prefix, size_t nvals);
void printContext(ClientContext* context);

#endif /* __DEBUG_UTILS__ */
