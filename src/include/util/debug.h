#ifndef DEBUG_UTILS_H
#define DEBUG_UTILS_H

#include "api/cs165.h"
#include "util/log.h"

/* Prints a DbOperator object. */
void printDbOperator(DbOperator* query);
void printDatabase(Db* db);
void printTable(Table* tbl, char* prefix);
void printColumn(Column* col, char* prefix, size_t nvals);
void printContext(ClientContext* context);

#endif