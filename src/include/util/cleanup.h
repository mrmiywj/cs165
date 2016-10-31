#ifndef CLEANUP_H
#define CLEANUP_H

#include "api/cs165.h"

// frees a table object
void freeTable(Table* tbl);
// frees a database object
void freeDb(Db* db);

#endif
