#ifndef DB_CATALOG_H
#define DB_CATALOG_H

#include "api/cs165.h"

// frees a table object
void freeTable(Table* tbl);
// frees a database object
void freeDb(Db* db);

#endif
