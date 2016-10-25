#ifndef DB_IO_H
#define DB_IO_H

#define DATA_PATH "./data/"
#define DATA_PATH_LENGTH 7

#define USER_PERM S_IRWXU

#include "cs165_api.h"
#include "db_catalog.h"

int createDatabase(const char* name);
int createTable(const char* db, const char* name);
int createColumn(const char* db, const char* table, const char* name);

char** getDbs();
char** getTables(const char* db);
char** getColumns(const char* db, const char* table);

Table* loadTable(const char* db, const char* table);
size_t* loadColumn(const char* db, const char* table, const char* column);

DbCatalog loadCatalog();

#endif
