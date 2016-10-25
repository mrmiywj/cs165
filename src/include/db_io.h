#ifndef DB_IO_H
#define DB_IO_H

#define DATA_PATH "./data/"
#define DATA_PATH_LENGTH 7

#define USER_PERM S_IRWXU

#include "cs165_api.h"
#include "db_catalog.h"

int createDatabase(char* name);
int createTable(char* db, char* name);
int createColumn(char* db, char* table, char* name);

char** getDbs();
char** getTables(char* db);
char** getColumns(char* db, char* table);

Table* loadTable(char* db, char* table);
size_t* loadColumn(char* db, char* table, char* column);

DbCatalog loadCatalog();

#endif
