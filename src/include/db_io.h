#ifndef DB_IO_H
#define DB_IO_H

#include "cs165_api.h"
#include "db_catalog.h"

typedef enum DbIOResult {
    SUCCESS = 0,
    NOTFOUND = 1,
    BADPERM = 2,
    EXISTS = 4
} DbIOError;

DbIOResult createDatabase(char* name);
DbIOResult createTable(char* name);
DbIOResult createColumn(char* db, char* table, char* name);

char** getDbs();
char** getTables(char* db);
char** getColumns(char* db, char* table);

Table* loadTable(char* db, char* table);
size_t* loadColumn(char* db, char* table, char* column);

DbCatalog loadCatalog();

#endif