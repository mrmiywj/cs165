#ifndef DB_CATALOG_H
#define DB_CATALOG_H

#include "cs165_api.h"

// database catalog file
typedef struct DbCatalog {
    char db_name[MAX_SIZE_NAME;         // name of active database
    size_t table_count;                 // number of tables in database
    Table* tables;                      // tables
} DbCatalog;

#endif