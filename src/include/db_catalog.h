#ifndef DB_CATALOG_H
#define DB_CATALOG_H

#include "cs165_api.h"

// table metadata struct
typedef struct TableInfo {
    char name[MAX_SIZE_NAME];           // name of table
    size_t col_count;                   // number of columns in database
} TableInfo;

// database catalog file
typedef struct DbCatalog {
    char db_name[MAX_SIZE_NAME;         // name of active database
    size_t table_count;                 // number of tables in database
    TableInfo* table_info;              // table metadata
} DbCatalog;

#endif