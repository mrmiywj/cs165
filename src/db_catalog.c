#include "db_catalog.h"

// frees a table object
void freeTable(Table* tbl) {
    if (tbl == NULL)
        return;
    for (size_t i = 0; i < tbl->col_count; i++)
        free(tbl->columns[i]);
    free(tbl);
}

// frees a database object
void freeDb(Db* db) {
    if (db == NULL)
        return;
    for (size_t i = 0; i < db->tables_size; i++)
        freeTable(db->tables[i]);
    free(db);
}
