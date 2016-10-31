#include <string.h>

#include "api/cs165.h"
#include "api/db_io.h"
#include "util/cleanup.h"
#include "util/log.h"

extern Db* current_db;

// frees a table object
void freeTable(Table* tbl) {
    if (tbl == NULL)
        return;
    for (size_t i = 0, count = tbl->col_count; i < count; i++)
        free(tbl->columns[i]);
    free(tbl);
}

// frees a database object
void freeDb(Db* db) {
    if (db == NULL)
        return;
    for (size_t i = 0, size = db->num_tables; i < size; i++)
        freeTable(db->tables[i]);
    free(db);
}
