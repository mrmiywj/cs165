#include <string.h>

#include "api/cs165.h"
#include "api/db_io.h"
#include "util/cleanup.h"
#include "util/log.h"

/**
 * External catalog file that will be used to index the current database at all times.
 * Will be initialized at startup and written to file at shutdown.
 */
Db *current_db;

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
    for (size_t i = 0, size = db->tables_size; i < size; i++)
        freeTable(db->tables[i]);
    free(db);
}

Column* create_column(char *name, Table *table, bool sorted, Status *ret_status) {
	(void) sorted;
	createColumn(current_db->name, table->name, name);
	ret_status->code=OK;
	return NULL;
}

Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	(void) num_columns;
	
	// create the actual table directory
	createTable((const char*) db->name, name);

	// return OK
	ret_status->code=OK;
	return NULL;
}

Status add_db(const char* db_name, bool new) {
	(void) new;

	struct Status ret_status;

	// trying to add the current db again, no need to do anything
	if (current_db != NULL && strcmp(current_db->name, db_name) == 0) {
		ret_status.code = ERROR;
		return ret_status;
	}

	// destroy current db and replace with new db object
	freeDb(current_db);
	current_db = malloc(sizeof(Db));
	strcpy(current_db->name, db_name);
	current_db->tables = NULL;
	current_db->tables_size = 0;
	current_db->tables_capacity = 0;

	// create the actual database directory
	createDatabase(db_name);
	
	// return OK
	ret_status.code = OK;
	return ret_status;
}
