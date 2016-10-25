#include "cs165_api.h"
#include "db_io.h"
#include "db_catalog.h"

#include <string.h>

// In this class, there will always be only one active database at a time
Db *current_db;

Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	(void) num_columns;
	
	createTable((const char*) db->name, name);
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

	freeDb(current_db);
	current_db = malloc(sizeof(Db));
	strcpy(current_db->name, db_name);
	current_db->tables = NULL;
	current_db->tables_size = 0;
	current_db->tables_capacity = 0;

	createDatabase(db_name);
	
	ret_status.code = OK;
	return ret_status;
}
