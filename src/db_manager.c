#include "cs165_api.h"
#include "db_io.h"

// In this class, there will always be only one active database at a time
Db *current_db;

Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	(void) num_columns;
	
	// createTable((const char*) db->name, name);
	ret_status->code=OK;
	return NULL;
}

Status add_db(const char* db_name, bool new) {
	(void) new;

	createDatabase(db_name);
	struct Status ret_status;
	
	ret_status.code = OK;
	return ret_status;
}
