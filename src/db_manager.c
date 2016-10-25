#include "cs165_api.h"

// In this class, there will always be only one active database at a time
Db *current_db;

Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	(void) db;
	(void) name;
	(void) num_columns;
	ret_status->code=OK;
	return NULL;
}

Status add_db(const char* db_name, bool new) {
	(void) db_name;
	(void) new;

	struct Status ret_status;
	
	ret_status.code = OK;
	return ret_status;
}
