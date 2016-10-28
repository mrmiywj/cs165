#include <string.h>

#include "client_context.h"

extern Db* current_db;

Table* lookup_table(char *name) {
	// check for NULL name
	if (name == NULL)
		return NULL;
	
	// search through current database's tables
	for (size_t i = 0, count = current_db->tables_size; i < count; i++) {
		if (strcmp(current_db->tables[i]->name, name) == 0) {
			return current_db->tables[i];
		}
	}

	return NULL;
}
