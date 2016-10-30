// Abstraction layer between set of client contexts and rest of database

#include "api/cs165.h"

ClientContext* searchContext(int fd);
void insertContext(ClientContext* context);
void deleteContext(ClientContext* context);
