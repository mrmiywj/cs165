// Abstraction layer between set of client contexts and rest of database

#include "api/cs165.h"

ClientContext* searchContext(int fd);
GeneralizedColumnHandle* findHandle(ClientContext* context, char* handle);
void insertContext(ClientContext* context);
void deleteContext(ClientContext* context);
bool checkContextSize(ClientContext* context);
