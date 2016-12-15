// Abstraction layer between set of client contexts and rest of database

#ifndef CONTEXT_H
#define CONTEXT_H

#include "api/cs165.h"

ClientContext* searchContext(int fd);
GeneralizedColumnHandle* findHandle(ClientContext* context, char* handle);
int findDuplicateHandle(ClientContext* context, char* handle);
void insertContext(ClientContext* context);
void deleteContext(ClientContext* context);
bool checkContextSize(ClientContext* context);

#endif
