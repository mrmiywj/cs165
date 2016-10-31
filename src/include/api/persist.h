#ifndef PERSIST_H
#define PERSIST_H

#include "api/cs165.h"
#include "db_io.h"

Db* startupDb();
bool writeDb();

#endif
