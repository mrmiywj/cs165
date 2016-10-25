#include "db_io.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

DbIOResult createDatabase(char* name) {
    // construct string
    int pathLength = DATA_PATH_LENGTH + strlen(name) + 1;
    char path[pathLength];
    if (sprintf(path, pathLength, "%s%s", DATA_PATH, name) != pathLength) {
        return RUNTIME;
    }

    // check directory
    struct stat st = {0};
    if (stat(path, &st) == -1) {
        if (mkdir(path, 0700) != 0) {
            return RUNTIME;
        }
    } else {
        return EXISTS;
    }

    return SUCCESS;
}

DbIOResult createTable(char* name);
DbIOResult createColumn(char* db, char* table, char* name);

char** getDbs();
char** getTables(char* db);
char** getColumns(char* db, char* table);

Table* loadTable(char* db, char* table);
size_t* loadColumn(char* db, char* table, char* column);

DbCatalog loadCatalog();