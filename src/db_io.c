#include "db_io.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

// creates a specified directory recursively
enum DbIOResult createDirectory(const char *path) {
    char tmp[MAX_SIZE_NAME * 2];

    snprintf(tmp, sizeof(tmp), "%s", path);
    size_t len = strlen(tmp);

    if (tmp[len - 1] == '/') {
        tmp[len - 1] = 0;
    }
    for (char* p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, USER_PERM);
            *p = '/';
        }
        if (*p == 0) {
            mkdir(tmp, USER_PERM);
        }
    }
    return SUCCESS;
}
// creates a specified file
enum DbIOResult createFile(char* dir, char* file) {
    // check directory for existence
    struct stat st = {0};
    if (stat(dir, &st) != -1) {
        return NOEXIST;
    }
    char* filePath[MAX_SIZE_NAME * 3 + 2];
    snprintf(filePath, strlen(dir) + 1 + strlen(file), "%s/%s", dir, file);
    printf("Creating file at %s...\n", filePath);

    FILE* fp = fopen(filePath, "rb+");
    if (fp == NULL) {
        return RUNTIME;
    }
    fclose(fp);
    return SUCCESS;
}

// creates a database folder
enum DbIOResult createDatabase(char* name) {
    int pathLength = DATA_PATH_LENGTH + strlen(name) + 1;
    char path[pathLength];
    if (snprintf(path, pathLength, "%s%s", DATA_PATH, name) != pathLength) {
        return RUNTIME;
    }
    return createDirectory(path);
}
// creates a table folder
enum DbIOResult createTable(char* db, char* name) {
    int pathLength = DATA_PATH_LENGTH + strlen(db) + strlen(name) + 2;
    char path[pathLength];
    if (snprintf(path, pathLength, "%s%s/%s", DATA_PATH, db, name) != pathLength) {
        return RUNTIME;
    }
    return createDirectory(path);
}
// creates a column file
enum DbIOResult createColumn(char* db, char* table, char* name) {
    int pathLength = DATA_PATH_LENGTH + strlen(db) + strlen(table) + 2;
    char path[pathLength];
    if (snprintf(path, pathLength, "%s%s/%s", DATA_PATH, db, table) != pathLength) {
        return RUNTIME;
    }
    return createFile(path, name);
}

char** getDbs();
char** getTables(char* db);
char** getColumns(char* db, char* table);

Table* loadTable(char* db, char* table);
size_t* loadColumn(char* db, char* table, char* column);

DbCatalog loadCatalog();