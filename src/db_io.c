#include "db_io.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

// creates a specified directory recursively
int createDirectory(const char *path) {
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
    return 0;
}
// creates a specified file
int createFile(char* dir, char* file) {
    // check directory for existence
    struct stat st;
    if (stat(dir, &st) != -1) {
        return ENOENT;
    }
    char filePath[MAX_SIZE_NAME * 3 + 2];
    snprintf(filePath, strlen(dir) + 1 + strlen(file), "%s/%s", dir, file);
    printf("Creating file at %s...\n", filePath);

    FILE* fp = fopen(filePath, "rb+");
    if (fp == NULL) {
        return errno;
    }
    fclose(fp);
    return 0;
}

// creates a database folder
int createDatabase(char* name) {
    int pathLength = DATA_PATH_LENGTH + strlen(name) + 1;
    char path[pathLength];
    sprintf(path, "%s%s", DATA_PATH, name);
    return createDirectory(path);
}
// creates a table folder
int createTable(char* db, char* name) {
    int pathLength = DATA_PATH_LENGTH + strlen(db) + strlen(name) + 2;
    char path[pathLength];
    sprintf(path, "%s%s/%s", DATA_PATH, db, name);
    return createDirectory(path);
}
// creates a column file
int createColumn(char* db, char* table, char* name) {
    int pathLength = DATA_PATH_LENGTH + strlen(db) + strlen(table) + 2;
    char path[pathLength];
    sprintf(path, "%s%s/%s", DATA_PATH, db, name);
    return createFile(path, name);
}

char** getDbs();
char** getTables(char* db);
char** getColumns(char* db, char* table);

Table* loadTable(char* db, char* table);
size_t* loadColumn(char* db, char* table, char* column);

DbCatalog loadCatalog();
