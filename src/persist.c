#include <stdio.h>

#include "api/persist.h"
#include "query/execute.h"
#include "util/debug.h"

extern Db* current_db;

bool startupDb() {
    // open file for reading
    FILE* fp = fopen("./catalog", "r");
    if (fp == NULL)
        return false;
    
    // reading buffer
    char buf[MAX_SIZE_NAME + 20];
    // initialize current_db
    current_db = malloc(sizeof(Db));

    // db values
    Table** tables = NULL;
    size_t table_count = 0;
    size_t table_capacity = 0;
    Column** columns = NULL;
    size_t col_count = 0;
    size_t col_capacity = 0;

    // iterate through file until EOF
    size_t len;
    while (fgets(buf, sizeof(buf), fp)) {
        size_t len = strlen(buf);
        if (buf[len - 1] == '\n') {
            buf[len - 1] = '\0';
        } else {
            buf[len] = '\0';
        }
        // check for db name
        if (strncmp(buf, "D", 1) == 0) {
            strcpy(current_db->name, (buf + 2));
            continue;
        }
        // check for table name
        if (strncmp(buf, "T", 1) == 0) {
            // reset column values
            if (columns != NULL) {
                tables[table_count - 1]->columns = columns;
                tables[table_count - 1]->col_count = col_count;
            } else {
                columns = NULL;
            }
            col_count = 0;
            col_capacity = 0;
            // check for table capacity
            if (table_count >= table_capacity) {
                size_t new_size = (table_capacity == 0) ? 1 : 2 * table_capacity;
                if (tables == NULL) {
                    tables = malloc(sizeof(Table*) * new_size);
                } else {
                    Table** new_tables = realloc(tables, sizeof(Table*) * new_size);
                    if (new_tables == NULL)
                        return false;
                    tables = new_tables;
                }
                table_capacity = new_size;
            }
            // add new table object
            Table* new_table = calloc(1, sizeof(Table));
            strcpy(new_table->name, (buf + 2));
            new_table->columns = NULL;
            new_table->col_count = 0;
            new_table->num_rows = 0;
            new_table->capacity = 0;
            tables[table_count++] = new_table;
            continue;
        }
        // check for col name
        if (strncmp(buf, "C", 1) == 0) {
            // check for column capacity
            if (col_count >= col_capacity) {
                size_t new_size = (col_capacity == 0) ? 1 : 2 * col_capacity;
                if (columns == NULL) {
                    columns = malloc(sizeof(Column*) * new_size);
                } else {
                    Column** new_cols = realloc(columns, sizeof(Column*) * new_size);
                    if (new_cols == NULL)
                        return false;
                    columns = new_cols;
                }
                col_capacity = new_size;
            }
            // add new column object
            Column* new_col = calloc(1, sizeof(Column));
            strcpy(new_col->name, (buf + 2));
            new_col->data = NULL;
            columns[col_count++] = new_col;
            continue;
        }
        // read a line that we don't understand
        return false;
    }
    // store last column in table and tables in db
    tables[table_count - 1]->columns = columns;
    tables[table_count - 1]->col_count = col_count;
    current_db->tables = tables;
    current_db->num_tables = table_count;
    
    printDatabase(current_db);
    
    fclose(fp);
    return true;
}

bool writeDb() {
    if (current_db == NULL)
        return true;
    // open file
    FILE* fp = fopen("./catalog", "ab+");
    if (fp == NULL)
        return false;
    
    // db name
    if (fprintf(fp, "D %s\n", current_db->name) < 0)
        return false;
    
    // table names
    for (size_t i = 0; i < current_db->num_tables; i++) {
        if (fprintf(fp, "T %s\n", current_db->tables[i]->name) < 0)
            return false;
        
        // column names
        for (size_t j = 0; j < current_db->tables[i]->col_count; j++) {
            if (fprintf(fp, "C %s\n", current_db->tables[i]->columns[j]->name) < 0)
                return false;
        }
    }
    fclose(fp);
    return true;
}
