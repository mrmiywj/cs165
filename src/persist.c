#include <stdio.h>

#include "api/persist.h"
#include "api/db_io.h"
#include "query/execute.h"
#include "util/debug.h"

extern Db* current_db;

bool loadColumnData() {
    char path[MAX_SIZE_NAME * 3 + DATA_PATH_LENGTH + 3];
    char buf[1024];
    // iterate over every column
    for (size_t i = 0; i < current_db->num_tables; i++) {
        Table* curr_table = current_db->tables[i];
        for (size_t j = 0; j < curr_table->col_count; j++) {
            Column* curr_col = curr_table->columns[j];
            sprintf(path, "%s%s/%s/%s", DATA_PATH, current_db->name, curr_table->name, curr_col->name);
            // REMOVE
            printf("Reading in column from path %s now...\n", path);

            // column data trackers
            size_t data_count = 0;
            size_t data_capacity = 0;
            int* data = NULL; 

            FILE* fp = fopen(path, "r");
            if (fp == NULL)
                return false;
            
            // iterate over all data in file
            while (fgets(buf, sizeof(buf), fp)) {
                // remove \n if necessary
                size_t len = strlen(buf);
                if (buf[len - 1] == '\n')
                    buf[len - 1] = '\0';
                else
                    buf[len] = '\0';
                
                // check capacity
                if (data_count >= data_capacity) {
                    size_t new_size = (data_capacity == 0) ? 1 : 2 * data_capacity;
                    if (data == NULL) {
                        data = malloc(sizeof(int) * new_size);
                    } else {
                        int* new_data = realloc(data, sizeof(int) * new_size);
                        if (new_data == NULL)
                            return false;
                        data = new_data;
                    }
                    data_capacity = new_size;
                }

                // insert new int value
                data[data_count++] = atoi(buf);
            }
            
            // store new data in column
            curr_col->data = data;
        }
    }

    printDatabase(current_db);

    return true;
}

bool writeColumnData() {
    char path[MAX_SIZE_NAME * 3 + DATA_PATH_LENGTH + 3];
    // iterate over every column
    for (size_t i = 0; i < current_db->num_tables; i++) {
        Table* curr_table = current_db->tables[i];
        for (size_t j = 0; j < curr_table->col_count; j++) {
            Column* curr_col = curr_table->columns[j];
            sprintf(path, "%s%s/%s/%s", DATA_PATH, current_db->name, curr_table->name, curr_col->name);
            // REMOVE
            printf("Writing column to path %s now...\n", path);

            FILE* fp = fopen(path, "ab+");
            if (fp == NULL)
                return false;
            
            // iterate over all data in file
            for (size_t k = 0; k < curr_table->num_rows; k++) {
                if (fprintf(fp, "%i\n", curr_col->data[k]) < 0)
                    return false;
            }

            fclose(fp);
        }
    }
    return true;
}

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
    
    fclose(fp);
    return loadColumnData();
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
    return writeColumnData();
}
