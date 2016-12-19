#include <stdio.h>

#include "api/sorted.h"
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
        
        // load all columns
        for (size_t j = 0; j < curr_table->col_count; j++) {
            Column* curr_col = curr_table->columns[j];
            sprintf(path, "%s%s/%s/%s", DATA_PATH, current_db->name, curr_table->name, curr_col->name);
            log_info("-- Reading in column from path %s now...\n", path);
        
            // column data trackers
            size_t data_count = 0;
            size_t data_capacity = 0;
            int* values = NULL;

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
                    if (values == NULL) {
                        values = malloc(sizeof(int) * new_size);
                    } else {
                        int* new_data = realloc(values, sizeof(int) * new_size);
                        if (new_data == NULL)
                            return false;
                        values = new_data;
                    }
                    data_capacity = new_size;
                }

                // insert new int value
                values[data_count++] = atoi(buf);
            }

            // store new data in column
            curr_col->data = values;
            curr_table->num_rows = data_count;
            curr_table->capacity = data_capacity;

            fclose(fp);
        }

        // load all indexes
        sprintf(path, "%s%s/%s/index", DATA_PATH, current_db->name, curr_table->name);
        FILE* fp = fopen(path, "r");

        // iterate over all data in file
        Index* curr_index = NULL;
        int curr_capacity = 0;
        int curr_inserted = 0;
        while (fgets(buf, sizeof(buf), fp)) {
            // remove \n if necessary
            size_t len = strlen(buf);
            if (buf[len - 1] == '\n')
                buf[len - 1] = '\0';
            else
                buf[len] = '\0';

            // tokenize
            char* token1 = buf;
            char* token2 = buf;
            while (*token2 != ' ')
                token2++;
            *token2 = '\0';
            token2++;
                        
            char* token3 = token2;
            while (*token3 != ' ' && *token3 != '\0')
                token3++;
            bool new_table = (*token3 == ' ');
            *token3 = '\0';
            if (new_table) {
                // this is a new table
                token3++;
                char* col_name = token1;
                IndexType type = strcmp(token2, "B") == 0 ? BTREE : SORTED;
                bool clustered = strcmp(token3, "C") == 0;

                // find this index
                for (size_t j = 0; j < curr_table->num_indexes; j++)
                    if (strcmp(curr_table->indexes[j]->column->name, col_name) == 0)
                        if (curr_table->indexes[j]->type == type && curr_table->indexes[j]->clustered == clustered)
                            curr_index = curr_table->indexes[j];

                // now continue
                curr_capacity = 0;
                curr_inserted = 0;
                continue;
            } else {
                // these tokens are two new values to insert
                int value = atoi(token1);
                int index = atoi(token2);
                
                // resize unclustered sorted column if necessary
                if (curr_index->type == SORTED && !curr_index->clustered) {
                    if (curr_inserted >= curr_capacity - 1) {
                        int new_size = (curr_capacity == 0) ? 1 : 2 * curr_capacity;
                        if (new_size == 1) {
                            curr_index->object->column->values = malloc(sizeof(int));
                            curr_index->object->column->indexes = malloc(sizeof(int));
                        } else {
                            int* new_array1 = realloc(curr_index->object->column->values, sizeof(int) * new_size);
                            int* new_array2 = realloc(curr_index->object->column->indexes, sizeof(int) * new_size);
                            if (new_array1 != NULL) {
                                curr_index->object->column->values = new_array1;
                            } else {
                                return false;
                            }
                            if (new_array2 != NULL) {
                                curr_index->object->column->indexes = new_array2;
                            } else {
                                return false;
                            }
                        }
                        curr_capacity = new_size;
                    }
                }

                // insert new value into index
                switch (curr_index->type) {
                    case BTREE:
                        if (curr_index->clustered) {
                            insertValueC(&(curr_index->object->btreec), value);
                        } else {
                            insertValueU(&(curr_index->object->btreeu), value, index);
                        }
                        break;
                    case SORTED:
                        if (!curr_index->clustered) {
                            insertIndex(curr_index->object->column, value, index, curr_inserted);
                        }
                        break;
                }

                curr_inserted++;
            }
        }
    }

    log_info("-- Loaded column data successfully.\n");
    printDatabase(current_db);

    return true;
}

bool writeColumnData() {
    char path[MAX_SIZE_NAME * 3 + DATA_PATH_LENGTH + 30];
    // iterate over every table
    for (size_t i = 0; i < current_db->num_tables; i++) {
        Table* curr_table = current_db->tables[i];
        
        // write each column to file
        for (size_t j = 0; j < curr_table->col_count; j++) {
            Column* curr_col = curr_table->columns[j];
            sprintf(path, "%s%s/%s/%s", DATA_PATH, current_db->name, curr_table->name, curr_col->name);

            FILE* fp = fopen(path, "w+");
            if (fp == NULL)
                return false;
            
            // iterate over all data in file
            for (size_t k = 0; k < curr_table->num_rows; k++) {
                if (fprintf(fp, "%i\n", curr_col->data[k]) < 0)
                    return false;
            }

            // fclose(fp);
        }
        
        // open index and write indexes to file
        sprintf(path, "%s%s/%s/index", DATA_PATH, current_db->name, curr_table->name);
        FILE* fp = fopen(path, "w+");
        if (fp == NULL)
            return false;
        
        // iterate over each index
        for (size_t j = 0; j < curr_table->num_indexes; j++) {
            Index* index = curr_table->indexes[j];
            char type = index->clustered ? 'C' : 'U';
            switch (index->type) {
                case BTREE:
                    fprintf(fp, "%s B %c\n", index->column->name, type);
                    if (index->clustered) {
                        BTreeCNode* ptr = index->object->btreec;
                        while (ptr->type != LEAF) {
                            ptr = ptr->object.parent.children[0];
                        }
                        BTreeCLeaf* leaf = &(ptr->object.leaf);
                        while (leaf != NULL) {
                            for (size_t k = 0; k < leaf->num_elements; k++) {
                                fprintf(fp, "%i %i\n", leaf->values[k], leaf->indexes[k]);
                            }
                            leaf = leaf->next;
                        }
                    } else {
                        BTreeUNode* ptr = index->object->btreeu;
                        while (ptr->type != LEAF) {
                            ptr = ptr->object.parent.children[0];
                        }
                        BTreeULeaf* leaf = &(ptr->object.leaf);
                        while (leaf != NULL) {
                            for (size_t k = 0; k < leaf->num_elements; k++) {
                                fprintf(fp, "%i %i\n", leaf->values[k], leaf->indexes[k]);
                            }
                            leaf = leaf->next;
                        }
                    }
                    break;
                case SORTED:
                    fprintf(fp, "%s S %c\n", index->column->name, type);
                    if (index->clustered) {
                        for (size_t k = 0; k < curr_table->num_rows; k++) {
                            fprintf(fp, "%i %zu\n", index->column->data[k], k);
                        }
                    } else {
                        ColumnIndex* cindex = index->object->column;
                        for (size_t k = 0; k < curr_table->num_rows; k++) {
                            fprintf(fp, "%i %i\n", cindex->values[k], cindex->indexes[k]);
                        }
                    }
                    break;
            }
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
    current_db = calloc(1, sizeof(Db));

    // db values
    Table** tables = NULL;
    size_t table_count = 0;
    size_t table_capacity = 0;
    Column** columns = NULL;
    Index** indexes = NULL;
    size_t col_count = 0;
    size_t col_capacity = 0;
    size_t index_count = 0;
    size_t index_capacity = 0;

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
                tables[table_count - 1]->indexes = indexes;
                tables[table_count - 1]->num_indexes = index_count;
            }
            columns = NULL;
            indexes = NULL;
            col_count = 0;
            col_capacity = 0;
            index_count = 0;
            index_capacity = 0;
            // check for table capacity
            if (table_count >= table_capacity) {
                size_t new_size = (table_capacity == 0) ? 1 : 2 * table_capacity;
                if (tables == NULL) {
                    tables = calloc(new_size, sizeof(Table*));
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
            new_table->indexes = NULL;
            new_table->col_count = 0;
            new_table->num_indexes = 0;
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
                    columns = calloc(new_size, sizeof(Column*));
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
        // check for an index
        if (strncmp(buf, "I", 1) == 0) {
            // check for index capacity
            if (index_count >= index_capacity) {
                size_t new_size = (index_capacity == 0) ? 1 : 2 * index_capacity;
                if (new_size == 1) {
                    indexes = malloc(sizeof(Index*));
                } else {
                    Index** new_indexes = realloc(indexes, sizeof(Index*) * new_size);
                    if (new_indexes == NULL)
                        return false;
                    indexes = new_indexes;
                }
                index_capacity = new_size;
            }
            // add new index object
            Index* new_index = malloc(sizeof(Index));
            new_index->type = (buf[2] == 'B') ? BTREE : SORTED;
            new_index->clustered = (buf[4] == 'C');
            char* col_name = buf + 6;
            for (size_t i = 0; i < col_count; i++) {
                if (strcmp(columns[i]->name, col_name) == 0) {
                    new_index->column = columns[i];
                }
            }
            new_index->object = malloc(sizeof(IndexObject));
            switch (new_index->type) {
                case BTREE:
                    if (new_index->clustered) {
                        new_index->object->btreec = createBTreeC();
                    } else {
                        new_index->object->btreeu = createBTreeU();
                    }
                    break;
                case SORTED:
                    if (!new_index->clustered) {
                        new_index->object->column = malloc(sizeof(ColumnIndex));
                    } else {
                        new_index->object->column = NULL;
                    }
                    break;
            }
            indexes[index_count++] = new_index;
            continue;
        }
        // read a line that we don't understand
        return false;
    }
    // store last column in table and tables in db
    tables[table_count - 1]->columns = columns;
    tables[table_count - 1]->col_count = col_count;
    tables[table_count - 1]->indexes = indexes;
    tables[table_count - 1]->num_indexes = index_count;
    current_db->tables = tables;
    current_db->num_tables = table_count;
    
    fclose(fp);

    log_info("-- Loaded db metadata.\n");
    return loadColumnData();
}

bool writeDb() {
    log_info("-- Writing database to file.\n");

    if (current_db == NULL)
        return true;
    // open file
    FILE* fp = fopen("./catalog", "w+");
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

        // indexes
        for (size_t j = 0; j < current_db->tables[i]->num_indexes; j++) {
            if (fprintf(fp, "I %c %c %s\n", 
                current_db->tables[i]->indexes[j]->type == BTREE ? 'B' : 'S',
                current_db->tables[i]->indexes[j]->clustered ? 'C' : 'U',
                current_db->tables[i]->indexes[j]->column->name) < 0)
                return false;
        }
    }
    // fclose(fp);
    return writeColumnData();
}
