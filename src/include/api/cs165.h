#ifndef CS165_H
#define CS165_H

#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include "api/btree.h"

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64
#define PAGE_SIZE sysconf(_SC_PAGESIZE)

// ================ DATABASE ================
typedef enum DataType {
     INT,
     LONG,
     FLOAT,
     DOUBLE
} DataType;
typedef struct Column {
    char name[MAX_SIZE_NAME + 1];
    int* data;
} Column;
typedef enum IndexType {
    BTREE,
    SORTED
} IndexType;
typedef struct ColumnIndex {
    int* values;
    int* indexes;
} ColumnIndex;
typedef union IndexObject {
    struct BTreeUNode* btreeu;
    struct BTreeCNode* btreec;
    struct ColumnIndex* column;
} IndexObject;
typedef struct Index {
    IndexObject* object;
    Column* column;
    IndexType type;
    bool clustered;
} Index;
typedef struct Table {
    char name [MAX_SIZE_NAME + 1];
    Column** columns;
    Index** indexes;
    size_t col_count;
    size_t num_rows;
    size_t capacity;
    size_t num_indexes;
} Table;
typedef struct Db {
    char name[MAX_SIZE_NAME + 1];
    Table** tables;
    size_t num_tables;
    size_t tables_capacity;
} Db;

// ================ CONTEXT/RESULTS ================
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void *payload;
} Result;

typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;

typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;
typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE + 1];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;

typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
    int client_fd;
} ClientContext;

// ================ STATUS ================
typedef enum StatusCode { OK, ERROR } StatusCode;
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;

// =============== EXECUTION ===============
typedef enum OperatorType { 
    OP_CREATE, 
    OP_INSERT,
    OP_LOADER,
    OP_SELECT,
    OP_PRINT,
    OP_FETCH,
    OP_MATH
} OperatorType;
typedef enum CreateType { CREATE_DB, CREATE_TBL, CREATE_COL, CREATE_IDX } CreateType;
typedef enum MathType { AVG, SUM, MAX, MIN, ADD, SUB } MathType;
typedef struct CreateOperator {
    CreateType type;
    char** params;
    size_t num_params;
} CreateOperator;
typedef struct InsertOperator {
    char* tbl_name;
    int* values;
    size_t num_values;
} InsertOperator;
typedef struct LoaderOperator {
    char* file_name;
} LoaderOperator;
typedef struct SelectOperator {
    char** params;
    char* handle;
    int minimum;
    int maximum;
    bool src_is_var;
} SelectOperator;
typedef struct FetchOperator {
    char* db_name;
    char* tbl_name;
    char* col_name;
    char* source;
    char* target;
} FetchOperator;
typedef struct PrintOperator {
    char** handles;
    size_t num_params;
} PrintOperator;
typedef struct MathOperator {
    MathType type;
    char* handle;
    char** params;
    size_t num_params;
    // marks whether the first argument is a variable or not
    bool is_var;
} MathOperator;
typedef union OperatorFields {
    CreateOperator create;
    InsertOperator insert;
    LoaderOperator loader;
    SelectOperator select;
    PrintOperator print;
    FetchOperator fetch;
    MathOperator math;
} OperatorFields;

typedef struct DbOperator {
    OperatorType type;
    OperatorFields fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

// =============== PUBLIC ===============
extern Db *current_db;

Status db_startup();
Status sync_db(Db* db);
Status add_db(const char* db_name, bool new);

Table* create_table(Db* db, const char* name, size_t num_columns, Status *status);
Column* create_column(char *name, Table *table, bool sorted, Status *ret_status);

Status shutdown_server();
Status shutdown_database(Db* db);

char** execute_db_operator(DbOperator* query);
void db_operator_free(DbOperator* query);

#endif /* CS165_H */
