// Abstraction layer between set of client contexts and rest of database

#include <string.h>
#include "api/context.h"

// should probably replace later with a binary tree of 
// some sort if we have lots of clients
typedef struct LinkedList {
    ClientContext* context;
    struct LinkedList* next;
} LinkedList;

LinkedList* contextList = NULL;

ClientContext* searchContext(int fd) {
    for (LinkedList* ptr = contextList; ptr != NULL; ptr = ptr->next)
        if (ptr->context->client_fd == fd)
            return ptr->context;
    return NULL;
}

GeneralizedColumnHandle* findHandle(ClientContext* context, char* handle) {
    for (int i = 0; i < context->chandles_in_use; i++)
        if (strcmp(context->chandle_table[i].name, handle) == 0)
            return &(context->chandle_table[i]);
    return NULL;
}

int findDuplicateHandle(ClientContext* context, char* handle) {
    for (int i = 0; i < context->chandles_in_use; i++) {
        if (strcmp(context->chandle_table[i].name, handle) == 0)
            destroyColumnHandle(context->chandle_table[i]);
            return i;
    }
    return -1;
}

void insertContext(ClientContext* context) {
    LinkedList* new_node = malloc(sizeof(LinkedList));
    new_node->context = context;
    new_node->next = contextList;
    contextList = new_node;
}

void deleteContext(ClientContext* context) {
    LinkedList** ptr = &contextList;
    while ((*ptr)->context != context)
        ptr = &(*ptr)->next;
    *ptr = (*ptr)->next;
}

bool checkContextSize(ClientContext* context) {
    if (context->chandles_in_use == context->chandle_slots) {
        int new_size = (context->chandle_slots == 0) ? 1 : 2 * context->chandle_slots;
        GeneralizedColumnHandle* new_table = realloc(context->chandle_table, new_size * sizeof(GeneralizedColumnHandle));
        if (new_table == NULL)
            return false;
        context->chandle_table = new_table;
        context->chandle_slots = new_size;
    }
    return true;
}

void destroyColumnHandle(GeneralizedColumnHandle handle) {
    GeneralizedColumn column = handle.generalized_column;
    switch (column.column_type) {
        case RESULT: {
            Result* result = column.column_pointer.result;
            free(result->payload);
        }
        case COLUMN: {
            Column* col = column.column_pointer.column;
            free(col->data);
        }
        default:
            break;
    }
}