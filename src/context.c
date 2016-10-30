// Abstraction layer between set of client contexts and rest of database

#include "api/context.h"

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
