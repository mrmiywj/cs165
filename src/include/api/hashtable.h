#include "stdlib.h"
#include "stdio.h"

#ifndef HASH_TABLE_GUARD
#define HASH_TABLE_GUARD
#define D_TABLE_SIZE 1000000
#define D_NODE_SIZE 20
#define MAX_Q 5

typedef struct HashNode {
    int count;
    int* keys;
    int* values;
    struct HashNode* next;
} HashNode;

typedef struct HashTable {
    int count;
    int tableSize;
    int nodeSize;
    struct HashNode** buckets;
} HashTable;

void init(HashTable** ht);
void put(HashTable* ht, int key, int value);
int get(HashTable* ht, int key, int *values, int num_values);
void erase(HashTable* ht, int key);

#endif
