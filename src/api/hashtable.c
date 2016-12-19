#include "api/hashtable.h"
#include "util/log.h"

size_t hash(HashTable* ht, int key) {
    return (key + ht->tableSize) % ht->tableSize;
}

int destroyNode(HashNode* n, int count) {
    if (n != NULL) {
        HashNode* next = n->next;
        free(n->keys);
        free(n->values);
        free(n);
        return destroyNode(next, count + n->count);
    }
    return 0;
}

void resize(HashTable* ht) {
    HashNode** oldBuckets = ht->buckets;
    HashNode** newBuckets = malloc(ht->tableSize * 2 * sizeof(HashNode*));
    ht->count = 0;
    ht->buckets = newBuckets;
    ht->tableSize *= 2;

    for (int i = 0; i < ht->tableSize / 2; i++) {
        HashNode* bucket = oldBuckets[i];
        while (bucket != NULL) {
            for (int j = 0; j < bucket->count; j++) {
                put(ht, bucket->keys[j], bucket->values[j]);
            }
            bucket = bucket->next;
        }

        destroyNode(oldBuckets[i], 0);
    }

    free(oldBuckets);
}

void printHashTable(HashTable* ht, char* prefix) {
    log_info("%sHashtable at %p:\n", prefix, ht);
    log_info("%s    # buckets:  %8i\n", prefix, ht->tableSize);
    log_info("%s    # elements: %8i\n", prefix, ht->count);
}

// initialize the components of the hashtable
void init(HashTable** ht) {
    HashTable* newTable = malloc(sizeof(HashTable));
    newTable->buckets = malloc(D_TABLE_SIZE * sizeof(struct HashNode*));
    newTable->count = 0;
    newTable->nodeSize = D_NODE_SIZE;
    newTable->tableSize = D_TABLE_SIZE;

    for (int i = 0; i < newTable->tableSize; i++)
        newTable->buckets[i] = NULL;

    *ht = newTable;
}

// insert a key-value pair into the hash table
void put(HashTable* ht, int key, int value) {
    if (ht->count > ht->tableSize * MAX_Q)
        resize(ht);
    size_t index = hash(ht, key);
    HashNode* bucket = ht->buckets[index];
    ht->count++;

    if (bucket != NULL && bucket->count < ht->nodeSize) {
        bucket->keys[bucket->count] = key;
        bucket->values[bucket->count++] = value;
    } else {
        HashNode* newNode = malloc(sizeof(HashNode));
        newNode->count = 1;
        
        newNode->keys = malloc(ht->nodeSize * sizeof(int));
        newNode->keys[0] = key;
        
        newNode->values = malloc(ht->nodeSize * sizeof(int));
        newNode->values[0] = value;
        
        newNode->next = bucket;
        ht->buckets[index] = newNode;
    }
}

// get entries with a matching key and stores the
// corresponding values in the values array.
int get(HashTable* ht, int key, int *values, int num_values) {
    int count = 0;
    int index = hash(ht, key);
    HashNode* bucket = ht->buckets[index];

    while (bucket != NULL) {
        for (int i = 0; i < bucket->count; i++) {
            if (count < num_values) {
                values[count] = bucket->values[i];
            }
            count++;
        }
        bucket = bucket->next;
    }

    return count;
}

// erase a key-value pair from the hash talbe
void erase(HashTable* ht, int key) {
    int index = hash(ht, key);
    HashNode* bucket = ht->buckets[index];
    ht->count -= destroyNode(bucket, 0);
    ht->buckets[index] = NULL;
}
