#ifndef BTREE_H
#define BTREE_H

#define CAPACITY 3

#include "api/cs165.h"

// nodes are either parents or leaves
typedef enum BTreeNodeType {
    PARENT,
    LEAF
} BTreeNodeType;

// unclustered btree structs
typedef struct BTreeUParent {
    struct BTreeUNode* children[2 * CAPACITY];
    int dividers[2 * CAPACITY - 1];
    struct BTreeUParent* parent;
    struct BTreeUParent* next;
    size_t num_children;
} BTreeUParent;
typedef struct BTreeULeaf {
    int values[2 * CAPACITY];
    int indexes[2 * CAPACITY];
    struct BTreeUParent* parent;
    struct BTreeULeaf* next;
    size_t num_elements;
} BTreeULeaf;
typedef union BTreeUObject {
    struct BTreeUParent parent;
    struct BTreeULeaf leaf;
} BTreeUObject;
typedef struct BTreeUNode {
    BTreeNodeType type;
    BTreeUObject object;
} BTreeUNode;

// unclustered btree functions
BTreeUNode* createBTreeU();
void insertValueU(BTreeUNode** tree, int value, int index);
void deleteValueU(BTreeUNode** tree, int value, int index);
void updateValueU(BTreeUNode** tree, int value, int index, int new_value);
void printTreeU(BTreeUNode* tree, char* prefix);
void traverseU(BTreeUNode* tree);
// returns the number of values found
int findRangeU(int** data, BTreeUNode* tree, int min, int max);

// clustered btree structs
typedef struct BTreeCParent {
    struct BTreeCNode* children[2 * CAPACITY];
    int dividers[2 * CAPACITY - 1];
    struct BTreeCParent* parent;
    struct BTreeCParent* next;
    size_t num_children;
} BTreeCParent;
typedef struct BTreeCLeaf {
    int values[2 * CAPACITY];
    int indexes[2 * CAPACITY];
    struct BTreeCParent* parent;
    struct BTreeCLeaf* next;
    size_t num_elements;
} BTreeCLeaf;
typedef union BTreeCObject {
    struct BTreeCParent parent;
    struct BTreeCLeaf leaf;
} BTreeCObject;
typedef struct BTreeCNode {
    BTreeNodeType type;
    BTreeCObject object;
} BTreeCNode;

// clustered btree functions
BTreeCNode* createBTreeC();
// returns new index of inserted element
size_t insertValueC(BTreeCNode** tree, int value);
// delete an element at a specific index
void deleteValueC(BTreeCNode** tree, int value, int index);
// updates an element at a specific index and returns the new index
size_t updateValueC(BTreeCNode** tree, int value, int index, int new_value);
void printTreeC(BTreeCNode* tree, char* prefix);
void traverseC(BTreeCNode* tree);
// returns the number of values found
int findRangeC(int** data, BTreeCNode* tree, int min, int max);

#endif
