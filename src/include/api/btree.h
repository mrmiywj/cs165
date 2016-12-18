#ifndef BTREE_H
#define BTREE_H

#define CAPACITY 100

#include "api/cs165.h"

// nodes are either parents or leaves
typedef enum BTreeNodeType {
    PARENT,
    LEAF
} BTreeNodeType;

// node structs
typedef struct BTreeParent {
    struct BTreeNode* children[2 * CAPACITY];
    int dividers[2 * CAPACITY - 1];
    struct BTreeParent* parent;
    struct BTreeParent* next;
    size_t num_children;
} BTreeParent;
typedef struct BTreeLeaf {
    int values[2 * CAPACITY];
    int indexes[2 * CAPACITY];
    struct BTreeParent* parent;
    struct BTreeLeaf* next;
    size_t num_elements;
} BTreeLeaf;
typedef union BTreeObject {
    struct BTreeParent parent;
    struct BTreeLeaf leaf;
} BTreeObject;
typedef struct BTreeNode {
    BTreeNodeType type;
    BTreeObject object;
} BTreeNode;

BTreeNode* createBTree();
void insertValue(BTreeNode** tree, int value, int index);
void deleteValue(BTreeNode** tree, int value, int index);
void updateValue(BTreeNode** tree, int value, int index, int new_value);
void printTree(BTreeNode* tree);
void traverse(BTreeNode* tree);

#endif
