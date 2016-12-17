#ifndef BTREE_H
#define BTREE_H

#define CAPACITY 10

#include "api/cs165.h"

typedef enum BTreeNodeType {
    PARENT,
    LEAF
}
typedef struct BTreeParent {
    BTreeNode* children[CAPACITY];
    int dividers[CAPACITY - 1];
    BTreeParent* parent;
    BTreeParent* next;
    size_t num_children;
} BTreeParent;
typedef struct BTreeLeaf {
    int values[CAPACITY];
    BTreeParent* parent;
    BTreeLeaf* next;
    size_t num_elements;
} BTreeLeaf;
typedef union BTreeObject {
    BTreeParent parent;
    BTreeLeaf leaf;
} BTreeObject;
typedef struct BTreeNode {
    BTreeNodeType type;
    BTreeObject object;
} BTreeNode;

BTreeNode* createBTree();
void insertValue(BTreeNode** tree, int value);
void deleteValue(BTreeNode** tree, int value);
void updateValue(BTreeNode** tree, int value, int new_value);

#endif
