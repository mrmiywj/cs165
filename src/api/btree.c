#include "btree.h"

void createBTreeParent(BTreeParent* parent) {
    *parent= (BTreeParent) {
        .children = {NULL},
        .dividers = {0},
        .parent = NULL,
        .next = NULL,
        .num_children = 0
    };
}
void createBTreeLeaf(BTreeLeaf* leaf) {
    *leaf = (BTreeLeaf) {
        .values = {0},
        .parent = NULL,
        .next = NULL,
        .num_elements = 0
    };
}
BTreeNode* createBTree() {
    BTreeNode* new_node = malloc(sizeof(BTreeNode));
    new_node->type = LEAF;
    createBTreeLeaf(&new_node->object.leaf);
    return new_node;
}

void insertValueParent(BTreeParent* parent, int value) {
    // parent node has all children
    if (parent->num_children == CAPACITY) {
        BTreeNode new_parent = malloc(sizeof(BTreeNode));
        new_parent->type = PARENT;
        createBTreeParent(&(new_parent->object.parent));
        
    }
}
void insertValueLeaf(BTreeLeaf* leaf, int value) {

}
void insertValue(BTreeNode* tree, int value) {
    switch (tree->type) {
        case PARENT:
            return insertValueParent(&(tree->object.parent)), value);
        case LEAF:
            return insertValueLeaf(&(tree->object.leaf)), value);
        default:
            break;
    }
}
void deleteValue(BTreeNode** tree, int value);
void updateValue(BTreeNode** tree, int value, int new_value);