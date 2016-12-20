#include <string.h>

#include "api/btree.h"
#include "util/log.h"

/*
==========================================
===== UNCLUSTERED B+ TREE FUNCTIONS ======
==========================================
*/

void createBTreeUParent(BTreeUParent* parent) {
    *parent= (BTreeUParent) {
        .children = {NULL},
        .dividers = {0},
        .parent = NULL,
        .next = NULL,
        .num_children = 0
    };
}
void createBTreeULeaf(BTreeULeaf* leaf) {
    *leaf = (BTreeULeaf) {
        .values = {0},
        .indexes = {0},
        .parent = NULL,
        .next = NULL,
        .num_elements = 0
    };
}
BTreeUNode* createBTreeU() {
    BTreeUNode* new_node = malloc(sizeof(BTreeUNode));
    new_node->type = LEAF;
    createBTreeULeaf(&new_node->object.leaf);
    return new_node;
}

bool insertValueParentU(BTreeUParent* parent, int value, int index);
bool insertValueLeafU(BTreeULeaf* leaf, int value, int index);

// returns false if this node is completely full
bool insertValueParentU(BTreeUParent* parent, int value, int index) {
    // find appropriate child node for this value
    int i;
    for (i = 0; i < (int) parent->num_children - 1; i++)
        if (parent->dividers[i] > value)
            break;
    
    // attempt to insert into child node
    switch (parent->children[i]->type) {
        case PARENT:
            if (insertValueParentU(&(parent->children[i]->object.parent), value, index) == true)
                return true;
            else
                break;
        case LEAF:
            if (insertValueLeafU(&(parent->children[i]->object.leaf), value, index) == true)
                return true;
            else
                break;
    }

    // fall-through; subchild is completely full, need to break into two children
    if (parent->num_children < 2 * CAPACITY) {
        // there's space to add another child, insert at i+1
        for (int j = parent->num_children - 1; j > i; j--) {
            parent->children[j+1] = parent->children[j];
            parent->dividers[j] = parent->dividers[j-1];
        }
        
        BTreeUNode* node1;
        BTreeUNode* node2;
        int new_divider;

        // need to handle parent and leaf cases separately
        switch(parent->children[i]->type) {
            case PARENT:
                // insert a new parent node after ith one
                parent->children[i+1] = malloc(sizeof(BTreeUNode));
                parent->children[i+1]->type = PARENT;
                createBTreeUParent(&(parent->children[i+1]->object.parent));

                // copy objects to new node
                node1 = parent->children[i];
                node2 = parent->children[i+1];
                // copy values from old root to new parent
                for (int j = CAPACITY; j < 2 * CAPACITY; j++) {
                    node2->object.parent.children[j - CAPACITY] = node1->object.parent.children[j];
                    node1->object.parent.children[j] = NULL;
                }
                // copy dividers from old root to new parent
                for (int j = CAPACITY; j < 2 * CAPACITY - 1; j++) {
                    node2->object.parent.dividers[j - CAPACITY] = node1->object.parent.dividers[j];
                    node1->object.parent.dividers[j] = 0;
                }
                node1->object.parent.dividers[CAPACITY - 1] = 0;
                // fix number of children
                node1->object.parent.num_children = CAPACITY;
                node2->object.parent.num_children = CAPACITY;
                // fix parents
                node1->object.parent.parent = parent;
                node2->object.parent.parent = parent;
                // fix next pointers
                node2->object.parent.next = node1->object.parent.next;
                node1->object.parent.next = &(node2->object.parent);
                
                // bubble up new divider to parent
                new_divider = node2->object.parent.dividers[0];
                for (int j = parent->num_children - 2; j >= i; j--) {
                    parent->dividers[i+1] = parent->dividers[i];
                }
                parent->dividers[i] = new_divider;
                parent->num_children++;

                // retry inserting value in this parent
                return insertValueParentU(parent, value, index);
                break;
            case LEAF:
                // insert a new leaf node after ith one
                parent->children[i+1] = malloc(sizeof(BTreeUNode));
                parent->children[i+1]->type = LEAF;
                createBTreeULeaf(&(parent->children[i+1]->object.leaf));

                // copy objects to new node
                node1 = parent->children[i];
                node2 = parent->children[i+1];
                for (int j = CAPACITY; j < 2 * CAPACITY; j++) {
                    node2->object.leaf.values[j - CAPACITY] = node1->object.leaf.values[j];
                    node2->object.leaf.indexes[j - CAPACITY] = node1->object.leaf.indexes[j];
                    node1->object.leaf.values[j] = 0;
                    node1->object.leaf.indexes[j] = 0;
                }
                
                // fix number of values
                node1->object.leaf.num_elements = CAPACITY;
                node2->object.leaf.num_elements = CAPACITY;
                // fix parents
                node1->object.leaf.parent = parent;
                node2->object.leaf.parent = parent;
                // fix next pointers
                node2->object.leaf.next = node1->object.leaf.next;
                node1->object.leaf.next = &(node2->object.leaf);
                
                // bubble up new divider to parent
                new_divider = node2->object.leaf.values[0];
                for (int j = parent->num_children - 2; j >= i; j--) {
                    parent->dividers[i+1] = parent->dividers[i];
                }
                parent->dividers[i] = new_divider;
                parent->num_children++;

                // retry inserting value in this parent
                return insertValueParentU(parent, value, index);
                break;
        }
    } else {
        // this parent has the max number of children
        return false;
    }

    return true;
}
// returns false if this node is completely full
bool insertValueLeafU(BTreeULeaf* leaf, int value, int index) {
    // check for full leaf node
    if (leaf->num_elements == 2 * CAPACITY)
        return false;
    
    // else, insert new value
    int i;
    for (i = 0; i < (int) leaf->num_elements; i++)
        if (leaf->values[i] >= value)
            break;
    
    // shift values over
    for (int j = leaf->num_elements - 1; j >= i; j--) {
        leaf->values[j+1] = leaf->values[j];
        leaf->indexes[j+1] = leaf->indexes[j];
    }
    leaf->values[i] = value;
    leaf->indexes[i] = index;
    leaf->num_elements++;

    return true;
}

void insertValueU(BTreeUNode** tree, int value, int index) {
    BTreeUNode* root = *tree;
    switch ((*tree)->type) {
        case PARENT:
            // attempt to insert new value; if false, root is full
            if (insertValueParentU(&(root->object.parent), value, index) == false) {
                // allocate a new root and a new parent
                BTreeUNode* new_root = malloc(sizeof(BTreeUNode));
                BTreeUNode* new_parent = malloc(sizeof(BTreeUNode));
                createBTreeUParent(&(new_root->object.parent));
                createBTreeUParent(&(new_parent->object.parent));
                BTreeUNode* old_root = root;
                
                // insert new children to new root
                new_root->object.parent.children[0] = old_root;
                new_root->object.parent.children[1] = new_parent;

                // copy values from old root to new parent
                for (int j = CAPACITY; j < 2 * CAPACITY; j++) {
                    new_parent->object.parent.children[j - CAPACITY] = old_root->object.parent.children[j];
                    old_root->object.parent.children[j] = NULL;
                }
                // copy dividers from old root to new parent
                for (int j = CAPACITY; j < 2 * CAPACITY - 1; j++) {
                    new_parent->object.parent.dividers[j - CAPACITY] = old_root->object.parent.dividers[j];
                    old_root->object.parent.dividers[j] = 0;
                }
                old_root->object.parent.dividers[CAPACITY - 1] = 0;
                // fix number of children
                new_root->object.parent.num_children = 2;
                new_parent->object.parent.num_children = CAPACITY;
                old_root->object.parent.num_children = CAPACITY;
                // fix parents
                new_parent->object.parent.parent = &(new_root->object.parent);
                old_root->object.parent.parent = &(new_root->object.parent);
                // fix next pointers
                old_root->object.parent.next = &(new_parent->object.parent);
                // bubble up divider to new root
                new_root->object.parent.dividers[0] = new_parent->object.parent.dividers[0];

                // save new root node and re-insert
                *tree = new_root;
                insertValueU(tree, value, index);
            }
            break;
        case LEAF:
            // attempt to insert new value; if false, root is full
            if (insertValueLeafU(&(root->object.leaf), value, index) == false) {
                // allocate a new root and a new leaf
                BTreeUNode* new_root = malloc(sizeof(BTreeUNode));
                BTreeUNode* new_leaf = malloc(sizeof(BTreeUNode));
                new_leaf->type = LEAF;
                createBTreeUParent(&(new_root->object.parent));
                createBTreeULeaf(&(new_leaf->object.leaf));
                BTreeUNode* old_leaf = *tree;
                
                // insert new children to new root
                new_root->object.parent.children[0] = old_leaf;
                new_root->object.parent.children[1] = new_leaf;

                // copy values from old leaf to new leaf
                for (int j = CAPACITY; j < 2 * CAPACITY; j++) {
                    new_leaf->object.leaf.values[j - CAPACITY] = old_leaf->object.leaf.values[j];
                    new_leaf->object.leaf.indexes[j - CAPACITY] = old_leaf->object.leaf.indexes[j];
                    old_leaf->object.leaf.values[j] = 0;
                    old_leaf->object.leaf.indexes[j] = 0;
                }
                // fix number of children
                new_root->object.parent.num_children = 2;
                new_leaf->object.leaf.num_elements = CAPACITY;
                old_leaf->object.leaf.num_elements = CAPACITY;
                // fix parents
                new_leaf->object.leaf.parent = &(new_root->object.parent);
                old_leaf->object.leaf.parent = &(new_root->object.parent);
                // fix next pointers
                old_leaf->object.leaf.next = &(new_leaf->object.leaf);
                // bubble up divider to new root
                new_root->object.parent.dividers[0] = new_leaf->object.leaf.values[0];

                // save new root node and re-insert
                *tree = new_root;
                insertValueU(tree, value, index);
            }
            break;
    }
    // successfully inserted value and index; need to shift all other indexes forward by 1
    BTreeUNode* ptr = *tree;
    while (ptr->type != LEAF) {
        ptr = ptr->object.parent.children[0];
    }
    BTreeULeaf* leaf = &(ptr->object.leaf);
    while (leaf != NULL) {
        for (size_t i = 0; i < leaf->num_elements; i++)
            leaf->indexes[i] += (leaf->indexes[i] >= index && leaf->values[i] != value);
        leaf = leaf->next;
    }
}

void deleteValueU(BTreeUNode** tree, int value, int index) {
    (void) tree;
    (void) value;
    (void) index;
}
void updateValueU(BTreeUNode** tree, int value, int index, int new_value) {
    deleteValueU(tree, value, index);
    insertValueU(tree, new_value, index);
}

void printTreeU(BTreeUNode* tree, char* prefix) {
    switch (tree->type) {
        case PARENT: {
            log_info("%sPARENT (%p) has %i children\n", prefix, tree, tree->object.parent.num_children);
            log_info("%s    Delimiters: [ ", prefix);
            for (size_t i = 0; i < tree->object.parent.num_children - 1; i++) {
                log_info("%i ", tree->object.parent.dividers[i]);
            }
            log_info("]\n");
            char new_prefix[strlen(prefix) + 5];
            for (size_t i = 0; i < strlen(prefix) + 4; i++) {
                new_prefix[i] = ' ';
            }
            new_prefix[strlen(prefix) + 4] = '\0';
            for (size_t i = 0; i < tree->object.parent.num_children; i++) {
                printTreeU(tree->object.parent.children[i], new_prefix);
            }
            break;
        }
        case LEAF: {
            log_info("%sLEAF   (%p) has %i values\n", prefix, tree, tree->object.leaf.num_elements);
            log_info("%s    Values: [ ", prefix);
            for (size_t i = 0; i < tree->object.leaf.num_elements; i++) {
                log_info("(%i, %i) ", tree->object.leaf.values[i], tree->object.leaf.indexes[i]);
            }
            log_info("]\n");
            break;
        }
    }
}

void traverseU(BTreeUNode* tree) {
    BTreeUNode* ptr = tree;
    while (ptr->type != LEAF) {
        ptr = ptr->object.parent.children[0];
    }
    BTreeULeaf* leaf = &(ptr->object.leaf);
    while (leaf != NULL) {
        for (size_t i = 0; i < leaf->num_elements; i++) {
            printf("(%i, %i)\n", leaf->values[i], leaf->indexes[i]);
        }
        leaf = leaf->next;
    }
}

// returns the number of values found
int findRangeU(int** data, BTreeUNode* tree, int min, int max) {
    // find closest leaf
    BTreeUNode* ptr = tree;
    while (ptr->type != LEAF) {
        size_t i;
        for (i = 0; i < ptr->object.parent.num_children - 1; i++)
            if (ptr->object.parent.dividers[i] > min)
                break;
        ptr = ptr->object.parent.children[i];
    }

    // iterate horizontally until we retrieve all values
    int* results = NULL;
    int num_tuples = 0;
    int capacity = 0;
    BTreeULeaf* leaf = &(ptr->object.leaf);
    while (leaf != NULL) {
        // iterate over all values in this leaf
        for (size_t i = 0; i < leaf->num_elements; i++) {
            // look for values >= min only
            if (leaf->values[i] < min)
                continue;
            // stop iterating if we find a value >= max
            if (leaf->values[i] >= max) {
                leaf = NULL;
                break;
            }
            // check if we need to resize
            if (capacity == num_tuples) {
                size_t new_size = (capacity == 0) ? 1 : 2 * capacity;
                if (new_size == 1) {
                    results = malloc(sizeof(int));
                } else {
                    int* new_data = realloc(results, sizeof(int) * new_size);
                    if (new_data != NULL) {
                        results = new_data;
                    } else {
                        return 0;
                    }
                }
                capacity = new_size;
            }
            // save new value
            results[num_tuples++] = leaf->indexes[i];
        }
        // move to next leaf
        if (leaf != NULL)
            leaf = leaf->next;
    }
    *data = results;
    return num_tuples;
}

/*
==========================================
====== CLUSTERED B+ TREE FUNCTIONS =======
==========================================
*/

// shifts all indexes in the tree after a position by a given amount
void shift(BTreeCLeaf* leaf, int index, int increment) {
    BTreeCLeaf* ptr = leaf;
    for (size_t i = index; i < ptr->num_elements; i++) {
        ptr->indexes[i] += increment;
    }
    ptr = ptr->next;
    while (ptr != NULL) {
        for (size_t i = 0; i < ptr->num_elements; i++) {
            ptr->indexes[i] += increment;
        }
        ptr = ptr->next;
    }
}

void createBTreeCParent(BTreeCParent* parent) {
    *parent= (BTreeCParent) {
        .children = {NULL},
        .dividers = {0},
        .parent = NULL,
        .next = NULL,
        .num_children = 0
    };
}
void createBTreeCLeaf(BTreeCLeaf* leaf) {
    *leaf = (BTreeCLeaf) {
        .values = {0},
        .indexes = {0},
        .parent = NULL,
        .next = NULL,
        .num_elements = 0
    };
}
BTreeCNode* createBTreeC() {
    BTreeCNode* new_node = malloc(sizeof(BTreeCNode));
    new_node->type = LEAF;
    createBTreeCLeaf(&new_node->object.leaf);
    return new_node;
}

int insertValueParentC(BTreeCParent* parent, int value);
int insertValueLeafC(BTreeCLeaf* leaf, int value);

// returns -1 if this node is completely full
int insertValueParentC(BTreeCParent* parent, int value) {
    // find appropriate child node for this value
    int i;
    for (i = 0; i < (int) parent->num_children - 1; i++)
        if (parent->dividers[i] > value)
            break;
    
    // attempt to insert into child node
    switch (parent->children[i]->type) {
        case PARENT: {
            int result = insertValueParentC(&(parent->children[i]->object.parent), value);
            if (result >= 0)
                return result;
            else
                break;
        }
        case LEAF: {
            int result = insertValueLeafC(&(parent->children[i]->object.leaf), value);
            if (result >= 0)
                return result;
            else
                break;
        }
    }

    // fall-through; subchild is completely full, need to break into two children
    if (parent->num_children < 2 * CAPACITY) {
        // there's space to add another child, insert at i+1
        for (int j = parent->num_children - 1; j > i; j--) {
            parent->children[j+1] = parent->children[j];
            parent->dividers[j] = parent->dividers[j-1];
        }
        
        BTreeCNode* node1;
        BTreeCNode* node2;
        int new_divider;

        // need to handle parent and leaf cases separately
        switch(parent->children[i]->type) {
            case PARENT:
                // insert a new parent node after ith one
                parent->children[i+1] = malloc(sizeof(BTreeCNode));
                parent->children[i+1]->type = PARENT;
                createBTreeCParent(&(parent->children[i+1]->object.parent));

                // copy objects to new node
                node1 = parent->children[i];
                node2 = parent->children[i+1];
                // copy values from old root to new parent
                for (int j = CAPACITY; j < 2 * CAPACITY; j++) {
                    node2->object.parent.children[j - CAPACITY] = node1->object.parent.children[j];
                    node1->object.parent.children[j] = NULL;
                }
                // copy dividers from old root to new parent
                for (int j = CAPACITY; j < 2 * CAPACITY - 1; j++) {
                    node2->object.parent.dividers[j - CAPACITY] = node1->object.parent.dividers[j];
                    node1->object.parent.dividers[j] = 0;
                }
                node1->object.parent.dividers[CAPACITY - 1] = 0;
                // fix number of children
                node1->object.parent.num_children = CAPACITY;
                node2->object.parent.num_children = CAPACITY;
                // fix parents
                node1->object.parent.parent = parent;
                node2->object.parent.parent = parent;
                // fix next pointers
                node2->object.parent.next = node1->object.parent.next;
                node1->object.parent.next = &(node2->object.parent);
                
                // bubble up new divider to parent
                new_divider = node2->object.parent.dividers[0];
                for (int j = parent->num_children - 2; j >= i; j--) {
                    parent->dividers[i+1] = parent->dividers[i];
                }
                parent->dividers[i] = new_divider;
                parent->num_children++;

                // retry inserting value in this parent
                return insertValueParentC(parent, value);
                break;
            case LEAF:
                // insert a new leaf node after ith one
                parent->children[i+1] = malloc(sizeof(BTreeCNode));
                parent->children[i+1]->type = LEAF;
                createBTreeCLeaf(&(parent->children[i+1]->object.leaf));

                // copy objects to new node
                node1 = parent->children[i];
                node2 = parent->children[i+1];
                for (int j = CAPACITY; j < 2 * CAPACITY; j++) {
                    node2->object.leaf.values[j - CAPACITY] = node1->object.leaf.values[j];
                    node2->object.leaf.indexes[j - CAPACITY] = node1->object.leaf.indexes[j];
                    node1->object.leaf.values[j] = 0;
                    node1->object.leaf.indexes[j] = 0;
                }
                
                // fix number of values
                node1->object.leaf.num_elements = CAPACITY;
                node2->object.leaf.num_elements = CAPACITY;
                // fix parents
                node1->object.leaf.parent = parent;
                node2->object.leaf.parent = parent;
                // fix next pointers
                node2->object.leaf.next = node1->object.leaf.next;
                node1->object.leaf.next = &(node2->object.leaf);
                
                // bubble up new divider to parent
                new_divider = node2->object.leaf.values[0];
                for (int j = parent->num_children - 2; j >= i; j--) {
                    parent->dividers[i+1] = parent->dividers[i];
                }
                parent->dividers[i] = new_divider;
                parent->num_children++;

                // retry inserting value in this parent
                return insertValueParentC(parent, value);
                break;
        }
    } else {
        // this parent has the max number of children
        return -1;
    }
    // should never reach this point
    log_info("CRITICAL ERROR: REACHED IMPOSSIBLE LINE AT %i in %i\n", __LINE__, __func__);
    return -1;
}
// returns int if this node is completely full
int insertValueLeafC(BTreeCLeaf* leaf, int value) {
    // check for full leaf node
    if (leaf->num_elements == 2 * CAPACITY)
        return -1;
    
    // else, insert new value
    int i;
    for (i = 0; i < (int) leaf->num_elements; i++)
        if (leaf->values[i] >= value)
            break;
    
    // shift values over
    for (int j = leaf->num_elements - 1; j >= i; j--) {
        leaf->values[j+1] = leaf->values[j];
        leaf->indexes[j+1] = leaf->indexes[j];
    }
    leaf->values[i] = value;
    leaf->indexes[i] = i > 0 ? leaf->indexes[i - 1] + 1 : leaf->indexes[i + 1];
    leaf->num_elements++;
    shift(leaf, i + 1, 1);
    return leaf->indexes[i];
}

size_t insertValueC(BTreeCNode** tree, int value) {
    BTreeCNode* root = *tree;
    switch ((*tree)->type) {
        case PARENT: {
            // attempt to insert new value; if false, root is full
            int result = insertValueParentC(&(root->object.parent), value);
            if (result < 0) {
                // allocate a new root and a new parent
                BTreeCNode* new_root = malloc(sizeof(BTreeCNode));
                BTreeCNode* new_parent = malloc(sizeof(BTreeCNode));
                createBTreeCParent(&(new_root->object.parent));
                createBTreeCParent(&(new_parent->object.parent));
                BTreeCNode* old_root = root;
                
                // insert new children to new root
                new_root->object.parent.children[0] = old_root;
                new_root->object.parent.children[1] = new_parent;

                // copy values from old root to new parent
                for (int j = CAPACITY; j < 2 * CAPACITY; j++) {
                    new_parent->object.parent.children[j - CAPACITY] = old_root->object.parent.children[j];
                    old_root->object.parent.children[j] = NULL;
                }
                // copy dividers from old root to new parent
                for (int j = CAPACITY; j < 2 * CAPACITY - 1; j++) {
                    new_parent->object.parent.dividers[j - CAPACITY] = old_root->object.parent.dividers[j];
                    old_root->object.parent.dividers[j] = 0;
                }
                old_root->object.parent.dividers[CAPACITY - 1] = 0;
                // fix number of children
                new_root->object.parent.num_children = 2;
                new_parent->object.parent.num_children = CAPACITY;
                old_root->object.parent.num_children = CAPACITY;
                // fix parents
                new_parent->object.parent.parent = &(new_root->object.parent);
                old_root->object.parent.parent = &(new_root->object.parent);
                // fix next pointers
                old_root->object.parent.next = &(new_parent->object.parent);
                // bubble up divider to new root
                new_root->object.parent.dividers[0] = new_parent->object.parent.dividers[0];

                // save new root node and re-insert
                *tree = new_root;
                return (size_t) insertValueC(tree, value);
            } else {
                return (size_t) result;
            }
        }
        case LEAF: {
            // attempt to insert new value; if false, root is full
            int result = insertValueLeafC(&(root->object.leaf), value);
            if (result < 0) {
                // allocate a new root and a new leaf
                BTreeCNode* new_root = malloc(sizeof(BTreeCNode));
                BTreeCNode* new_leaf = malloc(sizeof(BTreeCNode));
                new_leaf->type = LEAF;
                createBTreeCParent(&(new_root->object.parent));
                createBTreeCLeaf(&(new_leaf->object.leaf));
                BTreeCNode* old_leaf = *tree;
                
                // insert new children to new root
                new_root->object.parent.children[0] = old_leaf;
                new_root->object.parent.children[1] = new_leaf;

                // copy values from old leaf to new leaf
                for (int j = CAPACITY; j < 2 * CAPACITY; j++) {
                    new_leaf->object.leaf.values[j - CAPACITY] = old_leaf->object.leaf.values[j];
                    new_leaf->object.leaf.indexes[j - CAPACITY] = old_leaf->object.leaf.indexes[j];
                    old_leaf->object.leaf.values[j] = 0;
                    old_leaf->object.leaf.indexes[j] = 0;
                }
                // fix number of children
                new_root->object.parent.num_children = 2;
                new_leaf->object.leaf.num_elements = CAPACITY;
                old_leaf->object.leaf.num_elements = CAPACITY;
                // fix parents
                new_leaf->object.leaf.parent = &(new_root->object.parent);
                old_leaf->object.leaf.parent = &(new_root->object.parent);
                // fix next pointers
                old_leaf->object.leaf.next = &(new_leaf->object.leaf);
                // bubble up divider to new root
                new_root->object.parent.dividers[0] = new_leaf->object.leaf.values[0];

                // save new root node and re-insert
                *tree = new_root;
                return (size_t) insertValueC(tree, value);
            } else {
                return (size_t) result;
            }
        }
    }
    // should never reach this point
    log_info("CRITICAL ERROR: REACHED IMPOSSIBLE LINE AT %i in %i\n", __LINE__, __func__);
    return 0;
}

void deleteValueC(BTreeCNode** tree, int value, int index) {
    (void) tree;
    (void) value;
    (void) index;
}
size_t updateValueC(BTreeCNode** tree, int value, int index, int new_value) {
    deleteValueC(tree, value, index);
    return insertValueC(tree, new_value);
}

void printTreeC(BTreeCNode* tree, char* prefix) {
    switch (tree->type) {
        case PARENT: {
            log_info("%sPARENT (%p) has %i children\n", prefix, tree, tree->object.parent.num_children);
            log_info("%s    Delimiters: [ ", prefix);
            for (size_t i = 0; i < tree->object.parent.num_children - 1; i++) {
                log_info("%i ", tree->object.parent.dividers[i]);
            }
            log_info("]\n");
            char new_prefix[strlen(prefix) + 5];
            for (size_t i = 0; i < strlen(prefix) + 4; i++) {
                new_prefix[i] = ' ';
            }
            new_prefix[strlen(prefix) + 4] = '\0';
            for (size_t i = 0; i < tree->object.parent.num_children; i++) {
                printTreeC(tree->object.parent.children[i], new_prefix);
            }
            break;
        }
        case LEAF: {
            log_info("%sLEAF   (%p) has %i values\n", prefix, tree, tree->object.leaf.num_elements);
            log_info("%s    Values: [ ", prefix);
            for (size_t i = 0; i < tree->object.leaf.num_elements; i++) {
                log_info("(%i, %i) ", tree->object.leaf.values[i], tree->object.leaf.indexes[i]);
            }
            log_info("]\n");
            break;
        }
    }
}

void traverseC(BTreeCNode* tree) {
    BTreeCNode* ptr = tree;
    while (ptr->type != LEAF) {
        ptr = ptr->object.parent.children[0];
    }
    BTreeCLeaf* leaf = &(ptr->object.leaf);
    while (leaf != NULL) {
        for (size_t i = 0; i < leaf->num_elements; i++) {
            printf("(%i, %i)\n", leaf->values[i], leaf->indexes[i]);
        }
        leaf = leaf->next;
    }
}

// returns the number of values found
int findRangeC(int** data, BTreeCNode* tree, int min, int max) {
    // find closest leaf
    BTreeCNode* ptr = tree;
    while (ptr->type != LEAF) {
        size_t i;
        for (i = 0; i < ptr->object.parent.num_children - 1; i++)
            if (ptr->object.parent.dividers[i] > min)
                break;
        ptr = ptr->object.parent.children[i];
    }

    // iterate horizontally until we retrieve all values
    int* results = NULL;
    int num_tuples = 0;
    int capacity = 0;
    BTreeCLeaf* leaf = &(ptr->object.leaf);
    while (leaf != NULL) {
        // iterate over all values in this leaf
        for (size_t i = 0; i < leaf->num_elements; i++) {
            // look for values >= min only
            if (leaf->values[i] < min)
                continue;
            // stop iterating if we find a value >= max
            if (leaf->values[i] >= max) {
                leaf = NULL;
                break;
            }
            // check if we need to resize
            if (capacity == num_tuples) {
                size_t new_size = (capacity == 0) ? 1 : 2 * capacity;
                if (new_size == 1) {
                    results = malloc(sizeof(int));
                } else {
                    int* new_data = realloc(results, sizeof(int) * new_size);
                    if (new_data != NULL) {
                        results = new_data;
                    } else {
                        return 0;
                    }
                }
                capacity = new_size;
            }
            // save new value
            results[num_tuples++] = leaf->indexes[i];
        }
        // move to next leaf
        if (leaf != NULL)
            leaf = leaf->next;
    }
    *data = results;
    return num_tuples;
}
