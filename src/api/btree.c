#include <string.h>

#include "api/btree.h"
#include "util/log.h"

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

bool insertValueParent(BTreeParent* parent, int value);
bool insertValueLeaf(BTreeLeaf* leaf, int value);

// returns false if this node is completely full
bool insertValueParent(BTreeParent* parent, int value) {
    // find appropriate child node for this value
    int i;
    for (i = 0; i < (int) parent->num_children - 1; i++)
        if (parent->dividers[i] > value)
            break;
    
    // attempt to insert into child node
    switch (parent->children[i]->type) {
        case PARENT:
            if (insertValueParent(&(parent->children[i]->object.parent), value) == true)
                return true;
            else
                break;
        case LEAF:
            if (insertValueLeaf(&(parent->children[i]->object.leaf), value) == true)
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
        
        BTreeNode* node1;
        BTreeNode* node2;
        int new_divider;

        // need to handle parent and leaf cases separately
        switch(parent->children[i]->type) {
            case PARENT:
                // insert a new parent node after ith one
                parent->children[i+1] = malloc(sizeof(BTreeNode));
                parent->children[i+1]->type = PARENT;
                createBTreeParent(&(parent->children[i+1]->object.parent));

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
                return insertValueParent(parent, value);
                break;
            case LEAF:
                // insert a new leaf node after ith one
                parent->children[i+1] = malloc(sizeof(BTreeNode));
                parent->children[i+1]->type = LEAF;
                createBTreeLeaf(&(parent->children[i+1]->object.leaf));

                // copy objects to new node
                node1 = parent->children[i];
                node2 = parent->children[i+1];
                for (int j = CAPACITY; j < 2 * CAPACITY; j++)
                    node2->object.leaf.values[j - CAPACITY] = node1->object.leaf.values[j];
                
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
                return insertValueParent(parent, value);
                break;
        }
    } else {
        // this parent has the max number of children
        return false;
    }

    return true;
}
// returns false if this node is completely full
bool insertValueLeaf(BTreeLeaf* leaf, int value) {
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
    }
    leaf->values[i] = value;
    leaf->num_elements++;
    return true;
}

void insertValue(BTreeNode** tree, int value) {
    BTreeNode* root = *tree;
    switch ((*tree)->type) {
        case PARENT:
            // attempt to insert new value; if false, root is full
            if (insertValueParent(&(root->object.parent), value) == false) {
                // allocate a new root and a new parent
                BTreeNode* new_root = malloc(sizeof(BTreeNode));
                BTreeNode* new_parent = malloc(sizeof(BTreeNode));
                createBTreeParent(&(new_root->object.parent));
                createBTreeParent(&(new_parent->object.parent));
                BTreeNode* old_root = root;
                
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
                insertValue(tree, value);
            }
            break;
        case LEAF:
            // attempt to insert new value; if false, root is full
            if (insertValueLeaf(&(root->object.leaf), value) == false) {
                // allocate a new root and a new leaf
                BTreeNode* new_root = malloc(sizeof(BTreeNode));
                BTreeNode* new_leaf = malloc(sizeof(BTreeNode));
                new_leaf->type = LEAF;
                createBTreeParent(&(new_root->object.parent));
                createBTreeLeaf(&(new_leaf->object.leaf));
                BTreeNode* old_leaf = *tree;
                
                // insert new children to new root
                new_root->object.parent.children[0] = old_leaf;
                new_root->object.parent.children[1] = new_leaf;

                // copy values from old leaf to new leaf
                for (int j = CAPACITY; j < 2 * CAPACITY; j++) {
                    new_leaf->object.leaf.values[j - CAPACITY] = old_leaf->object.leaf.values[j];
                    old_leaf->object.leaf.values[j] = 0;
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
                insertValue(tree, value);
            }
            break;
    }
}

void deleteValue(BTreeNode** tree, int value);
void updateValue(BTreeNode** tree, int value, int new_value);

void printTreeHelper(BTreeNode* tree, char* prefix) {
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
                printTreeHelper(tree->object.parent.children[i], new_prefix);
            }
            break;
        }
        case LEAF: {
            log_info("%sLEAF   (%p) has %i values\n", prefix, tree, tree->object.leaf.num_elements);
            log_info("%s    Values: [ ", prefix);
            for (size_t i = 0; i < tree->object.leaf.num_elements; i++) {
                log_info("%i ", tree->object.leaf.values[i]);
            }
            log_info("]\n");
            break;
        }
    }
}
void printTree(BTreeNode* tree) {
    printTreeHelper(tree, "");
}

void traverse(BTreeNode* tree) {
    BTreeNode* ptr = tree;
    while (ptr->type != LEAF) {
        ptr = ptr->object.parent.children[0];
    }
    BTreeLeaf* leaf = &(ptr->object.leaf);
    while (leaf != NULL) {
        for (size_t i = 0; i < leaf->num_elements; i++) {
            printf("%i\n", leaf->values[i]);
        }
        leaf = leaf->next;
    }
}
