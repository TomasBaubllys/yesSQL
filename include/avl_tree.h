#ifndef YSQL_AVL_TREE_INCLUDED
#define YSQL_AVL_TREE_INCLUDED

#include <iostream>
#include "entry.h"
#include <algorithm>
#include <vector>

using namespace std;

#define AVL_TREE_INSERTION_FAILED_ERR "Failed to insert given entry to the tree\n"
#define AVL_TREE_DELETION_FAILED_ERR "Failed to delete given entry from the tree\n"

class AVL_tree
{
    struct Node
    {
        Entry data;
        Node* left;
        Node* right;
        int32_t height;
        Node(Entry& entry);
    };

    Node* root;

    uint32_t height(Node* node);
    int32_t get_balance(Node* node);
    
    Node* right_rotate(Node* node);
    Node* left_rotate(Node* node);

    Node* insert(Node* node, Entry& entry);
    Node* delete_node(Node* root, Entry& entry);
    Node* delete_node(Node* root, Bits& key);

    Node* min_value_node(Node* node);

    void inorder(Node* root, std::vector<Entry>& result);

    Entry search(Node* root, Bits& key, bool& found);

    // destroys the entire tree
    void make_empty(Node*& node);

    Entry pop_last(Node*& node);

    public:
        AVL_tree(Entry& entry);
        AVL_tree();
        ~AVL_tree();

        void insert(Entry& entry);
        void remove(Entry& entry);
        void remove(Bits& key);

        // found is set to true if the given entry was found, and false otherwise
        Entry search(Bits& key, bool& found);

        void print_inorder();

        void make_empty();
        Entry pop_last();

    	std::vector<Entry> inorder();
};

#endif // YSQL_AVL_TREE_INCLUDED
