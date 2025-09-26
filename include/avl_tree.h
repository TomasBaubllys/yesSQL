#include <iostream>
#include "entry.h"
#include <algorithm>
#include <vector.h>

using namespace std;

class AVL_tree
{
    struct Node
    {
        Entry data;
        Node* left;
        Node* right;
        int32_t height;
    };

    Node* root;

    uint32_t height(Node* node);
    uint32_t get_balance(Node* node);
    
    Node* right_rotate(Node* node);
    Node* left_rotate(Node* node);

    Node* insert(Node* node, Entry key);
    Node* delete_node(Node* root, Entry key);

    Node* min_value_node(Node* node);

    std::vector<Entry> inorder(Node* root);

    bool search(Node* root, Entry key);


    public:
        AVL_tree(Entry &root);
        AVL_tree();

        void insert(Entry key);
        void remove(Entry key);
        bool search(Entry key);

        void print_inorder();

        Entry get_last();
        void make_empty(Node* t);
        Entry pop_last();

    	std::vector<Entry> inorder(Node* root);
};
