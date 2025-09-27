#include <iostream>
#include "entry.h"
#include <algorithm>
#include <vector>

using namespace std;

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

    Node* min_value_node(Node* node);

    void inorder(Node* root, std::vector<Entry>& result);

    bool search(Node* root, Bits& key);


    public:
        AVL_tree(Entry& entry);
        AVL_tree();

        void insert(Entry& entry);
        void remove(Entry& entry);
        bool search(Bits& key);

        void print_inorder();

        Entry get_last();
        void make_empty(Node* t);
        Entry pop_last();

    	std::vector<Entry> inorder();
};
