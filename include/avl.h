#include <iostream>
#include "entry.h"

using namespace std;

template <typename T>
class AVL
{
    struct node
    {
        Entry data;
        node* left;
        node* right;
        uint32_t height;
    };

    node* root;

    uint32_t height(node* t);
    uint32_t get_balance(node* t);
    
    node* right_rotate(node* t);
    node* left_rotate(node* t);

    node* insert(node* t, Entry key);
    node* delete_note(node* root, Entry key);

    node* min_value_node(node* t);

    void inorder(node* root);

    bool search(node* root, Entry key);


    public:
        AVL(Entry &root);
        AVL();

        void insert(Entry key);
        void remove(Entry key);
        bool search(Entry key);

        void print_inorder();

        Entry get_last();
        void make_empty(node* t);
        Entry pop_last();

};
