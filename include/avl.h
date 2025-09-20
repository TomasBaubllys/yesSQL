#include<iostream>

using namespace std;

template <typename T>
class AVL
{
    struct node
    {
        T data;
        node* left;
        node* right;
        int height;
    };

    node* root;

    void makeEmpty(node* t);

    node* insert(T x, node* t);

    node* singleRightRotate(node* &t);

    node* singleLeftRotate(node* &t);

    node* doubleLeftRotate(node* &t);

    node* doubleRightRotate(node* &t);

    node* findMin(node* t);

    node* findMax(node* t);

    node* remove(T x, node* t);

    int height(node* t);

    int getBalance(node* t);

    void inorder(node* t);

    bool find(T x, node *t);

public:
    AVL();
    void insert(T x);
    void remove(T x);
    void display();
    bool find(T x);
};
