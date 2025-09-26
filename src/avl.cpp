#include "../include/avl.h"

template<typename T>
void AVL<T>::makeEmpty(node* t)
{
    if(t == NULL)
        return;
    makeEmpty(t->left);
    makeEmpty(t->right);
    delete t;
}

template<typename T>
AVL<T>::node* AVL<T>::insert(T x, node* t)
{
    if(t == NULL)
    {
        t = new node;
        t->data = x;
        t->height = 0;
        t->left = t->right = NULL;
    }
    else if(x < t->data)
    {
        t->left = insert(x, t->left);
        if(height(t->left) - height(t->right) == 2)
        {
            if(x < t->left->data)
                t = singleRightRotate(t);
            else
                t = doubleRightRotate(t);
        }
    }
    else if(x > t->data)
    {
        t->right = insert(x, t->right);
        if(height(t->right) - height(t->left) == 2)
        {
            if(x > t->right->data)
                t = singleLeftRotate(t);
            else
                t = doubleLeftRotate(t);
        }
    }

    t->height = max(height(t->left), height(t->right))+1;
    return t;
}

template<typename T>
AVL<T>::node* AVL<T>::singleRightRotate(node* &t)
{
    node* u = t->left;
    t->left = u->right;
    u->right = t;
    t->height = max(height(t->left), height(t->right))+1;
    u->height = max(height(u->left), t->height)+1;
    return u;
}

template<typename T>
AVL<T>::node* AVL<T>::singleLeftRotate(node* &t)
{
    node* u = t->right;
    t->right = u->left;
    u->left = t;
    t->height = max(height(t->left), height(t->right))+1;
    u->height = max(height(t->right), t->height)+1 ;
    return u;
}

template<typename T>
AVL<T>::node* AVL<T>::doubleLeftRotate(node* &t)
{
    t->right = singleRightRotate(t->right);
    return singleLeftRotate(t);
}

template<typename T>
AVL<T>::node* AVL<T>::doubleRightRotate(node* &t)
{
    t->left = singleLeftRotate(t->left);
    return singleRightRotate(t);
}

template<typename T>
AVL<T>::node* AVL::findMin(node* t)
{
    if(t == NULL)
        return NULL;
    else if(t->left == NULL)
        return t;
    else
        return findMin(t->left);
}

template<typename T>
AVL<T>::node* AVL::findMax(node* t)
{
    if(t == NULL)
        return NULL;
    else if(t->right == NULL)
        return t;
    else
        return findMax(t->right);
}

template<typename T>
AVL<T>::node* AVL::remove(T x, node* t)
{
    node* temp;

    // Element not found
    if(t == NULL)
        return NULL;

    // Searching for element
    else if(x < t->data)
        t->left = remove(x, t->left);
    else if(x > t->data)
        t->right = remove(x, t->right);

    // Element found
    // With 2 children
    else if(t->left && t->right)
    {
        temp = findMin(t->right);
        t->data = temp->data;
        t->right = remove(t->data, t->right);
    }
    // With one or zero child
    else
    {
        temp = t;
        if(t->left == NULL)
            t = t->right;
        else if(t->right == NULL)
            t = t->left;
        delete temp;
    }
    if(t == NULL)
        return t;

    t->height = max(height(t->left), height(t->right))+1;

    // If node is unbalanced
    // If left node is deleted, right case
        if(height(t->left) - height(t->right) == -2)
        {
            // right right case
            if(height(t->right->right) - height(t->right->left) == 1)
                return singleLeftRotate(t);
            // right left case
            else
                return doubleLeftRotate(t);
        }
        // If right node is deleted, left case
        else if(height(t->right) - height(t->left) == 2)
        {
            // left left case
            if(height(t->left->left) - height(t->left->right) == 1){
                return singleRightRotate(t);
            }
            // left right case
            else
                return doubleRightRotate(t);
        }
    return t;
}

template<typename T>
int AVL<T>::height(node* t)
{
    return (t == NULL ? -1 : t->height);
}

template<typename T>
int AVL<T>::getBalance(node* t)
{
    if(t == NULL)
        return 0;
    else
        return height(t->left) - height(t->right);
}

template<typename T>
void AVL<T>::inorder(node* t)
{
    if(t == NULL)
        return;
    inorder(t->left);
    cout << t->data << " ";
    inorder(t->right);
}

template<typename T>
AVL<T>::AVL()
{
    root = NULL;
}

template<typename T>
void AVL<T>::insert(T x)
{
    root = insert(x, root);
}

template<typename T>
void AVL<T>::remove(T x)
{
    root = remove(x, root);
}

template<typename T>
void AVL<T>::display()
{
    inorder(root);
    cout << endl;
}

template<typename T>
bool AVL<T>::find(T x, node *t)
{
    if(!t)
    {
        return false;
    }

    if(t -> data == x)
    {
        return true;
    }

    if(x > t -> data)
    {
        find(x, t -> right);
    }
    else
    {
        find(x, t -> left);
    }

    return false;
}

template<typename T>
bool AVL<T>::find(T x)
{
    return find(x, root);
}
