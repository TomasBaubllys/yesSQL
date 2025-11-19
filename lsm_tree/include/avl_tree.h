#ifndef YSQL_AVL_TREE_INCLUDED
#define YSQL_AVL_TREE_INCLUDED

#include <iostream>
#include "entry.h"
#include <algorithm>
#include <vector>
#include <set>

#define AVL_TREE_INSERTION_FAILED_ERR "Failed to insert given entry to the tree\n"
#define AVL_TREE_DELETION_FAILED_ERR "Failed to delete given entry from the tree\n"

class AVL_Tree
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

    template <typename T, typename Extractor>
    void collect_larger(Node* node, const Bits& threshold_key, uint32_t count, std::vector<T>& results, std::set<Bits>& dead_keys, Extractor extractor) const {
        if (!node || results.size() >= count) {
            return;
        }

        const Bits& current_key = node -> data.get_key();

        if (current_key >= threshold_key) {
            if(current_key > threshold_key) {
                collect_larger(node->left, threshold_key, count, results, dead_keys, extractor);
            }

            if (results.size() >= count) {
                return;
            }

            if (!node -> data.is_deleted()) {
                results.push_back(extractor(node -> data));
            }
            else {
                std::cout<<"I am at the deleted node and am pushing into dead_keys"<<std::endl;
                std::cout<<"key: "<<current_key.get_string()<<std::endl;
                dead_keys.emplace(node -> data.get_key());
            }

            if (results.size() >= count) return;

            collect_larger(node -> right, threshold_key, count, results, dead_keys, extractor);
        } 
        else {
            collect_larger(node -> right, threshold_key, count, results, dead_keys, extractor);
        }
    }

    public:
        AVL_Tree(Entry& entry);
        AVL_Tree();
        ~AVL_Tree();

        void insert(Entry& entry);
        void remove(Entry& entry);
        void remove(Bits& key);

        // found is set to true if the given entry was found, and false otherwise
        Entry search(Bits& key, bool& found);

        void print_inorder();

        void make_empty();
        Entry pop_last();

    	std::vector<Entry> inorder();

        std::vector<Entry> get_entries_larger_than_alive(const Bits& key, uint32_t count, std::set<Bits>& dead_keys) const;

        std::vector<Bits> get_keys_larger_than_alive(const Bits& key, uint32_t count, std::set<Bits>& dead_keys) const;

};

#endif // YSQL_AVL_TREE_INCLUDED
