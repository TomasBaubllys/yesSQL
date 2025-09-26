#include "../include/avl_tree.h"

/*
TODO:
- when inserting check entry checksum
-
-
-
*/

int32_t AVL_tree::height(AVL_tree::Node* node) {
	return node? node -> height : 0;
}

int32_t AVL_tree::get_balance(AVL_tree::Node* node) {
	return node? this -> height(node -> left) - this -> height(node -> right): 0;
}

AVL_tree::Node* AVL_tree::right_rotate(AVL_tree::Node* node) {
	if(!node) {
		return nullptr;
	}	

	AVL_tree::Node* l_node = node -> left;
	if(!l_node) {
		return nullptr;
	}

	AVL_tree::Node* lr_node = l_node -> right;

	// perform the rotation
	l_node -> right = node;
	node -> left = lr_node;
	
	// update heights
	node -> height = std::max(this -> height(node -> left), this -> height(node -> right)) + 1;
	l_node -> height = std::max(this -> height(l_node -> left), this -> height(l_node -> right)) + 1;
	
	return l_node;
}
	
AVL_tree::Node* AVL_tree::left_rotate(AVL_tree::Node* node) {
	if(!node) {
		return nullptr;
	}

	AVL_tree::Node* r_node = node -> right;
	if(!r_node) {
		return nullptr;
	}

	AVL_tree::Node* rl_node = r_node -> left;

	r_node -> left = node;
	node -> right = rl_node;

	node -> height = std::max(this -> height(node -> left), this -> height(node -> right)) + 1;
	r_node -> height = std::max(this -> height(r_node -> left), this -> height(r_node -> right)) + 1;
	
	return r_node;
}

AVL_tree::Node* insert(AVL_tree::Node* node, Entry entry) {
	// CHECK ENTRY CHECKSUM	

	if(!node) {
		AVL_tree::Node *new_node = new AVL_tree::Node();
		if(!new_node) {
			return nullptr;
		}
		
		new_node -> data = entry;
		new_node -> height = 0;
		new_node -> left = nullptr;
		new_node -> right = nullptr;
		
		return new_node;
	}

	if(entry < node -> data) {
		node -> left = insert(node -> left, entry);
	}	
	else if (entry > node -> data) {
		node -> right = insert(node -> right, entry);
	}
	else {
		return node; 
	}
	
	node -> height = 1 + std::max(this -> height(node -> left), this -> height(node -> right));	
	
	int32_t balance = this -> get_balance(node);
	
	// left left case
	if (balance > 1 && entry < node -> left -> data) {
		return this -> right_rotate(node);
	}
	// right right case
	if(balance < -1 && entry > node -> right -> data) {
		return this -> left_rotate(node);
	}
	// left right case
	if(balance > 1 && entry > node -> left -> data) {
		node -> left = this -> left_rotate(node -> left);
		return this -> right_rotate(node);
	}
	// right left rotate
	if(balance < -1 && entry < node -> right -> data) {
		node -> right = this -> right_rotate(node -> right);
		return this -> left_rotate(node);
	}

	return node;
}



