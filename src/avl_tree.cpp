#include "../include/avl_tree.h"

AVL_tree::Node::Node(Entry& entry) : data(entry), left(nullptr), right(nullptr), height(1) {

}

uint32_t AVL_tree::height(AVL_tree::Node* node) {
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

AVL_tree::Node* AVL_tree::insert(AVL_tree::Node* node, Entry& entry) {
	if(!entry.check_checksum()) {
		std::cerr << ENTRY_CHECKSUM_MISMATCH;
		return nullptr;
	}

	if(!node) {
		AVL_tree::Node *new_node = new AVL_tree::Node(entry);
		if(!new_node) {
			return nullptr;
		}

		return new_node;
	}

	if(entry < node -> data) {
		node -> left = insert(node -> left, entry);
	}	
	else if (entry > node -> data) {
		node -> right = insert(node -> right, entry);
	}
	else {
		node -> data = entry;
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

AVL_tree::Node* AVL_tree::min_value_node(AVL_tree::Node* node) {
	if(!node) {
		return nullptr;
	}	
	
	AVL_tree::Node* current = node;
	while(current -> left) {
		current = current -> left;
	}
	return current;
}

AVL_tree::Node* AVL_tree::delete_node(AVL_tree::Node* node, Bits& key) {
	if(!node) {
		return node;
	}

	if(key < node -> data.get_key()) {
		node -> left = this -> delete_node(node -> left, key);
	}
	else if(key > node -> data.get_key()) {
		node -> right = this -> delete_node(node -> right, key);
	}
	else {
		if(!(node -> left) || !(node -> right)) {
			AVL_tree::Node* temp = node -> left? node -> left : node -> right;
			if(!temp) {
				// temp = node;
				delete node;
				node = nullptr;
			}
			else {
				AVL_tree::Node* old = node;
				node = temp;
				delete old;
			}
		}
		else {
			AVL_tree::Node* temp = min_value_node(node -> right);
			node -> data = temp -> data;
			Bits temp_key = temp -> data.get_key();
			node -> right = this -> delete_node(node -> right, temp_key);
		}
	}

	if(!node) {
		return node;
	}

	node -> height = 1 + std::max(this -> height(node -> left), this -> height(node -> right));

	int32_t balance = this -> get_balance(node);

	if(balance > 1 && this -> get_balance(node -> left) >= 0) {
		return this -> right_rotate(node);
	}

	if(balance > 1 && this -> get_balance(node -> left) < 0) {
		node -> left = this -> left_rotate(node -> left);
		return this -> right_rotate(node);
	}

	if(balance < -1 && this -> get_balance(node -> right) <= 0) {
		return this -> left_rotate(node);
	}

	if(balance < -1 && this -> get_balance(node -> right) > 0) {
		node -> right = this -> right_rotate(node -> right);
		return this -> left_rotate(node);
	}

	return node;
}

// instead of entry use Bits
AVL_tree::Node* AVL_tree::delete_node(AVL_tree::Node* node, Entry& entry) {
	if(!node) {
		return node;
	}

	if(entry < node -> data) {
		node -> left = this -> delete_node(node -> left, entry);
	}
	else if(entry > node -> data) {
		node -> right = this -> delete_node(node -> right, entry);
	}
	else {
		if(!(node -> left) || !(node -> right)) {
			AVL_tree::Node* temp = node -> left? node -> left : node -> right;
			if(!temp) {
				// temp = node;
				delete node;
				node = nullptr;
			}
			else {
				AVL_tree::Node* old = node;
				node = temp;
				delete old;
			}
		}
		else {
			AVL_tree::Node* temp = min_value_node(node -> right);
			node -> data = temp -> data;
			node -> right = this -> delete_node(node -> right, temp -> data);
		}
	}

	if(!node) {
		return node;
	}

	node -> height = 1 + std::max(this -> height(node -> left), this -> height(node -> right));

	int32_t balance = this -> get_balance(node);
	
	if(balance > 1 && this -> get_balance(node -> left) >= 0) {
		return this -> right_rotate(node);
	}

	if(balance > 1 && this -> get_balance(node -> left) < 0) {
		node -> left = this -> left_rotate(node -> left);
		return this -> right_rotate(node);
	}

	if(balance < -1 && this -> get_balance(node -> right) <= 0) {
		return this -> left_rotate(node);
	}

	if(balance < -1 && this -> get_balance(node -> right) > 0) {
		node -> right = this -> right_rotate(node -> right);
		return this -> left_rotate(node);
	}

	return node;
}

void AVL_tree::inorder(AVL_tree::Node* node, std::vector<Entry>& result) {
	if(node) {
		this -> inorder(node -> left, result);
		result.push_back(node -> data);
		this -> inorder(node -> right, result);
	}	
}

void AVL_tree::print_inorder() {
	std::vector<Entry> vec_inord = this -> inorder();

	for(uint32_t i = 0; i < vec_inord.size(); ++i) {
		std::cout << vec_inord[i].get_ostream_bytes().str() << " ";
	}
}

Entry AVL_tree::search(AVL_tree::Node* node, Bits& key, bool& found) {
	if(!node) {
		found = false;
		std::string key(ENTRY_PLACEHOLDER_KEY);
		std::string value(ENTRY_PLACEHOLDER_VALUE);
		return Entry(Bits(key), Bits(value));
	}
	if(node -> data.get_key() == key) {
		found = true;
		return node -> data;
	}

	if(key < node -> data.get_key()) {
		return this -> search(node -> left, key, found);
	}

	return this -> search(node -> right, key, found);
}

AVL_tree::AVL_tree() : root(nullptr) {
	
}

AVL_tree::AVL_tree(Entry& entry) {
	this -> root = new AVL_tree::Node(entry);
	
	if(!this -> root) {
		// throw something
	}
}

void AVL_tree::insert(Entry& entry) {
	AVL_tree::Node* new_root = this -> insert(this -> root, entry);
	
	if(!new_root) {
		std::cerr << AVL_TREE_INSERTION_FAILED_ERR;
		return;
	}

	this -> root = new_root;
}

void AVL_tree::remove(Entry& entry) {
	AVL_tree::Node* new_root = this -> delete_node(this -> root, entry);
	if(!new_root) {
		std::cerr << AVL_TREE_DELETION_FAILED_ERR;
		return;

	}
	
	this -> root = new_root;
}

void AVL_tree::remove(Bits& key) {
	AVL_tree::Node* new_root = this -> delete_node(this -> root, key);
	if(!new_root) {
		std::cerr << AVL_TREE_DELETION_FAILED_ERR;
		return;
	}

	this -> root = new_root;
}

Entry AVL_tree::search(Bits& key, bool& found) {
	return this -> search(this -> root, key, found);
}

std::vector<Entry> AVL_tree::inorder() {
	std::vector<Entry> entry_vector;
	this -> inorder(this -> root, entry_vector);
	return entry_vector;
}

void AVL_tree::make_empty(AVL_tree::Node* root) {
	if(!root) {
		return;
	}
	
	this -> make_empty(root -> left);
	this -> make_empty(root -> right);

	delete root;
	root = nullptr;
} 

void AVL_tree::make_empty() {
	if(this -> root) {
		this -> make_empty(this -> root);	
	}
}

AVL_tree::~AVL_tree() {
	this -> make_empty();
}

Entry AVL_tree::pop_last() {
	return this -> pop_last(this -> root);
}

Entry AVL_tree::pop_last(AVL_tree::Node*& node) {
	// if tree is empty return a placeholder
	if(!node) {
		std::string string_key(ENTRY_PLACEHOLDER_KEY);
		std::string string_value(ENTRY_PLACEHOLDER_VALUE);
		return Entry(Bits(string_key), Bits(string_value));
	}
	
	// rightmost node
	if(!(node -> right)) {
		Entry result = node -> data;
		AVL_tree::Node* left_child = node -> left;
		delete node;
		node = left_child;
		return result;
	}

	Entry result = this -> pop_last(node -> right);
	
	node -> height = 1 + std::max(this -> height(node -> left), this -> height(node -> right));

	int32_t balance = this -> get_balance(node);

	if(balance > 1 && this -> get_balance(node -> left) >= 0) {
		node = this -> right_rotate(node);
	}
	else if(balance > 1 && this -> get_balance(node -> left) < 0) {
		node -> left = this -> left_rotate(node -> left);
		node = this -> right_rotate(node);
	}
	else if(balance < -1 && this -> get_balance(node -> right) <= 0) {
		node = this -> left_rotate(node);
	}
	else if(balance < -1 && this -> get_balance(node -> right) > 0) {
		node -> right = this -> right_rotate(node -> right);
		node = this -> left_rotate(node);
	}

	return result;
}
