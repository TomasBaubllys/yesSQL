#include "../include/mem_table.h"

MemTable::MemTable(){
    avl_tree = AVL_tree();
    entry_array_length = 0;
    total_mem_table_size = 0;
};

MemTable::~MemTable(){
    //destroy AVL
};

int MemTable::get_entry_array_length(){
    return entry_array_length;
};

uint64_t MemTable::get_total_mem_table_size(){
    return total_mem_table_size;
};

bool MemTable::insert_entry(Entry entry){
    try{
        avl_tree.insert(entry);
        entry_array_length++;
        total_mem_table_size += entry.get_entry_length();
        return true;
    }
    catch(const std::exception& e){
        return false;
    }
    
};

bool MemTable::remove_entry(Bits key){
    try{
        bool was_key_found;
        entry_array_length--;
        total_mem_table_size -= avl_tree.search(key, was_key_found).get_entry_length();
        avl_tree.remove(key);
        return true;
    }
    catch(const std::exception& e){
        return false;
    }
    
};

Entry MemTable::find(Bits key){
    bool found;
    Entry found_entry = avl_tree.search(key, found);

    return found_entry;
};

std::vector<Entry> MemTable::dump_entries(){
    std::vector<Entry> results;
    results = avl_tree.inorder();

    return results;
}

