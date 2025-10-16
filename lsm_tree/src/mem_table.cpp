#include "../include/mem_table.h"

MemTable::MemTable(){
    avl_tree = AVL_Tree();
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
        std::cerr << e.what() << std::endl;
        return false;
    }
    
};

bool MemTable::remove_find_entry(Bits key){
    bool is_entry_found;
    try{
        // for now create a copy, we can play with memory in the future
        Entry entry = avl_tree.search(key, is_entry_found);
        if(is_entry_found && !entry.is_deleted()){
            entry.set_tombstone(true);
            avl_tree.insert(entry);
        };
        return true;
    }
    catch(const std::exception& e){
        return false;
    }
    
};

bool MemTable::remove_entry(Entry& entry){
    try{
        if(!entry.is_deleted()){
            entry.set_tombstone(true);
            avl_tree.insert(entry);
        };
        return true;
    }
    catch(const std::exception& e){
        return false;
    }
    
};

Entry MemTable::find(Bits key, bool& found){
    Entry found_entry = avl_tree.search(key, found);

    return found_entry;
};

bool MemTable::is_full(){

    if(total_mem_table_size >= MEM_TABLE_BYTES_MAX_SIZE){
        return true;
    }

    return false;
};

std::vector<Entry> MemTable::dump_entries(){
    std::vector<Entry> results;
    results = avl_tree.inorder();

    return results;
};

void MemTable::make_empty() {
    this -> avl_tree.make_empty();
}