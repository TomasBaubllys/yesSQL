#include "../include/mem_table.h"

MemTable::MemTable(){
    avl_tree = AVL<Entry>();
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
        entry_array_length--;
        total_mem_table_size -= avl_tree.remove(key).get_entry_length();
        return true;
    }
    catch(const std::exception& e){
        return false;
    }
    
};

Entry MemTable::find(Bits key){
    Entry found_entry = avl_tree.search(key);

    return found_entry;
};
