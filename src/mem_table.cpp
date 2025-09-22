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

// sita fun reikia keist, nepatinka Bits sprendimas
bool MemTable::remove_entry(Bits key){
    try{
        std::string key_string = key.get_string_char();
        avl_tree.remove(key_string);
        return true;
    }
    catch(const std::exception& e){
        return false;
    }
    
};

Entry MemTable::find(Bits key){
    // rasti dalyka pagal rakto bits klase, nenaudoti string lyginimui
};
