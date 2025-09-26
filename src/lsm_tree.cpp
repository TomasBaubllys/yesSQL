#include "../include/lsm_tree.h"


LsmTree::LsmTree(){
    write_ahead_log = Wal();
    mem_table = MemTable();
};

LsmTree::~LsmTree(){
};

Entry LsmTree::get(std::string key){
    Bits key_bits(key);

    Entry return_entry = mem_table.find(key_bits);

    return return_entry;

    // add sstable handling if no entry found in memtable
};

bool LsmTree::set(std::string key, std::string value){
    
    
    Bits key_bits(key);
    Bits value_bits(value);

    Entry entry(key_bits, value_bits);

    try{
        std::thread thread_1(&Wal::append_entry,&write_ahead_log, &entry);
        std::thread thread_2(&MemTable::insert_entry, &mem_table, &entry);

        thread_1.join();
        thread_2.join();

        return true;
    }
    catch(std::exception e){
        return false;
    }
    
};

vector<std::string> LsmTree::get_keys(){

};

vector<std::string> LsmTree::get_keys(std::string prefix){

};

vector<Entry> LsmTree::get_ff(std::string key){

};

vector<Entry> LsmTree::get_fb(std::string key){

};

bool remove(std::string key){

};


// LsmTree
// To do:
// SSTable rasymas is MemTable
// SSTable skaitymas
// GET <key>
// SET <key> <value>
// Multithread SET method to write into WAL and MemTable at the same time
// GETKEYS - grąžina visus raktus
// GETKEYS <prefix> - grąžina visus raktus su duota pradžia
// GETFF <key> - gauti raktų reikšmių poras nuo key
// GETFB <key> - gauti raktų reikšmių poras nuo key atvirkščia tvarka
// GETFF ir GETFB turi leisti puslapiuoti, t.y. gauti po n porų ir fektyviai tęsti toliau
// REMOVE <key>

