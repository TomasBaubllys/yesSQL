#include "../include/lsm_tree.h"
#include <filesystem>
#include <fstream>
#include <iostream>


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

// data compression??
 void LsmTree::flush_mem_table(){
    std::vector<Entry> entries = mem_table.dump_entries();

    std::filesystem::path level0_dir = "data/val/Level_0";
    if (!std::filesystem::exists(level0_dir)) {
        std::filesystem::create_directories(level0_dir);
    }

    int sm_id = 0;

    for(auto& p : std::filesystem::directory_iterator(level0_dir)){
        if(p.is_regular_file()){
            ++sm_id;
        }
    }
    // TODO LATER if sm_id > MAX_SS_TABLES_PER_LEVEL --> merge

    std::string filename_data = "sst_l0_data_" + std::to_string(sm_id) + ".bin";
    std::string filename_index = "sst_l0_index_" + std::to_string(sm_id) + ".bin";
    std::filesystem::path filepath_data = level0_dir / filename_data;
    std::filesystem::path filepath_index = level0_dir / filename_index;


    std::ofstream ofsd(filepath_data, std::ios::binary);
    std::ofstream ofsi(filename_index, std::ios::binary);
    if(!ofsd && !ofsi){
        cout << "Failed to create a Level_0 SStable" << endl;
    }

    for(auto& entry : entries){
        auto offset = ofsd.tellp();
        std::ostringstream oss = entry.get_ostream_bytes();
        std::string raw = oss.str();

        ofsd.write(raw.data(), raw.size());
    }

    ofsd.close();
    ofsi.close();

    return;
 }

