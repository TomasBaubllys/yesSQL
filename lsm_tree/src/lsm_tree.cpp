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

    bool is_found;

    Entry entry = mem_table.find(key_bits, is_found);

    if(is_found){
        return entry;
    }

    if(ss_table_controllers.size() > 0){
        for(auto ss_table_controller_level : ss_table_controllers){
            entry = ss_table_controller_level.get(key_bits, is_found);
            
            if(is_found){
                return entry;
            }
        }
    }

    std::cout<<"No entry found"<<std::endl;
    return entry;
};

bool LsmTree::set(std::string key, std::string value){
    Bits key_bits(key);
    Bits value_bits(value);

    Entry entry(key_bits, value_bits);

    try{
        std::thread thread_1(&Wal::append_entry,&write_ahead_log, entry.get_ostream_bytes());
        std::thread thread_2(&MemTable::insert_entry, &mem_table, entry);

        thread_1.join();
        thread_2.join();

    }
    catch(std::exception e){
        return false;
    }

    if(mem_table.is_full()){
        try{
            flush_mem_table();

            mem_table.~MemTable();
            write_ahead_log.clear_entries();

            mem_table = MemTable();

        }
        catch(const std::exception& e){
            return false;
        }        
    }

    return true;

};

std::vector<std::string> LsmTree::get_keys(){
    std::vector<Entry> entries = mem_table.dump_entries();
    std::vector<std::string> keys;
    keys.reserve(entries.size());

    for(auto entry : entries){
        keys.emplace_back(entry.get_key().get_string_char());
    }
    // add adding keys from ss tables and checking if overlap

    return keys;
};

std::vector<std::string> LsmTree::get_keys(std::string prefix){
    int string_start_position = 0;

    std::vector<Entry> entries = mem_table.dump_entries();
    std::vector<std::string> keys;
    

    for(auto entry : entries){
        std::string key = entry.get_key().get_string_char();
        if(key.rfind("prefix", string_start_position)){
            keys.push_back(key);
        }
    }
    // add adding keys from ss tables and checking if overlap

    return keys;
};

std::vector<Entry> LsmTree::get_ff(std::string _key){
    int ff_marker = -1;

    std::vector<std::string> keys = get_keys();

    for(int i = 0; i<keys.size(); ++i){
        if(keys.at(i) == _key){
            ff_marker = i;
            break;
        }
    }
    if (ff_marker == -1){
        std::cerr<<"No key was found";
    };

    std::vector<Entry> all_entries = mem_table.dump_entries();

    // add adding keys from ss tables and checking if overlap
    
    return std::vector<Entry>(all_entries.begin() + ff_marker, all_entries.end());

};

std::vector<Entry> LsmTree::get_fb(std::string _key){
    int fb_marker = -1;

    std::vector<std::string> keys = get_keys();

    for(int i = 0; i<keys.size(); ++i){
        if(keys.at(i) == _key){
            fb_marker = i;
            break;
        }
    }
    if (fb_marker == -1){
        std::cerr<<"No key was found";
    }

    std::vector<Entry> all_entries = mem_table.dump_entries();

    // add adding keys from ss tables and checking if overlap
    
    return std::vector<Entry>(all_entries.begin(), all_entries.begin() + fb_marker);
};

bool LsmTree::remove(std::string key){
    try{
        Entry entry = get(key);

        if(!entry.is_deleted()){
            entry.set_tombstone(true);
        }
        
        std::thread thread_1(&Wal::append_entry, &write_ahead_log, entry.get_ostream_bytes());
        std::thread thread_2(&MemTable::insert_entry, &mem_table, entry);

        thread_1.join();
        thread_2.join();
    }
    catch(std::exception e){
        return false;
    }

    if(mem_table.is_full()){
        try{
            flush_mem_table();

            mem_table.~MemTable();
            write_ahead_log.clear_entries();

            mem_table = MemTable();

        }
        catch(const std::exception& e){
            return false;
        }        
    }
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
        std::cout << "Failed to create a Level_0 SStable" << std::endl;
    }

    for(auto& entry : entries){
        auto offset = ofsd.tellp();

        std::ostringstream oss = entry.get_ostream_bytes();
        std::string raw = oss.str();
        ofsd.write(raw.data(), raw.size());

        Bits key = entry.get_key();
        bit_arr_size_type key_size = key.size();

        ofsi.write(reinterpret_cast<char*>(&key_size), sizeof(key_size));
        Bits key = entry.get_key();
        ofsi.write(key.get_string_char().c_str(), key_size);
        ofsi.write(reinterpret_cast<char*>(&offset), sizeof(offset));
    }

    

    ofsd.close();
    ofsi.close();

    SS_Table ss_table_l0(filename_data, filename_index);

    if(ss_table_controllers.size() == 0){
            SS_Table_Controller controller_l0 = SS_Table_Controller(ratio, ss_table_controllers.size() + 1);
            ss_table_controllers.push_back(controller_l0);
    }

    // ss table controller level 0 add a table
    ss_table_controllers.at(0).add_sstable(ss_table_l0);

    if(ss_table_controllers.at(0).is_over_limit()){
        compact_level(0);
    }

    return;
 }

