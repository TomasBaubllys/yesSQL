#include "../include/lsm_tree.h"
#include "../include/min_heap.h"
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <cstring>


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
        for(const SS_Table_Controller& ss_table_controller_level : ss_table_controllers){
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

    mem_table.insert_entry(entry);
    try{
        std::thread thread_1(&Wal::append_entry,&write_ahead_log, entry.get_ostream_bytes());
        std::thread thread_2(&MemTable::insert_entry, &mem_table, entry);

        thread_1.join();
        thread_2.join();

    }
    catch(std::exception& e){
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
            std::cerr << e.what() << std::endl;
            return false;
        }        
    }

    return true;

};

std::vector<std::string> LsmTree::get_keys(){
    std::vector<Entry> entries = mem_table.dump_entries();
    std::vector<std::string> keys;
    keys.reserve(entries.size());

    for(const Entry& entry : entries){
        keys.emplace_back(entry.get_key().get_string_char());
    }
    // add adding keys from ss tables and checking if overlap

    return keys;
};

std::vector<std::string> LsmTree::get_keys(std::string prefix){
    uint32_t string_start_position = 0;

    std::vector<Entry> entries = mem_table.dump_entries();
    std::vector<std::string> keys;
    

    for(const Entry& entry : entries){
        std::string key = entry.get_key().get_string_char();
        if(key.rfind("prefix", string_start_position)){
            keys.push_back(key);
        }
    }
    // add adding keys from ss tables and checking if overlap

    return keys;
};

std::vector<Entry> LsmTree::get_ff(std::string _key){
    int32_t ff_marker = -1;

    std::vector<std::string> keys = get_keys();

    for(uint32_t i = 0; i < keys.size(); ++i){
        if(keys.at(i) == _key){
            ff_marker = i;
            break;
        }
    }
    if (ff_marker == -1){
        // no magical strings
        std::cerr<<"No key was found";
    }

    std::vector<Entry> all_entries = mem_table.dump_entries();

    // add adding keys from ss tables and checking if overlap
    
    return std::vector<Entry>(all_entries.begin() + ff_marker, all_entries.end());

};

std::vector<Entry> LsmTree::get_fb(std::string _key){
    int fb_marker = -1;

    std::vector<std::string> keys = get_keys();

    for(uint32_t i = 0; i < keys.size(); ++i){
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
    catch(std::exception& e){
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

    // move to define
    std::filesystem::path level0_dir = "data/val/Level_0";
    if (!std::filesystem::exists(level0_dir)) {
        std::filesystem::create_directories(level0_dir);
    }

    // TODO LATER if sm_id > MAX_SS_TABLES_PER_LEVEL --> merge

    uint64_t current_name_index = ss_table_controllers.empty()? 0 : ss_table_controllers.front().get_current_name_counter();
    std::string filename_data(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');
    std::string filename_index(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');
    std::string filename_offset(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');

    snprintf(&filename_data[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_SS_TABLE_FILE_NAME_DATA,  0, current_name_index);
    snprintf(&filename_index[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_SS_TABLE_FILE_NAME_INDEX,  0, current_name_index);
    snprintf(&filename_offset[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_SS_TABLE_FILE_NAME_OFFSET,  0, current_name_index);

   
    std::filesystem::path filepath_data = level0_dir / filename_data;
    std::filesystem::path filepath_index = level0_dir / filename_index;
    std::filesystem::path filepath_offset = level0_dir / filename_offset;

    SS_Table* ss_table = new SS_Table(filepath_data, filepath_index, filepath_offset);

    uint16_t record_count = ss_table -> fill_ss_table(entries);

    if(record_count == 0){
        throw std::runtime_error("Empty entries vector, could not fill ss table");
    }

    if(ss_table_controllers.size() == 0){
        ss_table_controllers.emplace_back(SS_TABLE_CONTROLER_RATIO, ss_table_controllers.size());
    }

    // ss table controller level 0 add a table
    ss_table_controllers.at(0).add_sstable(ss_table);

    return;
 }

bool LsmTree::compact_level(level_index_type index) {
    // check for overflow
    if(index + 1 < index) {
        return false;
    }

    if(this -> ss_table_controllers.empty()){
        throw std::runtime_error(LSM_TREE_EMPTY_SS_TABLE_CONTROLLERS_ERR_MSG);
    }

    try {
        while(!ss_table_controllers.at(index).empty()) {
            // pair to save level index, and table index
            std::vector<std::pair<level_index_type, table_index_type>> overlapping_key_ranges;

            // push in our current table
            overlapping_key_ranges.push_back(std::make_pair(index, 0));
            
            Bits first_index = ss_table_controllers.at(index).front() -> get_first_index();
            Bits last_index = ss_table_controllers.at(index).front() -> get_last_index();

            // find all the overlapping keys and push them to the vector
            // if we are merging level 0 overlapping keys can be found in the same level
            if(index == 0) {
                for(table_index_type i = 1; i < ss_table_controllers.front().get_ss_tables_count(); ++i) {
                    if(ss_table_controllers.front().at(i) -> overlap(first_index, last_index)) {
                        overlapping_key_ranges.push_back(std::make_pair(0, i));
                    }    
                }
            }

            // now if the next level exist check for overlapping keys there
            if(ss_table_controllers.size() > index + 1) {
                for(table_index_type i = 0; i < ss_table_controllers.at(index + 1).get_ss_tables_count(); ++i) {
                    if(ss_table_controllers.at(index + 1).at(i) -> overlap(first_index, last_index)) {
                        overlapping_key_ranges.push_back(std::make_pair(index + 1, i));
                    }
                }
            }

            //  create a directory and a new table
            std::string filename_data(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');
            std::string filename_index(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');
            std::string filename_offset(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');

            uint64_t ss_table_count = ss_table_controllers.size() > index + 1? (ss_table_controllers.at(index + 1).get_current_name_counter()) : 0;

            snprintf(&filename_data[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_SS_TABLE_FILE_NAME_DATA,  index + 1, ss_table_count);
            snprintf(&filename_index[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_SS_TABLE_FILE_NAME_INDEX,  index + 1, ss_table_count);
            snprintf(&filename_offset[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_SS_TABLE_FILE_NAME_OFFSET,  index + 1, ss_table_count);

            std::string level_n_1_dir(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');
            snprintf(&level_n_1_dir[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_LEVEL_DIR, index + 1);
            level_n_1_dir.resize(strlen(level_n_1_dir.c_str())); // trim nulls

            if (!std::filesystem::exists(level_n_1_dir)) {
                std::filesystem::create_directories(level_n_1_dir);
            }

            std::filesystem::path filepath_data(std::filesystem::path(level_n_1_dir) / filename_data);
            std::filesystem::path filepath_index(std::filesystem::path(level_n_1_dir) / filename_index);
            std::filesystem::path filepath_offset(std::filesystem::path(level_n_1_dir) / filename_offset);
            
            SS_Table* new_table = new SS_Table(filepath_data, filepath_index, filepath_offset);

            // create keynators and push them to a vector
            std::vector<SS_Table::Keynator> keynators;

            // add all the other keynators
            for(const std::pair<level_index_type, table_index_type>& ss_table_data : overlapping_key_ranges) {
                level_index_type level_index = ss_table_data.first;
                table_index_type table_index = ss_table_data.second;
                keynators.push_back(ss_table_controllers.at(level_index).at(table_index) -> get_keynator());
            }

            // using heap push to a new table
            Min_Heap heap;

            for(table_index_type i = 0; i < keynators.size(); ++i) {
                heap.push(keynators.at(i).get_next_key(), overlapping_key_ranges.at(i).first, overlapping_key_ranges.at(i).second, &keynators.at(i));
            }

            new_table -> init_writing();
            while(!heap.empty()) {
                Min_Heap::Heap_Element top_element = heap.top();
                new_table -> write(top_element.key, top_element.keynator -> get_current_data_string());
                heap.remove_by_key(top_element.key);
            }
            new_table -> stop_writing();

            // sort the pair vector in ascending order
            std::sort(overlapping_key_ranges.begin(), overlapping_key_ranges.end(), [&](const std::pair<level_index_type, table_index_type>& a, const std::pair<level_index_type, table_index_type>& b) {
                return a.second < b.second;
            });

            while(!overlapping_key_ranges.empty()) {
                ss_table_controllers.at(overlapping_key_ranges.back().first).delete_sstable(overlapping_key_ranges.back().second);
                overlapping_key_ranges.pop_back();
            }

            // add the new table to our vector
            if(ss_table_controllers.size() == index + 1) {
                ss_table_controllers.emplace_back(SS_TABLE_CONTROLER_RATIO, ss_table_controllers.size());
            }

            ss_table_controllers.at(index + 1).add_sstable(new_table);
        }

    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return false;
    }

    return true;
}