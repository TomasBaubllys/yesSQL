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


bool LsmTree::compact_level(uint16_t index){
    // check for overflow
    if(index + 1 < index) {
        return false;
    }

    if(this -> ss_table_controllers.empty()){
        throw std::runtime_error("no ss_table_controllers - no levels.");
    }


    // select one table at level[index]
    // get a vector of all overlapping key ranges in level[index + 1]
    try{
        // probably wrap this in try catch
        while(!ss_table_controllers[index].empty()){
            std::vector<uint16_t> ss_tables_overlaping_key_ranges_indexes;
            // only used for level0 compaction
            std::vector<uint16_t> ss_tables_overlaping_key_ranges_indexes_level_0;

            Bits first_index(ss_table_controllers[index][0] -> get_first_index());    
            Bits last_index(ss_table_controllers[index][0] -> get_last_index());


            // special compression for level 0; get overlapping ss_tables in the same 0 level
            if(index == 0){
                for(uint16_t m = 1; m < ss_table_controllers.at(0).get_ss_tables_count(); ++m){
                    if(ss_table_controllers.at(0)[m] -> overlap(first_index, last_index)){
                        ss_tables_overlaping_key_ranges_indexes_level_0.push_back(m);
                    }
                }
            }
            
            if(ss_table_controllers.size() > index + 1) {
                // get indexes of all overlapping ss_tables in level n + 1
                for(uint16_t j = 0; j < ss_table_controllers[index + 1].get_ss_tables_count(); ++j){
                    if(ss_table_controllers.at(index + 1)[j] -> overlap(first_index, last_index)){
                        ss_tables_overlaping_key_ranges_indexes.push_back(j);
                    }
                }
            }

            // turiu vectoriu visu lenteliu indexu kurias reikes kartu merginti
            // man reikia sukurti nauja lentele, i kuria viska merginsiu
            // reikia keynatoriu tu sstable is kuriu skaitau

            std::string filename_data(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');
            std::string filename_index(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');
            std::string filename_offset(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');

            // PROBLEM: nameing files: how do we know which one is deleted, which is still there? we cannot have repeating names
            // bad fix is still a fix 
            // has to be some global variable ;O
            uint64_t ss_table_count = ss_table_controllers.size() > index + 1? (ss_table_controllers[index + 1].get_current_name_counter()) : 0;

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

            // create keynator for the level n ss table
            SS_Table::Keynator keynator_level_n = (ss_table_controllers[index][0] -> get_keynator());

            std::vector<SS_Table::Keynator> keynators;
            keynators.push_back(std::move(keynator_level_n));

            int count_level_0_tables = 0;
            // level 0 compaction: create keynators for overlapping tables for level 0
            if(index == 0){
                for(uint16_t k  = 0; k < ss_tables_overlaping_key_ranges_indexes_level_0.size(); ++k){
                    uint16_t table_index = ss_tables_overlaping_key_ranges_indexes_level_0.at(k);
                    // KOKS GYVAVIMO LAIKAS SITO DAIKTO:DDDDDD
                    // USE std::unique ptr and std::move()
                    keynators.push_back(std::move((ss_table_controllers[index][table_index]) -> get_keynator()));
                    ++count_level_0_tables;
                    
                }
            }

            // create keynators for level n + 1 sstables
            for(uint16_t k  = 0; k < ss_tables_overlaping_key_ranges_indexes.size(); ++k){
                uint16_t table_index = ss_tables_overlaping_key_ranges_indexes.at(k);
                keynators.push_back(std::move((ss_table_controllers.at(index + 1)[table_index]) -> get_keynator()));
            }

            // now the heap logic....
            // ss_table turi gebet pasakyti koks yra jos file index... (0 lygio merginimui)
            // pagal ideja mes i ss_table_overlapping kisam visas lenteles,kurios saugomos contrllerio sstables, kontrollerio sstables yra vektorius, tai viskas kisama i gaa --> kuo didesnis indexas, tuo naujesne lentele
            Min_heap heap = Min_heap();

            for(uint16_t k = 0; k < (keynators.size() - count_level_0_tables); ++k){
                if(k == 0){
                    heap.push(keynators.front().get_next_key(), ss_table_controllers.at(index).get_level(), 0, &keynators.front());
                }
                else{
                    // fails here when keynator.size() > 1
                    // std::cout << k << "  " << ss_tables_overlaping_key_ranges_indexes.size() << std::endl;
                    // NOW IT WILL NOT WORK COMPLETELY fails because ss_tables_overlaping... == k
                    if(ss_table_controllers.size() > index + 1) {
                        std::cout << "HERE1" << std::endl;
                        heap.push(keynators.at(k).get_next_key(), ss_table_controllers.at(index + 1).get_level(), ss_tables_overlaping_key_ranges_indexes.at(k), &keynators.at(k));
                        std::cout << "HERE2" << std::endl;
                    }
                }
            }

            for(uint16_t k = 0; k < count_level_0_tables; ++k){
                uint16_t current_index = (keynators.size() - count_level_0_tables) + k;
                heap.push(keynators.at(current_index).get_next_key(), ss_table_controllers[index].get_level(), ss_tables_overlaping_key_ranges_indexes_level_0[k], &keynators.at(current_index));
            }

            new_table -> init_writing();
            // wont work for level 0 compactin!!!!
            while(!heap.empty()){
                Min_heap::Heap_element top_element = heap.top();
                new_table -> write(top_element.key, top_element.keynator -> get_current_data_string());
                heap.remove_by_key(top_element.key);
                
            };

            new_table -> stop_writing();

            // istrinti ss_table_controllers[index][i]
            // istrnti visas overlappinancias lenteles is index + 1
            // deleting tables

            std::sort(ss_tables_overlaping_key_ranges_indexes_level_0.begin(), ss_tables_overlaping_key_ranges_indexes_level_0.end());
            if(index == 0){
                while(!ss_tables_overlaping_key_ranges_indexes_level_0.empty()){
                    ss_table_controllers.at(index).delete_sstable(ss_tables_overlaping_key_ranges_indexes_level_0.back());
                    ss_tables_overlaping_key_ranges_indexes_level_0.pop_back();
                }
            }

            std::sort(ss_tables_overlaping_key_ranges_indexes.begin(), ss_tables_overlaping_key_ranges_indexes.end());
            while(!ss_tables_overlaping_key_ranges_indexes.empty()){
                ss_table_controllers.at(index + 1).delete_sstable(ss_tables_overlaping_key_ranges_indexes.back());
                ss_tables_overlaping_key_ranges_indexes.pop_back();
            }

            // noriu compactint 3 lygi, vadinas conreoller.size() yra 4, tai turiu tikrint 
            if(ss_table_controllers.size() == (uint16_t)(index + 1)){
                // reikia naujo lygio
                ss_table_controllers.emplace_back(SS_TABLE_CONTROLER_RATIO, ss_table_controllers.size());
            }

            // 0 -> 0; 1 -> 1 
            ss_table_controllers.at(index).delete_sstable(0);
            ss_table_controllers.at(index + 1).add_sstable(new_table);
        }
    }
    catch(std::exception& e){
        std::cerr << e.what() << std::endl;
        return false;
    }

    return true;
}
