#include "../include/lsm_tree.h"
#include "../include/min_heap.h"
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

    Entry return_entry = mem_table.find(key_bits);

    return return_entry;

    // add sstable handling if no entry found in memtable
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

        return true;
    }
    catch(std::exception e){
        return false;
    }
    
    // add memtable check and flushing to sstable


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
        return;
    }

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
        return;
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

        return true;
    }
    catch(std::exception e){
        return false;
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

    std::string filename_data(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');
    std::string filename_index(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');
    std::string filename_offset(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');

    snprintf(&filename_data[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_SS_TABLE_FILE_NAME_DATA,  0, std::to_string(sm_id));
    snprintf(&filename_index[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_SS_TABLE_FILE_NAME_INDEX,  0, std::to_string(sm_id));
    snprintf(&filename_offset[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_SS_TABLE_FILE_NAME_OFFSET,  0, std::to_string(sm_id));

   
    std::filesystem::path filepath_data = level0_dir / filename_data;
    std::filesystem::path filepath_index = level0_dir / filename_index;
    std::filesystem::path filepath_offset = level0_dir / filename_offset;

    SS_Table ss_table(filepath_data, filepath_index, filepath_offset);

    uint16_t record_count = ss_table.fill_ss_table(entries);

    if(record_count == 0){
        throw std::runtime_error("Empty entries vector, could not fill ss table");

    }

    if(ss_table_controllers.size() == 0){
            SS_Table_Controller controller_l0 = SS_Table_Controller(SS_TABLE_CONTROLER_RATIO, ss_table_controllers.size());
            ss_table_controllers.push_back(controller_l0);
    }

    // ss table controller level 0 add a table
    ss_table_controllers.at(0).add_sstable(ss_table);

    return;
 }

bool LsmTree::compact_level(uint16_t index){
>>>>>>> SStable
    if(this -> ss_table_controllers.empty()){
        throw std::runtime_error("no ss_table_controllers - no levels.");
    }

    if(ss_table_controllers.size() <= index + 1){
        // reikia naujo lygio
        SS_Table_Controller level_n_1 = SS_Table_Controller(SS_TABLE_CONTROLER_RATIO, ss_table_controllers.size());
        ss_table_controllers.push_back(level_n_1);
    }

    // select one table at level[index]
    // get a vector of all overlapping key ranges in level[index + 1]
    try{
        // probably wrap this in try catch

        for(uint16_t i = 0; i < ss_table_controllers[index].get_ss_tables_count(); ++i){
            std::vector<uint16_t> ss_tables_overlaping_key_ranges_indexes;

            Bits first_index = ss_table_controllers[index][i].get_first_index();    
            Bits last_index = ss_table_controllers[index][i].get_last_index();
        
            // get indexes of all overlapping ss_tables in level n + 1
            for(uint16_t j = 0; i < ss_table_controllers[index + 1].get_ss_tables_count(); ++j){
                if(ss_table_controllers[index + 1][j].overlap(first_index, last_index)){
                    ss_tables_overlaping_key_ranges_indexes.push_back(j);
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
            uint16_t ss_table_count = ss_table_controllers[index + 1].get_ss_tables_count();

            snprintf(&filename_data[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_SS_TABLE_FILE_NAME_DATA,  index + 1, ss_table_count);
            snprintf(&filename_index[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_SS_TABLE_FILE_NAME_INDEX,  index + 1, ss_table_count);
            snprintf(&filename_offset[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_SS_TABLE_FILE_NAME_OFFSET,  index + 1, ss_table_count);

            std::string level_n_1_dir(LSM_TREE_SS_TABLE_MAX_LENGTH, '\0');
            snprintf(&level_n_1_dir[0], LSM_TREE_SS_TABLE_MAX_LENGTH, LSM_TREE_LEVEL_DIR, index + 1);
            //level_n_1_dir.resize(strlen(level_n_1_dir.c_str())); // trim nulls

            if (!std::filesystem::exists(level_n_1_dir)) {
                std::filesystem::create_directories(level_n_1_dir);
            }

            std::filesystem::path filepath_data = std::filesystem::path(level_n_1_dir) / filename_data;
            std::filesystem::path filepath_index = std::filesystem::path(level_n_1_dir) / filename_index;
            std::filesystem::path filepath_offset = std::filesystem::path(level_n_1_dir) / filename_offset;

            
            SS_Table new_table = SS_Table(filepath_data, filepath_index, filepath_offset);

            // create keynator for the level n ss table
            SS_Table::Keynator keynator_level_n = ss_table_controllers[index][i].get_keynator();

            std::vector<SS_Table::Keynator> keynators;
            keynators.push_back(keynator_level_n);

            // create keynators for level n + 1 sstables
            for(uint16_t k  = 0; k < ss_tables_overlaping_key_ranges_indexes.size(); ++k){
                uint16_t table_index = ss_tables_overlaping_key_ranges_indexes[k];
                SS_Table::Keynator keynator = (ss_table_controllers[index + 1][table_index]).get_keynator();

                keynators.push_back(keynator);
            }

            // now the heap logic....
            // ss_table turi gebet pasakyti koks yra jos file index... (0 lygio merginimui)
            // pagal ideja mes i ss_table_overlapping kisam visas lenteles,kurios saugomos contrllerio sstables, kontrollerio sstables yra vektorius, tai viskas kisama i gaa --> kuo didesnis indexas, tuo naujesne lentele
            Min_heap heap = Min_heap();

            new_table.init_writing();

            // wont work for level 0 compactin!!!!
            do{
                for(uint16_t k = 0; k < keynators.size(); ++k){
                    if(k == 0){
                        heap.push(keynators[0].get_next_key(), ss_table_controllers[index].get_level(), ss_tables_overlaping_key_ranges_indexes[k], &(keynators[0]));
                    }
                    else{
                        heap.push(keynators[k].get_next_key(), ss_table_controllers[index + 1].get_level(), ss_tables_overlaping_key_ranges_indexes[k]);
                    }
                }

                // problem problem - kai man reikia data duomenu, as neglaiu kreiptis i keynatoriu nes as net nezinau kuris keynatorius ten buvo - as tik zinau 
                // kad mano top elemetas turi key, level and indexa bet tada kazkaip man reikia atsekti kurios lenteles keynatorius buvo ant virusaus???
                new_table.write(, )




            }while(!heap.empty());
        
    }
    }
    catch(std::exception e){
        return false;
    }
