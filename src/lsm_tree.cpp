#include "../include/lsm_tree.h"
#include <filesystem>
#include <fstream>
#include <iostream>


LsmTree::LsmTree(){
}

LsmTree::~LsmTree(){
}
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

