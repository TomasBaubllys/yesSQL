#include "../include/ss_table_controller.h"

void SS_Table_Controller::add_sstable(SS_Table sstable){
    sstables.push_back(sstable);
    ++ss_table_count;
}


Entry SS_Table_Controller::get(Bits& key, bool& found){
    found = false;

    std::string placeholder_key(ENTRY_PLACEHOLDER_KEY);
	std::string placeholder_value(ENTRY_PLACEHOLDER_VALUE);

    for(auto it = sstables.rbegin(); it != sstables.rend(); ++it){
        Entry e = it -> get(key, found);
        if(found){
            return e;
        }
    }

    return Entry(Bits(placeholder_key), Bits(placeholder_value));

}

SS_Table_Controller:: SS_Table_Controller(uint16_t ratio, uint16_t current_level): 
    sstables(SS_TABLE_CONTROLLER_MAX_VECTOR_SIZE), ss_table_count(0){
        this -> level = current_level;
        // to do later: use ratio to calculate size for every level
        max_size = SS_TABLE_CONTROLLER_MAX_SIZE;
   
};

SS_Table_Controller:: ~SS_Table_Controller(){

};

uint64_t SS_Table_Controller:: calculate_size_bytes(){
    uint64_t size = 0;

    // for now only data files
    for(auto& sst : sstables){
        size += std::filesystem::file_size(sst.data_path());
    }

    return size;
}

bool SS_Table_Controller:: is_over_limit(){
    return calculate_size_bytes() > max_size;
}


