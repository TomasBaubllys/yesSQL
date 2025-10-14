#include "../include/ss_table_controller.h"

void SS_Table_Controller::add_sstable(const SS_Table* sstable){
    this -> sstables.push_back(sstable);
    ++this -> current_name_counter;
}


Entry SS_Table_Controller::get(const Bits& key, bool& found) const{
    found = false;

    std::string placeholder_key(ENTRY_PLACEHOLDER_KEY);
	std::string placeholder_value(ENTRY_PLACEHOLDER_VALUE);

    for(std::vector<const SS_Table*>::const_reverse_iterator it = sstables.rbegin(); it != sstables.rend(); ++it){
        Entry e = (*it)->get(key, found);
        if(found){
            return e;
        }
    }

    return Entry(Bits(placeholder_key), Bits(placeholder_value));

}

SS_Table_Controller:: SS_Table_Controller(uint16_t ratio, uint16_t current_level): 
    sstables(SS_TABLE_CONTROLLER_MAX_VECTOR_SIZE), current_name_counter(0){
        this -> level = current_level;
        // to do later: use ratio to calculate size for every level
        max_size = SS_TABLE_CONTROLLER_MAX_SIZE;
   
};

SS_Table_Controller:: ~SS_Table_Controller(){

};

uint64_t SS_Table_Controller:: calculate_size_bytes(){
    uint64_t size = 0;

    // for now only data files
    for(const SS_Table*& sst : sstables){
        size += std::filesystem::file_size(sst -> data_path());
    }

    return size;
}

bool SS_Table_Controller:: is_over_limit(){
    return calculate_size_bytes() > max_size;
}

uint16_t SS_Table_Controller::get_ss_tables_count(){
    return sstables.size();
}



const SS_Table* SS_Table_Controller::operator[](std::size_t index){
    return sstables.at(index);
}

uint16_t SS_Table_Controller::get_level(){
    return this -> level;
}

void  SS_Table_Controller::delete_sstable(uint16_t index){
    this -> sstables.erase(sstables.begin() + index);
    return;
}

uint64_t SS_Table_Controller::get_current_name_counter() const {
    return this -> current_name_counter;
}


