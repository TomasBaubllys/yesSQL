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
        Entry e = (*it) -> get(key, found);
        if(found){
            return e;
        }
    }

    return Entry(Bits(placeholder_key), Bits(placeholder_value));

}

SS_Table_Controller:: SS_Table_Controller(uint16_t ratio, level_index_type current_level): current_name_counter(0){
        this -> level = current_level;
        sstables.reserve(SS_TABLE_CONTROLLER_MAX_VECTOR_SIZE);

        // 0 LEVEL -> 10^1 * 1MB = 10MB
        // 1 LEVEL -> 10^2 * 1MB = 100MB
        // 2 LEVEL -> 10^3 * 1MB = 1000MB
        // ...
        // 
        max_size = static_cast<uint64_t>(std::pow(ratio, level + 1)) * SS_TABLE_CONTROLLER_LEVEL_SIZE_BASE;
};

SS_Table_Controller:: ~SS_Table_Controller(){

};

uint64_t SS_Table_Controller:: calculate_size_bytes(){
    uint64_t size = 0;
    // for now only data files
    for(const SS_Table*& sst : sstables){
        size += std::filesystem::file_size(sst -> data_path());
        //size += std::filesystem::file_size(sst -> index_path());
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

void  SS_Table_Controller::delete_sstable(table_index_type index){
    const SS_Table *ss_table = this -> sstables.at(index);
    delete(ss_table);

    this -> sstables.erase(sstables.begin() + index);
    return;
}

uint64_t SS_Table_Controller::get_current_name_counter() const {
    return this -> current_name_counter;
}

bool SS_Table_Controller::empty() const {
    return this -> sstables.empty();
}

const SS_Table*& SS_Table_Controller::at(table_index_type index) {
    return this -> sstables.at(index);
}

const SS_Table*& SS_Table_Controller::front() {
    return this -> sstables.front();
}

double SS_Table_Controller::get_fill_ratio(){
    if(max_size == 0){
        return 0.0;
    }
    return static_cast<double>((calculate_size_bytes()) /  static_cast<double>(max_size));
}
