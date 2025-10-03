#include "../include/ss_table_controller.h"

void SS_Table_Controller::add_sstable(std::shared_ptr<SS_Table> sstable){
    sstables.push_back(sstable);
    ++ss_table_count;
}


Entry* SS_Table_Controller::get(Bits& key, bool& found){
    found = false;

    std::string placeholder_key(ENTRY_PLACEHOLDER_KEY);
	std::string placeholder_value(ENTRY_PLACEHOLDER_VALUE);

    for(auto it = sstables.rbegin(); it != sstables.rend(); ++it){
        Entry *e = (*it)->get(key, found);
        if(found){
            return e;
        }
    }

    return &Entry(Bits(placeholder_key), Bits(placeholder_value));

}

SS_Table_Controller:: SS_Table_Controller(): 
    sstables(SS_TABLE_CONTROLLER_MAX_VECTOR_SIZE), ss_table_count(0){
};

SS_Table_Controller:: ~SS_Table_Controller(){

};