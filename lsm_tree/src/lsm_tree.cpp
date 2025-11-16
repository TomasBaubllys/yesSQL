#include "../include/lsm_tree.h"

LSM_Tree::LSM_Tree():
    write_ahead_log(),
    mem_table(write_ahead_log),
    max_files_count(get_max_file_limit())
{
    reconstruct_tree();
};

LSM_Tree::~LSM_Tree(){
};

Entry LSM_Tree::get(std::string key){
    Bits key_bits(key);

    bool is_found = false;

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
    return entry;
};

bool LSM_Tree::set(std::string key, std::string value){
    Bits key_bits(key);
    Bits value_bits(value);

    Entry entry(key_bits, value_bits);
    std::ostringstream bytes = entry.get_ostream_bytes();

    try{
        write_ahead_log.append_entry(bytes);
        mem_table.insert_entry(entry);
    }
    catch(const std::exception& e){
        std::cerr<< e.what() <<std::endl;
        return false;
    }

    if(mem_table.is_full()){
        try{
            flush_mem_table();
            this -> mem_table.make_empty();
            write_ahead_log.clear_entries();
        }
        catch(const std::exception& e){
            std::cerr << e.what() << std::endl;
            return false;
        }        
    }

    return true;

};

std::pair<std::set<Bits>, uint16_t> LSM_Tree::get_keys(uint16_t n, uint16_t skip_n){
    n += skip_n;

    std::vector<Entry> entries = mem_table.dump_entries();
    std::set<Bits> keys;

    for(const Entry& entry : entries){
        if(keys.size() >= n){break;}
        keys.emplace(entry.get_key());
    }

    for(SS_Table_Controller& ss_table_controller : ss_table_controllers) {
        if(keys.size() >= n){break;}
        uint16_t sstable_count = ss_table_controller.get_ss_tables_count();

        for(uint16_t i = sstable_count-1; i != UINT16_MAX; --i){
            if(keys.size() >= n){break;}
            std::vector<Bits> sstable_keys = ss_table_controller.at(i)->get_all_keys_alive();
            for(const Bits& key : sstable_keys){
                keys.emplace(key);
            }
        }
    }

    if(skip_n > 0 && keys.size() > skip_n){
        std::set<Bits>::iterator it = keys.begin();
        for(size_t i = 0; i < skip_n; ++i){
            ++it;
        }
        keys.erase(keys.begin(), it);
    }
    uint16_t next_skip = skip_n + keys.size();
    return std::make_pair(keys, next_skip);
};

std::pair<std::set<Bits>, uint16_t> LSM_Tree::get_keys(std::string prefix, uint16_t n, uint16_t skip_n){
    n += skip_n;
    const uint32_t string_start_position = 0;

    std::vector<Entry> entries = mem_table.dump_entries();
    std::set<Bits> keys;

    for(const Entry& entry : entries){
        if(keys.size() >= n){break;}
        Bits key = entry.get_key();

        if(key.get_string().rfind(prefix, string_start_position) == 0){
            keys.emplace(key);
        }
    }

    for(SS_Table_Controller& ss_table_controller : ss_table_controllers) {
        if(keys.size() >= n){break;}
        uint16_t sstable_count = ss_table_controller.get_ss_tables_count();

        for(uint16_t i = sstable_count-1; i != UINT16_MAX; --i){
            if(keys.size() >= n){break;}
            std::vector<Bits> sstable_keys = ss_table_controller.at(i)->get_all_keys_alive();
            for(const Bits& key : sstable_keys){
                if(key.get_string().rfind(prefix, string_start_position) == 0){
                    keys.emplace(key);
                }
            }
        }
    }

    if(skip_n > 0 && keys.size() > skip_n){
        std::set<Bits>::iterator it = keys.begin();
        for(size_t i = 0; i < skip_n; ++i){
            ++it;
        }
        keys.erase(keys.begin(), it);
    }
    uint16_t next_skip = skip_n + keys.size();
    return std::make_pair(keys, next_skip);
};

std::pair<std::set<Bits>, std::string> LSM_Tree::get_keys_cursor(std::string cursor, uint16_t n){
    Bits key_bits(cursor);
    std::set<Bits> keys;
    Bits next_key(ENTRY_PLACEHOLDER_KEY);

    std::vector<Entry> mem_table_entries = mem_table.dump_entries();

    if(!mem_table_entries.empty() && !(mem_table_entries.back().get_key() < key_bits)){
        for(const Entry& mem_table_entry : mem_table_entries){
            const Bits& entry_key = mem_table_entry.get_key();
            if(entry_key >= key_bits){
                keys.emplace(entry_key);
            }
        }
        if(!keys.empty()){
            next_key = clean_forward_set_keys(keys, n);
        }
    }
    mem_table_entries.clear();
    
    for(SS_Table_Controller& ss_table_controller : ss_table_controllers) {
        uint16_t sstable_count = ss_table_controller.get_ss_tables_count();

        for(uint16_t i = sstable_count-1; i != UINT16_MAX; --i){
            const SS_Table* ss_table = ss_table_controller.at(i);

            std::pair<std::vector<Bits>, Bits> temp_pair = ss_table -> get_n_next_keys_alive(key_bits, n);

            keys.insert(temp_pair.first.begin(), temp_pair.first.end());
            
            Bits temp_next_key = clean_forward_set_keys(keys, n);
            if(temp_next_key.get_string() != ENTRY_PLACEHOLDER_KEY){
                next_key = temp_next_key;
            }
        }
    }
    //
    //std::cout << next_key.get_string() << std::endl;
    return std::make_pair(keys, next_key.get_string());
};

std::pair<std::set<Bits>, std::string> LSM_Tree::get_keys_cursor_prefix(std::string prefix,std::string cursor, uint16_t n){

    Bits key_bits(cursor);
    const uint32_t string_start_position = 0;
    std::set<Bits> keys;
    Bits next_key(ENTRY_PLACEHOLDER_KEY);

    std::vector<Entry> mem_table_entries = mem_table.dump_entries();

    if(!mem_table_entries.empty() && !(mem_table_entries.back().get_key() < key_bits)){
        for(const Entry& mem_table_entry : mem_table_entries){
            const Bits& entry_key = mem_table_entry.get_key();
            if(entry_key >= key_bits && entry_key.get_string().rfind(prefix, string_start_position) == 0){
                keys.emplace(entry_key);
            }
        }
        if(!keys.empty()){
            next_key = clean_forward_set_keys(keys, n);
        }
    }
    mem_table_entries.clear();
    
    for(SS_Table_Controller& ss_table_controller : ss_table_controllers) {
        uint16_t sstable_count = ss_table_controller.get_ss_tables_count();

        for(uint16_t i = sstable_count-1; i != UINT16_MAX; --i){
            const SS_Table* ss_table = ss_table_controller.at(i);

            std::vector<Bits> no_prefix_keys = ss_table -> get_all_keys_alive();

            for(const Bits& key : no_prefix_keys){
                if(key >= key_bits && key.get_string().rfind(prefix, string_start_position) == 0){
                    keys.emplace(key);
                }
            }

            Bits temp_next_key = clean_forward_set_keys(keys, n);
            if(temp_next_key.get_string() != ENTRY_PLACEHOLDER_KEY){
                next_key = temp_next_key;
            }
        }
    }
    return std::make_pair(keys, next_key.get_string());
};

std::pair<std::set<Entry>, std::string> LSM_Tree::get_ff(std::string _key, uint16_t n){
    std::set<Entry> ff_entries;
    Bits key_bits(_key);
    Bits next_key(ENTRY_PLACEHOLDER_KEY);

    std::vector<Entry> mem_table_entries = mem_table.dump_entries();


    if(!mem_table_entries.empty() && !(mem_table_entries.back().get_key() < key_bits)){
        for(const Entry& mem_table_entry : mem_table_entries){
            forward_validate(ff_entries, mem_table_entry, true, key_bits);
        }
        if(!ff_entries.empty()){
            next_key = clean_forward_set(ff_entries, true, n);
        }
    }
    
    
    for(SS_Table_Controller& ss_table_controller : ss_table_controllers) {
        uint16_t sstable_count = ss_table_controller.get_ss_tables_count();

        for(uint16_t i = sstable_count-1; i != UINT16_MAX; --i){
            const SS_Table* ss_table = ss_table_controller.at(i);

            std::pair<std::vector<Entry>, Bits> temp_pair = ss_table -> get_entries_key_larger_or_equal_alive(key_bits, n);

            ff_entries.insert(temp_pair.first.begin(), temp_pair.first.end());
            
            Bits temp_next_key = clean_forward_set(ff_entries, true, n);

            if(temp_next_key.get_string() != ENTRY_PLACEHOLDER_KEY){
                next_key = temp_next_key;
            }
        }
    }
    
    return std::make_pair(ff_entries, next_key.get_string());
};

std::pair<std::set<Entry>, std::string> LSM_Tree::get_fb(std::string _key, uint16_t n){
    std::set<Entry> fb_entries;
    Bits key_bits = Bits(_key);
    Bits next_key(ENTRY_PLACEHOLDER_KEY);

    std::vector<Entry> mem_table_entries = mem_table.dump_entries();

    if(!mem_table_entries.empty() && !(mem_table_entries.front().get_key() > key_bits)){
        for(const Entry& mem_table_entry : mem_table_entries){
            forward_validate(fb_entries, mem_table_entry, false, key_bits);
        }
        if(!fb_entries.empty()){
            next_key = clean_forward_set(fb_entries, false, n);
        }
    }
    
    for(SS_Table_Controller& ss_table_controller : ss_table_controllers) {
        uint16_t sstable_count = ss_table_controller.get_ss_tables_count();

        for(uint16_t i = sstable_count-1; i != UINT16_MAX; --i){
            const SS_Table* ss_table = ss_table_controller.at(i);

            std::pair<std::vector<Entry>, Bits> temp_pair = ss_table -> get_entries_key_smaller_or_equal_alive(key_bits, n);

            fb_entries.insert(temp_pair.first.begin(), temp_pair.first.end());

            Bits temp_next_key = clean_forward_set(fb_entries, false, n);
            if(temp_next_key.get_string() != ENTRY_PLACEHOLDER_KEY){
                next_key = temp_next_key;
            }
        }
    }

    return std::make_pair(fb_entries, next_key.get_string());
};

void LSM_Tree::forward_validate(std::set<Entry>& entries,const Entry& entry_to_append,const bool is_greater_operation,const Bits key_value){
    if(is_greater_operation){
        if(entry_to_append.get_key() >= key_value){
            entries.emplace(entry_to_append);
        }
    }else{
        if(entry_to_append.get_key() <= key_value){
            entries.emplace(entry_to_append);
        }
    }
};

Bits LSM_Tree::clean_forward_set(std::set<Entry>& set_to_clean,const bool is_greater_operation, uint16_t n){
    Bits last_key(ENTRY_PLACEHOLDER_KEY);
    if(set_to_clean.size() <= n){
        return last_key;
    };
    if(is_greater_operation){
        std::set<Entry>::iterator it = set_to_clean.begin();
        for(uint16_t i = 0; i < n; ++i){
            ++it;
        }
        last_key = it -> get_key();
        set_to_clean.erase(it, set_to_clean.end());
    }
    else{
        std::set<Entry>::iterator it = set_to_clean.begin();
        uint16_t elements_to_skip = set_to_clean.size() - n;
        for(uint16_t i = 0; i < elements_to_skip; ++i){
            ++it;
        }
        std::set<Entry>::iterator prev_it = it;
        --prev_it;
        last_key = prev_it -> get_key();

        set_to_clean.erase(set_to_clean.begin(), it);
    }
    return last_key;
};
Bits LSM_Tree::clean_forward_set_keys(std::set<Bits>& set_to_clean, uint16_t n){
    Bits last_key(ENTRY_PLACEHOLDER_KEY);
    if(set_to_clean.size() <= n){
        return last_key;
    };
    std::set<Bits>::iterator it = set_to_clean.begin();
    for(uint16_t i = 0; i < n; ++i){
        ++it;
    }

    std::set<Bits>::iterator last_kept_it = it;
    --last_kept_it;

    last_key = *last_kept_it;


    //last_key = *it;
    set_to_clean.erase(it, set_to_clean.end());
    return last_key;
};

bool LSM_Tree::remove(std::string key){

    try{
        //Entry entry = get(key);
        Bits b_key(key);
        Bits v_val(ENTRY_PLACEHOLDER_VALUE);
        Entry entry(b_key, v_val);
        entry.set_tombstone(ENTRY_TOMBSTONE_ON);
        std::ostringstream bytes = entry.get_ostream_bytes();

        /*if(!entry.is_deleted()){
            entry.set_tombstone(true);
        }*/
        write_ahead_log.append_entry(bytes);
        mem_table.insert_entry(entry);
    }
    catch(const std::exception& e){
        std::cerr<< e.what() <<std::endl;
        return false;
    }

    if(mem_table.is_full()){
        try{
            flush_mem_table();
            mem_table.make_empty();
            write_ahead_log.clear_entries();
        }
        catch(const std::exception& e){
            std::cerr<< e.what() <<std::endl;
            return false;
        }        
    }

    return true;
};


// LSM_Tree
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

void LSM_Tree::flush_mem_table(){

    // check for compaction
    if(!ss_table_controllers.empty()){
        // check if levels has not reached a limit of file count 
        for(uint16_t i = 0; i < ss_table_controllers.size(); ++i){
            if((ss_table_controllers.at(i).get_ss_tables_count() * 3) > (this -> max_files_count / 2)){
                if(!this -> compact_level(i)){
                    throw std::runtime_error(LSM_TREE_FAILED_COMPACTION_ERR_MSG);
                }
            }
        }
        
    }

    // compact level that is the most filled
    std::pair<uint16_t, double> max_pair = this -> get_max_fill_ratio();
    if(max_pair.second >= 1.0){
        if(!this -> compact_level(max_pair.first)){
            throw std::runtime_error(LSM_TREE_FAILED_COMPACTION_ERR_MSG);
        }
    }

    std::vector<Entry> entries = mem_table.dump_entries();

    // move to define
    std::filesystem::path level0_dir = LSM_TREE_LEVEL_0_PATH;
    if (!std::filesystem::exists(level0_dir)) {
        std::filesystem::create_directories(level0_dir);
    }

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
        throw std::runtime_error(LSM_TREE_EMPTY_ENTRY_VECTOR_ERR_MSG);
    }

    if(ss_table_controllers.size() == 0){
        ss_table_controllers.emplace_back(SS_TABLE_CONTROLLER_RATIO, ss_table_controllers.size());
    }

    // ss table controller level 0 add a table
    ss_table_controllers.at(0).add_sstable(ss_table);

    return;
 }

bool LSM_Tree::compact_level(level_index_type index) {
    // check for overflow
    if(index + 1 < index) {
        return false;
    }

    if(this -> ss_table_controllers.empty()){
        throw std::runtime_error(LSM_TREE_EMPTY_SS_TABLE_CONTROLLERS_ERR_MSG);
    }

    if(index >= ss_table_controllers.size()) {
        return false;
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
                    overlapping_key_ranges.push_back(std::make_pair(0, i));
                }
            }

            // now if the next level exist check for overlapping keys there
            if(ss_table_controllers.size() > (uint64_t)(index + 1)) {
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

            uint64_t ss_table_count = ss_table_controllers.size() > (uint64_t)(index + 1)? (ss_table_controllers.at(index + 1).get_current_name_counter()) : 0;

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
            if(ss_table_controllers.size() <= (uint64_t)(index + 1)) {
                ss_table_controllers.emplace_back(SS_TABLE_CONTROLLER_RATIO, ss_table_controllers.size());
            }

            ss_table_controllers.at(index + 1).add_sstable(new_table);
        }

    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return false;
    }

    return true;
}

std::vector<std::pair<uint16_t, double>> LSM_Tree::get_fill_ratios(){
    std::vector<std::pair<uint16_t, double>> ratios;

    for(uint16_t i = 0; i < this -> ss_table_controllers.size(); ++i){
        ratios.push_back(std::make_pair(i, ss_table_controllers.at(i).get_fill_ratio()));
    }

    // sort in a way that the biggest ratios are last
    std::sort(ratios.begin(), ratios.end(),
        [](const auto& a, const auto& b){
            return a.second > b.second;
    });

    return ratios;
}

std::pair<uint16_t, double> LSM_Tree::get_max_fill_ratio(){
    if(ss_table_controllers.empty()){
        return std::make_pair(0, 0.0);
    }

    std::pair<uint16_t, double> max_pair = std::make_pair(0, ss_table_controllers.front().get_fill_ratio());

    for(uint16_t i = 1; i < this -> ss_table_controllers.size(); ++i){
        double ratio = ss_table_controllers.at(i).get_fill_ratio();
        if(ratio > max_pair.second){
            max_pair = std::make_pair(i, ratio);
        }
    }

    return max_pair;
}


uint64_t LSM_Tree::get_max_file_limit(){
    #ifdef __linux__
        struct rlimit limit;
        if(getrlimit(RLIMIT_NOFILE, &limit) == 0){
            return limit.rlim_cur;
        }
        else{
            throw std::runtime_error(LSM_TREE_GETRLIMIT_ERR_MSG);
        }
    #endif
    #ifdef _WIN32
        int limit = _getmaxstdio();
        if(limit == -1){
            throw std::runtime_error(LSM_TREE_GETRLIMIT_ERR_MSG);
        }
        return static_cast<uint64_t>(limit);
    #endif
}


bool LSM_Tree::reconstruct_tree(){
    try{

        
        std::filesystem::path ss_level_path = LSM_TREE_SS_LEVEL_PATH;
        if(!std::filesystem::exists(ss_level_path)){
            return true;
        }
        
        std::regex ss_table_pattern(R"(\.sst_l(\d+)_(data|index|offset)_(\d+)\.bin)");
        std::regex folder_pattern(R"(Level_(\d+))");
        // match[1] -> level number
        // match[2] -> file type
        // match[3] -> file ID

        std::vector<std::pair<uint8_t, uint16_t>> corrupted_indexes;
        std::vector<std::pair<uint8_t, std::filesystem::path>> levels;

        for(const std::filesystem::directory_entry& level_path : std::filesystem::directory_iterator(ss_level_path)){
            if(level_path.is_directory()){
                std::string level_folder = level_path.path().filename().string();
                std::smatch match;
                if(std::regex_match(level_folder, match, folder_pattern)){
                    uint8_t level = static_cast<uint8_t>(std::stoi(match[1]));
                    levels.emplace_back(level, level_path);

                }
            }
        }

        std::sort(levels.begin(), levels.end(),
                  [](const auto& a, const auto& b) {
                      return a.first < b.first;
                  });
        

        for(std::vector<std::pair<uint8_t, std::filesystem::path>>::const_iterator it = levels.begin(); it != levels.end(); ++it){
                ss_table_controllers.emplace_back(SS_TABLE_CONTROLLER_RATIO, ss_table_controllers.size());

                std::map<uint16_t, LSM_Tree::SS_Table_Files> table_map;

                std::vector<std::filesystem::path> ss_table_data_files;
                std::vector<std::filesystem::path> ss_table_index_files;
                std::vector<std::filesystem::path> ss_table_offset_files;
                
                for(const std::filesystem::directory_entry& ss_table_file : std::filesystem::directory_iterator(it -> second )){
                    std::string filename = ss_table_file.path().filename().string();
                    std::smatch match;

                    if(std::regex_match(filename, match, ss_table_pattern)){
                        std::string type = match[2];
                        uint16_t id = static_cast<uint16_t>(std::stoi(match[3]));

                        SS_Table_Files& set = table_map[id];

                        if(type == LSM_TREE_TYPE_DATA)
                            set.data_file = ss_table_file.path();
                        else if (type == LSM_TREE_TYPE_INDEX)
                            set.index_file = ss_table_file.path();
                        else if (type == LSM_TREE_TYPE_OFFSET)
                            set.offset_file = ss_table_file.path();
                    }
                }

                for(std::pair<const uint16_t, SS_Table_Files>& entry : table_map){
                    // uint16_t id = entry.first;
                    SS_Table_Files& set = entry.second;

                    if(!set.data_file.empty() && !set.index_file.empty() && !set.offset_file.empty()){
                        SS_Table* new_table = new SS_Table(entry.second.data_file, entry.second.index_file, entry.second.offset_file);
                        new_table -> reconstruct_ss_table();
                        ss_table_controllers.at(it -> first).add_sstable(new_table);
                        

                        //std::cout << entry.second.data_file.string() << std::endl;
                        //std::cout << entry.second.index_file.string() << std::endl;
                        //std::cout << entry.second.offset_file.string() << std::endl;

                        
                    }
                    else{

                        if (!std::filesystem::exists(LSM_TREE_CORRUPT_FILES_PATH)) {
                            std::filesystem::create_directories(LSM_TREE_CORRUPT_FILES_PATH);
                        }

                        // move the entry.second.data_file, entry.second.index_file and entry.second.offset_file to the LMS_CORRUPT_FILES_PATH folder

                        if(std::filesystem::exists(set.data_file)){
                            std::filesystem::path dest = LSM_TREE_CORRUPT_FILES_PATH / set.data_file.filename();
                            std::filesystem::rename(set.data_file, dest);                         
                        }

                        if(std::filesystem::exists(set.index_file)){
                            std::filesystem::path dest = LSM_TREE_CORRUPT_FILES_PATH / set.index_file.filename();
                            std::filesystem::rename(set.index_file, dest);
                        }


                        if(std::filesystem::exists(set.offset_file)){
                            std::filesystem::path dest = LSM_TREE_CORRUPT_FILES_PATH / set.offset_file.filename();
                            std::filesystem::rename(set.offset_file, dest);
                        }
                    }

                }

            }

            return true;

        }

    catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Filesystem error: " << e.what() << std::endl;
        return false;
    }
    catch(const std::exception& e){
        std::cerr << e.what() << std::endl;
        return false;
    }

    return true;
}


