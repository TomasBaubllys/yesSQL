#include "../include/lsm_tree.h"

LSM_Tree::LSM_Tree(){
    write_ahead_log = Wal();
    mem_table = Mem_Table();
    max_files_count = get_max_file_limit();


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

std::set<Bits> LSM_Tree::get_keys(){
    std::vector<Entry> entries = mem_table.dump_entries();
    std::set<Bits> keys;

    for(const Entry& entry : entries){
        keys.emplace(entry.get_key());
    }

    for(SS_Table_Controller& ss_table_controller : ss_table_controllers) {
        uint16_t sstable_count = ss_table_controller.get_ss_tables_count();

        for(uint16_t i = 0; i < sstable_count; ++i){
            std::vector<Bits> sstable_keys = ss_table_controller.at(i)->get_all_keys();
            for(const Bits& key : sstable_keys){
                keys.emplace(key);
            }
        }
    }

    return keys;
};

std::set<Bits> LSM_Tree::get_keys(std::string prefix){
    const uint32_t string_start_position = 0;

    std::vector<Entry> entries = mem_table.dump_entries();
    std::set<Bits> keys;

    for(const Entry& entry : entries){
        Bits key = entry.get_key();

        keys.emplace(entry.get_key());

        if(key.get_string().rfind("prefix", string_start_position)){
            keys.emplace(key);
        }
    }

    for(SS_Table_Controller& ss_table_controller : ss_table_controllers) {
        uint16_t sstable_count = ss_table_controller.get_ss_tables_count();

        for(uint16_t i = 0; i < sstable_count; ++i){
            std::vector<Bits> sstable_keys = ss_table_controller.at(i)->get_all_keys();
            for(const Bits& key : sstable_keys){
                if(key.get_string().rfind("prefix", string_start_position)){
                    keys.emplace(key);
                }
            }
        }
    }

    return keys;
};

std::pair<std::set<Entry>, std::string> LSM_Tree::get_ff(std::string _key){
    std::set<Entry> ff_entries;
    Bits key_bits(_key);
    Bits next_key(ENTRY_PLACEHOLDER_KEY);

    std::vector<Entry> mem_table_entries = mem_table.dump_entries();


    if(!mem_table_entries.empty() && !(mem_table_entries.back().get_key() < key_bits)){
        for(const Entry& mem_table_entry : mem_table_entries){
            forward_validate(ff_entries, mem_table_entry, true, key_bits);
        }
    }
    
    for(SS_Table_Controller& ss_table_controller : ss_table_controllers) {
        uint16_t sstable_count = ss_table_controller.get_ss_tables_count();

        for(uint16_t i = 0; i < sstable_count; ++i){
            const SS_Table* ss_table = ss_table_controller.at(i);

            std::pair<std::vector<Entry>, Bits> temp_pair = ss_table -> get_entries_key_larger_or_equal(key_bits, LSM_TREE_FORWARD_MAX_RETURN);

            ff_entries.insert(temp_pair.first.begin(), temp_pair.first.end());
            clean_forward_set(ff_entries, true, key_bits);
        }
    }

    if(!ff_entries.empty()){
        const Entry& last_entry = *ff_entries.rbegin();
        next_key = last_entry.get_key();
    }
    
    return std::make_pair(ff_entries, next_key.get_string());
};

std::pair<std::set<Entry>, std::string> LSM_Tree::get_fb(std::string _key){
    std::set<Entry> fb_entries;
    Bits key_bits = Bits(_key);
    Bits next_key(ENTRY_PLACEHOLDER_KEY);

    std::vector<Entry> mem_table_entries = mem_table.dump_entries();

    if(!mem_table_entries.empty() && !(mem_table_entries.front().get_key() > key_bits)){
        for(const Entry& mem_table_entry : mem_table_entries){
            forward_validate(fb_entries, mem_table_entry, false, key_bits);
        }
    }
    
    for(SS_Table_Controller& ss_table_controller : ss_table_controllers) {
        uint16_t sstable_count = ss_table_controller.get_ss_tables_count();

        for(uint16_t i = 0; i < sstable_count; ++i){
            const SS_Table* ss_table = ss_table_controller.at(i);

            std::pair<std::vector<Entry>, Bits> temp_pair = ss_table -> get_entries_key_smaller_or_equal(key_bits, LSM_TREE_FORWARD_MAX_RETURN);

            fb_entries.insert(temp_pair.first.begin(), temp_pair.first.end());
            clean_forward_set(fb_entries, false, key_bits);
        }
    }

    if(!fb_entries.empty()){
        const Entry& last_entry = *fb_entries.begin();
        next_key = last_entry.get_key();
    }
    return std::make_pair(fb_entries, next_key.get_string());
};

void LSM_Tree::forward_validate(std::set<Entry>& entries,const Entry& entry_to_append,const bool is_greater_operation,const Bits key_value){
    switch (is_greater_operation)
    {
    case true:
        if(entry_to_append.get_key() >= key_value){
            entries.emplace(entry_to_append);
        }
        break;
    case false:
        if(entry_to_append.get_key() <= key_value){
            entries.emplace(key_value);
        }
        break;
    default:
        break;
    }
};

void LSM_Tree::clean_forward_set(std::set<Entry>& set_to_clean,const bool is_greater_operation,const Bits key_value){

    if(set_to_clean.size() <= LSM_TREE_FORWARD_MAX_RETURN){
        return;
    };
    switch (is_greater_operation)
    {
    case true:
    {
        std::_Tree<std::_Tset_traits<Entry, std::less<Entry>, std::allocator<Entry>, false>>::iterator it = set_to_clean.begin();
        for(size_t i = 0; i < LSM_TREE_FORWARD_MAX_RETURN; ++i){
            ++it;
        }
        set_to_clean.erase(it, set_to_clean.end());
        break;
    }
    case false:
    {
        std::_Tree<std::_Tset_traits<Entry, std::less<Entry>, std::allocator<Entry>, false>>::iterator it = set_to_clean.begin();
        size_t elements_to_skip = set_to_clean.size() - LSM_TREE_FORWARD_MAX_RETURN;
        for(size_t i = 0; i < elements_to_skip; ++i){
            ++it;
        }
        set_to_clean.erase(set_to_clean.begin(), it);
        break;
    }
    default:
        break;
    }
}

bool LSM_Tree::remove(std::string key){

    try{
        Entry entry = get(key);
        std::ostringstream bytes = entry.get_ostream_bytes();

        if(!entry.is_deleted()){
            entry.set_tombstone(true);
        }
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


