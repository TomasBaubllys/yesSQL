#ifndef YSQL_LSM_Tree_H_INCLUDED
#define YSQL_LSM_Tree_H_INCLUDED

#ifdef __linux__
    #include <sys/resource.h>
#endif

#include "bits.h"
#include "entry.h"
#include "wal.h"
#include "mem_table.h"
#include "ss_table_controller.h"
#include <thread>
#include <limits>
#include <algorithm>

#include "../include/min_heap.h"
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <cstring>
#include <set>
#include <regex>
#include <map>

// .sst_l[level_index]_[file_type]_[ss_table_count].bin
#define LSM_TREE_SS_TABLE_FILE_NAME_DATA ".sst_l%u_data_%lu.bin"
#define LSM_TREE_SS_TABLE_FILE_NAME_INDEX ".sst_l%u_index_%lu.bin"
#define LSM_TREE_SS_TABLE_FILE_NAME_OFFSET ".sst_l%u_offset_%lu.bin"
#define LSM_TREE_LEVEL_DIR "./data/val/Level_%u"
#define LSM_TREE_SS_TABLE_MAX_LENGTH 35
#define LSM_TREE_SS_LEVEL_PATH "./data/val/"
#define LSM_TREE_TYPE_DATA "data"
#define LSM_TREE_TYPE_INDEX "index"
#define LSM_TREE_TYPE_OFFSET "offset"


#define LSM_TREE_EMPTY_SS_TABLE_CONTROLLERS_ERR_MSG "LSM_Tree ss_table_controller vector is empty\n"
#define LSM_TREE_EMPTY_ENTRY_VECTOR_ERR_MSG "LSM_Tree empty entries vector, could not fill ss table\n"
#define LSM_TREE_GETRLIMIT_ERR_MSG "Failed getrlimit() call\n"
#define LSM_TREE_FAILED_COMPACTION_ERR_MSG "Failed to compact levels\n"

#define LSM_TREE_LEVEL_0_PATH "./data/val/Level_0"
#define LSM_TREE_CORRUPT_FILES_PATH "./data/val/corrupted"

class LSM_Tree{
    private:
        Wal write_ahead_log;
        Mem_Table mem_table;
        // one contrller per each level
        std::vector<SS_Table_Controller> ss_table_controllers;
        uint16_t ratio;
        uint64_t max_files_count;

        // returns Max open files per process
        uint64_t get_max_file_limit();

        struct SS_Table_Files{
            std::filesystem::path data_file;
            std::filesystem::path index_file;
            std::filesystem::path offset_file;
        };

        // performs validation with given set and inserts if operation matches
        void forward_validate(std::set<Entry>& entries, const Entry& entry_to_append, bool is_greater_operation, const Bits key_value);

        // performs cleaning operation on the given set to keep only the n number of entries
        Bits clean_forward_set(std::set<Entry>& set_to_clean,const bool is_greater_operation, uint16_t n);

        // performs cleaning operation on the given set to keep only the n number of entries
        Bits clean_forward_set_keys(std::set<Bits>& set_to_clean, uint16_t n);

    public:
        // default constructor initializes mem_table 
        LSM_Tree();

        // destructor deallocates mem_table
        ~LSM_Tree();

        // returns an Entry object with provided key
        Entry get(std::string key);

        // returns true if inserting a value was successful
        bool set(std::string key, std::string value);

        // Returns a pair containing a set of up to n keys and the skip value for the next call
        // @param n - number of keys to return
        // @param skip_n - number of keys to skip from the beginning (default: 0)
        // @return pair of: (1) set of keys, (2) skip value to use for next pagination call
        // @note For pagination: use the returned uint16_t as skip_n in the next call to get the next batch
        std::pair<std::set<Bits>, uint16_t> get_keys(uint16_t n, uint16_t skip_n = 0);

        // Returns a pair containing a set of up to n keys with specified prefix and the skip value for the next call
        // @param n - number of keys to return
        // @param skip_n - number of keys to skip from the beginning (default: 0)
        // @return pair of: (1) set of keys, (2) skip value to use for next pagination call
        // @note For pagination: use the returned uint16_t as skip_n in the next call to get the next batch
        std::pair<std::set<Bits>, uint16_t> get_keys(std::string prefix, uint16_t n, uint16_t skip_n = 0);

        std::pair<std::set<Bits>, std::string> get_keys_cursor(std::string cursor, uint16_t n);

        std::pair<std::set<Bits>, std::string> get_keys_cursor_prefix(std::string prefix,std::string cursor, uint16_t n);

        // Returns up to n entries with keys greater than or equal to the given key (forward pagination)
        // @param _key - the starting key (inclusive) for the search
        // @param n - maximum number of entries to return
        // @return pair of: (1) set of entries with keys >= _key, (2) next key for pagination
        // @note For pagination: use the returned string as _key in the next call to get the next batch
        // @note The next key is the first key that was excluded (boundary key), or empty if no more entries
        // @note Searches both mem_table and all SS_Tables, with newer entries taking precedence
        std::pair<std::set<Entry>, std::string> get_ff(std::string _key, uint16_t n);

        // Returns up to n entries with keys less than or equal to the given key (backward pagination)
        // @param _key - the starting key (inclusive) for the search
        // @param n - maximum number of entries to return
        // @return pair of: (1) set of entries with keys <= _key, (2) next key for pagination
        // @note For pagination: use the returned string as _key in the next call to get the previous batch
        // @note The next key is the last key that was excluded (boundary key), or empty if no more entries
        // @note Searches both mem_table and all SS_Tables, with newer entries taking precedence
        std::pair<std::set<Entry>, std::string> get_fb(std::string _key, uint16_t n);

        // returns true if removing an entry with provided key was successful
        bool remove(std::string key);

        // flushes MemTable to SStable
        void flush_mem_table();

        // compact level[index] with level [index + 1]
        bool compact_level(level_index_type index);

        // get fill ratios of all levels. returns a sorted vector of pair <level, ratio>, where biggest ratios are in front
        std::vector<std::pair<uint16_t, double>> get_fill_ratios();

        // returns a pair <level, ratio> of the highest fill ratio on whole LSM tree
        std::pair<uint16_t, double> get_max_fill_ratio();

        // reconstructs LSM tree in case of a crash
        bool reconstruct_tree();
};

#endif
