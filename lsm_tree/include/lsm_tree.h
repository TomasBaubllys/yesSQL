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

// .sst_l[level_index]_[file_type]_[ss_table_count].bin
#define LSM_TREE_SS_TABLE_FILE_NAME_DATA ".sst_l%u_data_%lu.bin"
#define LSM_TREE_SS_TABLE_FILE_NAME_INDEX ".sst_l%u_index_%lu.bin"
#define LSM_TREE_SS_TABLE_FILE_NAME_OFFSET ".sst_l%u_offset_%lu.bin"
#define LSM_TREE_LEVEL_DIR "./data/val/Level_%u"
#define LSM_TREE_SS_TABLE_MAX_LENGTH 35

#define LSM_TREE_EMPTY_SS_TABLE_CONTROLLERS_ERR_MSG "LSM_Tree ss_table_controller vector is empty\n"
#define LSM_TREE_EMPTY_ENTRY_VECTOR_ERR_MSG "LSM_Tree empty entries vector, could not fill ss table\n"
#define LSM_TREE_GETRLIMIT_ERR_MSG "Failed getrlimit() call\n"
#define LSM_TREE_FAILED_COMPACTION_ERR_MSG "Failed to compact levels\n"

#define LSM_TREE_LEVEL_0_PATH "data/val/Level_0"



// Hello everybody
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

class LSM_Tree{
    private:
        Wal write_ahead_log;
        MemTable mem_table;
        // one contrller per each level
        std::vector<SS_Table_Controller> ss_table_controllers;
        uint16_t ratio;
        uint64_t max_files_count;

        // returns Max open files per process
        uint64_t get_max_file_limit();

    public:
        // default constructor initializes mem_table 
        LSM_Tree();

        // destructor deallocates mem_table
        ~LSM_Tree();

        // returns an Entry object with provided key
        Entry get(std::string key);

        // returns true if inserting a value was successful
        bool set(std::string key, std::string value);

        // returns a vector of all keys in current database
        std::set<Bits> get_keys();

        // returns a vector of all keys with provided prefix
        std::set<Bits> get_keys(std::string prefix);

        // returns all keys forward from the provided key
        std::set<Entry> get_ff(std::string key);

        // returns all keys backwards from the provided key
        std::set<Entry> get_fb(std::string key);

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

        // 
};

#endif
