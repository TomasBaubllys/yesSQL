#ifndef YSQL_LSMTREE_H_INCLUDED
#define YSQL_LSMTREE_H_INCLUDED

#include "bits.h"
#include "entry.h"
#include "wal.h"
#include "mem_table.h"
#include "ss_table_controller.h"
#include <thread>

// .sst_l[level_index]_[file_type]_[ss_table_count].bin
#define LSM_TREE_SS_TABLE_FILE_NAME_DATA ".sst_l%d_data_%d.bin"
#define LSM_TREE_SS_TABLE_FILE_NAME_INDEX ".sst_l%d_index_%d.bin"
#define LSM_TREE_SS_TABLE_FILE_NAME_OFFSET ".sst_l%d_offset_%d.bin"
<<<<<<< HEAD
=======
#define LSM_TREE_LEVEL_DIR "data/val/Level_%d"
>>>>>>> SStable
#define LSM_TREE_SS_TABLE_MAX_LENGTH 25


// Hello everybody
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

class LsmTree{
    private:
        Wal write_ahead_log;
        MemTable mem_table;
        // one contrller per each level
        std::vector<SS_Table_Controller> ss_table_controllers;
        uint16_t ratio;

    public:
        // default constructor initializes mem_table 
        LsmTree();

        // destructor deallocates mem_table
        ~LsmTree();

        // returns an Entry object with provided key
        Entry get(std::string key);

        // returns true if inserting a value was successful
        bool set(std::string key, std::string value);

        // returns a vector of all keys in current database
        std::vector<std::string> get_keys();

        // returns a vector of all keys with provided prefix
        std::vector<std::string> get_keys(std::string prefix);

        // returns all keys forward from the provided key
        std::vector<Entry> get_ff(std::string key);

        // returns all keys backwards from the provided key
        std::vector<Entry> get_fb(std::string key);

        // returns true if removing an entry with provided key was successful
        bool remove(std::string key);

        // flushes MemTable to SStable
        void flush_mem_table();

        // compact level[index] with level [index + 1]
<<<<<<< HEAD
        void compact_level(uint16_t index);
        
        // compact level0 with level1
        void compact_level_0();
=======
        bool compact_level(uint16_t index);
        
        // compact level0 with level1
        bool compact_level_0();
>>>>>>> SStable
};

#endif
