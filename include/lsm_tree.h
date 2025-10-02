#ifndef YSQL_LSMTREE_H_INCLUDED
#define YSQL_LSMTREE_H_INCLUDED

#include "bits.h"
#include "entry.h"
#include "wal.h"
#include "mem_table.h"
#include <thread>
#include "ss_table_controller.h"
#include <thread>

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
        SS_Table_Controller ss_table_controller;
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
        vector<std::string> get_keys();

        // returns a vector of all keys with provided prefix
        vector<std::string> get_keys(std::string prefix);

        // returns all keys forward from the provided key
        vector<Entry> get_ff(std::string key);

        // returns all keys backwards from the provided key
        vector<Entry> get_fb(std::string key);

        // returns true if removing an entry with provided key was successful
        bool remove(std::string key);

        // flushes MemTable to SStable
        void flush_mem_table();
};

#endif
