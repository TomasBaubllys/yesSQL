#ifndef YSQL_MEM_TABLE_H_INCLUDED
#define YSQL_MEM_TABLE_H_INCLUDED

#include "avl_tree.h"
#include "entry.h"

#define MEM_TABLE_BYTES_MAX_SIZE 1000000

class MemTable{
    private:
        AVL_tree avl_tree;
        int entry_array_length;
        uint64_t total_mem_table_size;
    public:
        // default constructor
        MemTable();

        // destructor, clears avl tree
        ~MemTable();

        // returns the current size of the avl tree
        int get_entry_array_length();

        // returns the size of the mem_table in bytes
        uint64_t get_total_mem_table_size();

        // returns true if entry was inserted correctly
        bool insert_entry(Entry entry);

        // returns true if entry was removed correctly
        bool remove_find_entry(Bits key);

        // returns true if entry was removed correctly
        bool remove_entry(Entry& entry);

        // returns an Entry with parameter key
        Entry find(Bits key, bool& found);

        // returns true if total_mem_table_size has exceeded MEM_TABLE_BYTES_MAX_SIZE
        bool is_full();

        // get all entries from AVL tree
        std::vector<Entry> dump_entries();

    
};

#endif
