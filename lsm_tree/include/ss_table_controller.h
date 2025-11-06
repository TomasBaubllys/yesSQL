#ifndef SS_TABLE_CONTROLLER_H_INCLUDED
#define SS_TABLE_CONTROLLER_H_INCLUDED

#include <vector>
#include <memory>
#include <string>
#include "ss_table.h"
#include "file_exception.h"
#include <cmath>

    #include <iostream>

#define SS_TABLE_CONTROLLER_MAX_VECTOR_SIZE 5
#define SS_TABLE_CONTROLLER_MAX_SIZE 0xffffffffffffffff
#define SS_TABLE_CONTROLLER_RATIO 10

// 1MB LIKE IN MEMTABLE
#define SS_TABLE_CONTROLLER_LEVEL_SIZE_BASE 1000000

class SS_Table_Controller{
    private:
        std::vector<const SS_Table*> sstables;
        level_index_type level;
        uint16_t ratio;
        // ideally not fixed for every level (the higher the level, more tables)
        uint64_t max_size;
        uint64_t current_name_counter;



    public:
        SS_Table_Controller(uint16_t ratio, level_index_type current_level);
        ~SS_Table_Controller();
        void add_sstable(const SS_Table* sstable);
        Entry get(const Bits& key, bool& found) const;

        uint64_t calculate_size_bytes();

        bool is_over_limit();

        uint16_t get_ss_tables_count();

        const SS_Table* operator[](std::size_t index);

        const SS_Table*& at(table_index_type index);
        
        const SS_Table*& front();

        uint16_t get_level();

        void delete_sstable(table_index_type index);

        uint64_t get_current_name_counter() const;

        bool empty() const; 

        double get_fill_ratio();
        /*
        for()
        std::vector<SS_table> check_overlapping_key_range(Bits firs_index, Bits last_index, SS_Table_Controller ss_controller);
        // SS_TABLE_VIDUJ

        bool cfkjdhjkf(first, last)
        */

};


#endif // SS_TABLE_CONTROLLER_H_INCLUDED
