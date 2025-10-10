#ifndef SS_TABLE_CONTROLLER_H_INCLUDED
#define SS_TABLE_CONTROLLER_H_INCLUDED

#include <vector>
#include <memory>
#include <string>
#include "ss_table.h"

#define SS_TABLE_CONTROLLER_MAX_VECTOR_SIZE 5
#define SS_TABLE_CONTROLLER_MAX_SIZE 0xffffffffffffffff

class SS_Table_Controller{
    private:
        std::vector<SS_Table> sstables;
        uint16_t level;
        uint16_t ratio;
        // ideally not fixed for every level (the higher the level, more tables)
        uint64_t max_size;

    public:
        SS_Table_Controller(uint16_t ratio, uint16_t current_level);
        ~SS_Table_Controller();
        void add_sstable(SS_Table sstable);
        Entry get(Bits& key, bool& found);

        uint64_t calculate_size_bytes();

        bool is_over_limit();

        uint16_t get_ss_tables_count();

};


#endif // SS_TABLE_CONTROLLER_H_INCLUDED
