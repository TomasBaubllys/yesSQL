#ifndef SS_TABLE_CONTROLLER_H_INCLUDED
#define SS_TABLE_CONTROLLER_H_INCLUDED

#include <vector>
#include <memory>
#include <string>
#include "ss_table.h"

#define SS_TABLE_CONTROLLER_MAX_VECTOR_SIZE 5

class SS_Table_Controller{
    private:
        std::vector<SS_Table> sstables;
        uint16_t ss_table_count;
        // ideally not fixed for every level (the higher the level, more tables)
        uint16_t max_size;

    public:
        SS_Table_Controller(uint16_t ratio, uint16_t current_level);
        ~SS_Table_Controller();
        void add_sstable(SS_Table sstable);
        Entry* get(Bits& key, bool& found);

        uint64_t calculate_size_bytes();

};


#endif // SS_TABLE_CONTROLLER_H_INCLUDED
