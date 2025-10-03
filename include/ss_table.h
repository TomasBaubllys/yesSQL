#ifndef SS_TABLE_H_INCLUDED
#define SS_TABLE_H_INCLUDED

#include "entry.h"
#include <filesystem>


class SS_Table{
    private:
        std::filesystem::path data_file;
        std::filesystem::path index_file;
        
        // ??? for level compaction to check for overlapping rnges
        uint16_t first_index;
        uint16_t last_index;

    public:
        std::filesystem::path& data_path();
        std::filesystem::path& index_path();
        SS_Table(const std::filesystem::path& data, const std::filesystem::path& index);
        ~SS_Table();

        // returns a specific entry at specific index
        Entry* read_entry_at_offset(uint64_t offset);
        // returns entry with a given key. if entry is not there, returns placeolder entry and found = false
        Entry* get(Bits& key, bool& found);

};


#endif // SS_TABLE_H_INCLUDED
