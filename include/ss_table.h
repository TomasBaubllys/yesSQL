#ifndef SS_TABLE_H_INCLUDED
#define SS_TABLE_H_INCLUDED

#include "entry.h"
#include <filesystem>
#include <stdexcept>

#define SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG "Failed to open data file\n"
#define SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG "Failed to open index file\n"

class SS_Table{
    private:
        std::filesystem::path data_file;
        std::filesystem::path index_file;
        
        // ??? for level compaction to check for overlapping rnges
        Bits first_index;
        Bits last_index;

    public:
        std::filesystem::path data_path();
        std::filesystem::path index_path();
        SS_Table(const std::filesystem::path& data, const std::filesystem::path& index);
        ~SS_Table();

        // returns a specific entry at specific index
        Entry read_entry_at_offset(uint64_t offset);
        // returns entry with a given key. if entry is not there, returns placeolder entry and found = false
        Entry get(Bits& key, bool& found);

        // returns the first index in the ss_table
        Bits get_last_index();

        // returns the last index in the ss_table
        Bits get_first_index();

        // creates / fills the ss table with the given vector
        uint16_t fill_ss_table(std::vector<Entry>& entry_vector);
};


#endif // SS_TABLE_H_INCLUDED
