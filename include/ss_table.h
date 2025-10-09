#ifndef SS_TABLE_H_INCLUDED
#define SS_TABLE_H_INCLUDED

#include "entry.h"
#include <filesystem>
#include <stdexcept>

#define SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG "SS_table failed to open data file\n"
#define SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG "SS_table failed to open index file\n"
#define SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG "SS_table failed to open index offset file\n"
#define SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG "SS_table unexpected EOF encountered in index offset file\n"
#define SS_TABLE_UNEXPECTED_INDEX_EOF_MSG "SS_table unexpected EOF encountered in index file\n"
#define SS_TABLE_EMPTY_INDEX_FILE_MSG "SS_Table index file is empty\n"
#define SS_TABLE_EMPTY_INDEX_OFFSET_FILE_MSG "SS_Table index offset file is empty\n"
#define SS_TABLE_EMPTY_DATA_FILE_MSG "SS_Table data file is empty\n"
#define SS_TABLE_BAD_OFFSET_ERR_MSG "SS_Table incorrect data offset\n"
#define SS_TABLE_UNEXPECTED_DATA_EOF_MSG "SS_table unexpected EOF encountered in data file\n"

#define SS_TABLE_KEY_RECORD_SIZE

class SS_Table{
    private:
        std::filesystem::path data_file;
        std::filesystem::path index_file;
        std::filesystem::path index_offset_file;
        
        // ??? for level compaction to check for overlapping rnges
        Bits first_index;
        Bits last_index;

        // used for binary search (the maximum allowed key is 2^16 so if the table contains unique keys...)
        uint64_t record_count;

        // returns a stream from n bytes with a certain offset
        // the stringstream can be used directly to construct an entry after reading the key
        std::string read_stream_at_offset(uint64_t& offset);

    public:
        std::filesystem::path data_path();
        std::filesystem::path index_path();
        SS_Table(const std::filesystem::path& _data_file, const std::filesystem::path& _index_file, std::filesystem::path& _index_offset_file);
        ~SS_Table();

        // returns a specific entry at specific index
        // Entry read_entry_at_offset(uint64_t offset);
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
