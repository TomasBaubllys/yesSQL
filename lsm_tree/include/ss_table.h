#ifndef SS_TABLE_H_INCLUDED
#define SS_TABLE_H_INCLUDED

#include "entry.h"
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>

#define SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG "SS_Table failed to open data file\n"
#define SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG "SS_Table failed to open index file\n"
#define SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG "SS_Table failed to open index offset file\n"
#define SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG "SS_Table unexpected EOF encountered in index offset file\n"
#define SS_TABLE_UNEXPECTED_INDEX_EOF_MSG "SS_Table unexpected EOF encountered in index file\n"
#define SS_TABLE_EMPTY_INDEX_FILE_MSG "SS_Table index file is empty\n"
#define SS_TABLE_EMPTY_INDEX_OFFSET_FILE_MSG "SS_Table index offset file is empty\n"
#define SS_TABLE_EMPTY_DATA_FILE_MSG "SS_Table data file is empty\n"
#define SS_TABLE_BAD_OFFSET_ERR_MSG "SS_Table incorrect data offset\n"
#define SS_TABLE_UNEXPECTED_DATA_EOF_MSG "SS_Table unexpected EOF encountered in data file\n"

#define SS_TABLE_FAILED_DATA_WRITE_ERR_MSG "SS_Table failed to write to the data file\n"
#define SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG "SS_Table failed to write to the index file\n"
#define SS_TABLE_FAILED_INDEX_OFFSET_WRITE_ERR_MSG "SS_Table failed to write to the index offset file\n"

#define SS_TABLE_KEY_OFFSET_RECORD_SIZE sizeof(uint64_t)

#define SS_TABLE_KEYNATOR_FAILED_OPEN_INDEX_FILE_ERR_MSG "Keynator failed to open index file\n"
#define SS_TABLE_KEYNATOR_FAILED_OPEN_INDEX_OFFSET_FILE_ERR_MSG "Keynator failed to open index offset file\n"
#define SS_TABLE_KEYNATOR_FAILED_OPEN_DATA_FILE_ERR_MSG "Keynator failed to open data file\n";

#define SS_TABLE_PARTIAL_READ_ERR_MSG "SS_Table partial read occured\n"
#define SS_TABLE_KEYS_LARGER_THAN_FAILED_ERR_MSG "SS_Table failed to find keys larger\n"
#define SS_TABLE_KEYS_SMALLER_THAN_FAILED_ERR_MSG "SS_Table failed to find keys smaller\n"

#define SS_TABLE_INDEX_SEEK0_FAILED_ERR_MSG "SS_Table failed to seek to the index file beggining\n"
#define SS_TABLE_INDEX_OFFSET_SEEK0_FAILED_ERR_MSG "SS_Table failed to seek to the index offset file beggining\n"

#define SS_TABLE_LEVEL_SIZE_BASE 1000000

using table_index_type = uint16_t;
using level_index_type = uint16_t;

enum SS_Table_Stop_Writting_Error_Code {
    DATA_CLOSE_FAILED = 1,
    INDEX_CLOSE_FAILED,
    INDEX_OFFSET_CLOSE_FAILED
};

#define SS_TABLE_BINARY_SEARCH_UNKNOWN_TYPE_ERR_MSG "Unknown argument for SS_Table binary search\n"

enum SS_Table_Binary_Search_Type : uint8_t {
    SS_TABLE_LARGER_OR_EQUAL,
    SS_TABLE_SMALLER_OR_EQUAL
};

enum SS_Table_Entry_Filter : uint8_t {
    SS_TABLE_FILTER_ALL_ENTRIES,
    SS_TABLE_FILTER_ALIVE_ENTRIES
};

class SS_Table{
    private:
        const std::filesystem::path data_file;
        const std::filesystem::path index_file;
        const std::filesystem::path index_offset_file;
        
        // ??? for level compaction to check for overlapping rnges
        Bits first_index;
        Bits last_index;

        // used for binary search (the maximum allowed key is 2^16 so if the table contains unique keys...)
        uint64_t record_count;

        uint64_t data_file_size;
        uint64_t index_file_size;
        uint64_t index_offset_file_size;

        std::ofstream data_ofstream;
        std::ofstream index_ofstream;
        std::ofstream index_offset_ofstream;

        // returns a stream from n bytes with a certain offset
        // the stringstream can be used directly to construct an entry after reading the key
        std::string read_stream_at_offset(uint64_t& offset) const;

        // THROWS
        // returns the key index of the key that is larger or smaller than the key depending on the type than the target key
        // if ifstreams are not open, opens them
        uint64_t binary_search_nearest(std::ifstream& index_ifstream, std::ifstream& offset_ifstream, const Bits& target_key, SS_Table_Binary_Search_Type search_type) const;

        //THROWS
        // returns UP TO count entries, if less entries are returned (because n entries dont exist in this table, the next key is a placeholder)
        std::pair<std::vector<Entry>, Bits> get_entries_key_smaller_or_equal(const Bits& target_key, SS_Table_Entry_Filter entry_filter, uint32_t count) const;

        // returns UP TO count entries, if less entries are returned (because n entries dont exist in this table, the next key is a placeholder)
        std::pair<std::vector<Entry>, Bits> get_entries_key_larger_or_equal(const Bits& target_key, SS_Table_Entry_Filter entry_filter, uint32_t count) const;
        
        // returns UP TO count entries, if less entries are returned (because n entries dont exist in this table, the next key is a placeholder)
        std::pair<std::vector<Entry>, Bits> get_n_entries(SS_Table_Entry_Filter entry_filter, uint32_t count) const;

        std::vector<Bits> get_all_keys(SS_Table_Entry_Filter key_filter) const;

        std::pair<std::vector<Bits>, Bits> get_n_next_keys(const Bits& target_key, SS_Table_Entry_Filter entry_filter, uint32_t count) const;

    public:
        std::filesystem::path data_path() const;
        std::filesystem::path index_path() const;
        std::filesystem::path offset_path() const;

        SS_Table(const std::filesystem::path& _data_file, const std::filesystem::path& _index_file, std::filesystem::path& _index_offset_file);

        // no copying allowed
        SS_Table(const SS_Table&) = delete;
        SS_Table& operator=(const SS_Table&) = delete;

        // moving is allowed
        SS_Table(SS_Table&&) = default;
        SS_Table& operator=(SS_Table&&) = default;

        ~SS_Table();

        // returns a specific entry at specific index
        // Entry read_entry_at_offset(uint64_t offset);
        // returns entry with a given key. if entry is not there, returns placeolder entry and found = false
        Entry get(const Bits& key, bool& found) const;

        // returns the first index in the ss_table
        Bits get_last_index() const;

        // returns the last index in the ss_table
        Bits get_first_index() const;

        // appends a new vector to the end of the ss table
        // does NOT check if a vector is sorted or if it is correct
        // use with caution
        uint64_t append(const std::vector<Entry>& entry_vector);

        // creates / fills the ss table with the given vector
        // overwrites the current files
        uint64_t fill_ss_table(const std::vector<Entry>& entry_vector);

        void reconstruct_ss_table();


        class Keynator {
            private:
                std::ifstream index_stream;
                std::ifstream index_offset_stream;

                const std::filesystem::path data_file;

                // THROWS
                Keynator(const std::filesystem::path& index_file, const std::filesystem::path& index_offset_file, const std::filesystem::path& data_file, uint64_t record_count);

                uint64_t current_data_offset;
                uint64_t records_read;
                uint64_t record_count;
            public:
                ~Keynator();
                // THROWS
                Bits get_next_key();
                // THROWS
                std::string get_current_data_string();

                // allow move
                Keynator(Keynator&&) = default;
                Keynator& operator=(Keynator&&) = default;

                // delete old on copy
                Keynator(const Keynator&) = delete;
                Keynator& operator=(const Keynator&) = delete;

                friend class SS_Table;
        };

        // THROWS
        // @brief returns Keynator type for key value merging logic
        Keynator get_keynator() const;

        // THROWS
        // @brief initializes internal files for writing
        // allow the function write(const Bits&, const string&) to be called
        int8_t init_writing();

        // THROWS
        // @brief writes the key and data string to internal files
        // can only be called once init_writing() has been called, it is up to the caller to make sure that writen data is the correct order
        // throws an error if init_write was not called
        // @note data_string must be exactly the same format as it was received from Keynator
        int8_t write(const Bits& key, const std::string& data_string);

        // @brief closes internal files
        // must be called after write() was called and the user is done writing data
        // if first bit is set - data file closing failed
        // if second bit is set - index file closing failed
        // if third bit is set - index offset file closing failed
        // @return 0 - if success  > 0 if failed to close the files 
        int8_t stop_writing();

        // @brief returns true if provided indexes (keys) overlap with this tables indexes
        bool overlap(const Bits& first_index, const Bits& last_index) const;

        // THROWS
        // @brief returns all the keys contained in a table
        std::vector<Bits> get_all_keys() const;

        // THROWS
        std::vector<Bits> get_all_keys_alive() const;
 
        std::pair<std::vector<Bits>, Bits>get_n_next_keys_alive(const Bits& target_key, uint32_t count) const;

        // used for testing
        std::pair<std::vector<Bits>, Bits> get_n_next_keys(const Bits& target_key, uint32_t count) const;

        // THROWS
        // returns UP TO count entries, if less entries are returned (because n entries dont exist in this table, the next key is a placeholder)
        // returns the entries in descending order by keys
        std::pair<std::vector<Entry>, Bits> get_entries_key_smaller_or_equal(const Bits& target_key, uint32_t count) const;

        // THROWS
        // returns UP TO count entries, if less entries are returned (because n entries dont exist in this table, the next key is a placeholder)
        std::pair<std::vector<Entry>, Bits> get_entries_key_larger_or_equal(const Bits& target_key, uint32_t count) const;

        // THROWS
        // returns UP TO count entries, if less entries are returned (because n entries dont exist in this table, the next key is a placeholder)
        // returns the entries in descending order by keys
        std::pair<std::vector<Entry>, Bits> get_entries_key_smaller_or_equal_alive(const Bits& target_key, uint32_t count) const;

        // THROWS
        // returns UP TO count entries, if less entries are returned (because n entries dont exist in this table, the next key is a placeholder)
        std::pair<std::vector<Entry>, Bits> get_entries_key_larger_or_equal_alive(const Bits& target_key, uint32_t count) const;

        // THROWS
        // returns UP TO count entries, if less entries are returned (because n entries dont exist in this table, the next key is a placeholder)
        std::pair<std::vector<Entry>, Bits> get_n_entries_alive(uint32_t count) const;

        // THROWS
        // returns UP TO count entries, if less entries are returned (because n entries dont exist in this table, the next key is a placeholder)
        std::pair<std::vector<Entry>, Bits> get_n_entries(uint32_t count) const;
};

#endif // SS_TABLE_H_INCLUDED
