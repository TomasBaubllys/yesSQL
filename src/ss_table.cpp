#include "../include/ss_table.h"
#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include "../include/entry.h"

std::stringstream SS_Table::read_stream_at_offset(uint64_t& offset) {
    std::ifstream data_in(this -> data_file, std::ios::binary);
    if(!data_in) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG);
    }

    data_in.seekg(offset, data_in.beg);

    if(data_in.fail()) {
        throw std::runtime_error(SS_TABLE_BAD_OFFSET_ERR_MSG);
    }

    value_len_type data_len = 0;
    data_in.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));

    if(data_in.fail()) {
        throw std::runtime_error(SS_TABLE_UNEXPECTED_DATA_EOF_MSG);
    }

    std::string raw_data(data_len, '\0');

    data_in.read(&raw_data[0], data_len);

    if(data_in.fail()) {
        throw std::runtime_error(SS_TABLE_UNEXPECTED_DATA_EOF_MSG);
    }

    return std::stringstream(raw_data);
}

// TEST AND check return values
Entry SS_Table::get(Bits& key, bool& found) {
    found = false;

    if(key < this -> first_index || key > this ->last_index) {
        return Entry(Bits(ENTRY_PLACEHOLDER_KEY), Bits(ENTRY_PLACEHOLDER_VALUE));
    }

    std::ifstream index_in(this -> index_file, std::ios::binary);
    if(!index_in) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG);
    }

    std::ifstream index_offset_in(this ->index_offset_file, std::ios::binary);
    if(!index_offset_in) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG);
    }

    // first set the index to the middle
    key_len_type binary_search_right = this -> record_count - 1;
    key_len_type binary_search_left = 0;
    uint64_t key_offset = 0;
    uint64_t data_offset = 0;
    key_len_type key_length = 0;
    std::string key_string;
    std::stringstream key_stream;

    // binary search for the key
    while(binary_search_left <= binary_search_right) {
        key_len_type binary_search_index = binary_search_left + (binary_search_right - binary_search_left) / 2;

        // read the key offset
        index_offset_in.seekg(binary_search_index * sizeof(uint64_t) , index_offset_in.beg);
        if(index_offset_in.fail()) {
            throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG);
        }


        // read the keys offset
        index_offset_in.read(reinterpret_cast<char*>(&key_offset), sizeof(key_offset));

        // place the pointer in index_in
        index_in.seekg(key_offset, index_in.beg);

        // read the size of the key itself
        index_in.read(reinterpret_cast<char*>(&key_length), sizeof(key_length));

        // now read the key
        // not very efficient
        key_string.resize(key_length);
        index_in.read(&key_string[0], key_length);

        // compare the key
        int8_t compare = key.compare_to_str(key_string);

        // match found
        if(compare == 0) {
            index_in.read(reinterpret_cast<char*>(&data_offset), sizeof(data_offset));
            found = true;
            key_stream.clear();
            key_stream.str(key_string);
            break;
        }

        // Key is bigger
        if(compare > 0) {
            binary_search_left = binary_search_index + 1;
        }

        // Key is smaller
        if(compare < 0) {
            binary_search_right = binary_search_index - 1;
        }


    }

    // if not found make a place holder found = false and return
    if(!found) {
        Bits key(ENTRY_PLACEHOLDER_KEY);
        Bits value(ENTRY_PLACEHOLDER_VALUE);
        return Entry(key, value);
    }

    // read the value
    std::stringstream data_stream = this -> read_stream_at_offset(data_offset);

    // construct an entry and return AAAAAAAAA
    return Entry(key_stream, data_stream);
}

SS_Table::SS_Table(const std::filesystem::path& _data_file, const std::filesystem::path& _index_file, std::filesystem::path& _index_offset_file)
    : data_file(_data_file), index_file(_index_file), index_offset_file(_index_offset_file), first_index(ENTRY_PLACEHOLDER_KEY), last_index((ENTRY_PLACEHOLDER_VALUE)), record_count(0) {
        

    };

SS_Table:: ~SS_Table(){};

std::filesystem::path SS_Table::data_path() {
    return this -> data_file;
}

std::filesystem::path SS_Table::index_path() {
    return this -> index_file;
}


Bits SS_Table::get_last_index() {
	return this -> first_index;
}

Bits SS_Table::get_first_index() {
	return this -> last_index;
}

uint16_t SS_Table::fill_ss_table(std::vector<Entry>& entry_vector) {
	if(entry_vector.size() == 0) {
		return 0;
	}	

	this -> first_index = entry_vector.front().get_key();
	this -> last_index = entry_vector.back().get_key();
	
	std::ofstream data_out(this -> data_file, std::ios::binary);
	std::ofstream index_out(this -> index_file, std::ios::binary);
    std::ofstream index_offset_out(this ->index_offset_file, std::ios::binary);

	if(!data_out) {
		throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG);
	}

	if(!index_out) {
		throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG);
	}

	if(!index_offset_out) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG);
    }

	uint64_t data_offset = 0;
    uint64_t key_offset = 0;

	for(key_len_type i = 0; i < entry_vector.size(); ++i) {
        std::ostringstream key_bytes = entry_vector.at(i).get_ostream_key_bytes();
        std::ostringstream value_bytes = entry_vector.at(i).get_ostream_data_bytes();

        std::string key_str = key_bytes.str();
        std::string value_str = value_bytes.str();

        // sizes guaranteed by ENTRY constructor
        if(key_str.size() > ENTRY_MAX_KEY_LEN) {
            throw std::length_error(ENTRY_MAX_KEY_LEN_EXCEEDED_ERR_MSG);
        }

        if(value_str.size() > ENTRY_MAX_VALUE_LEN) {
            throw std::length_error(ENTRY_MAX_VALUE_LEN_EXCEEDED_ERR_MSG);
        }
        key_len_type key_len = key_str.size();
        value_len_type value_len = value_str.size();

        // write to the index offset mapping file
        index_offset_out.write(reinterpret_cast<char*>(&key_offset), sizeof(key_offset));

        // write the index length
        index_out.write(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        // write the key itself
        index_out.write(&key_str[0], key_len);
        // write the offset
        index_out.write(reinterpret_cast<char*>(&data_offset), sizeof(data_offset));

        // write the sizeof data
        data_out.write(reinterpret_cast<char*>(&value_len), sizeof(value_len));
        // write the data
        data_out.write(&value_str[0], value_len);

        data_offset += value_len + sizeof(value_len);
        key_offset += key_len + sizeof(key_len);
    }

    this -> record_count = entry_vector.size();

    return record_count;
}

