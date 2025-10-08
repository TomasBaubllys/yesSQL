#include "../include/ss_table.h"
#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include "../include/entry.h"

// bad
Entry SS_Table::read_entry_at_offset(uint64_t offset){
    std::ifstream ifs(data_file, std::ios::binary);
    if(!ifs){
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG);
    }

    ifs.seekg(offset);

    bit_arr_size_type key_len;
    ifs.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
    std::string key_str(key_len, '\0');
    ifs.read(key_str.data(), key_len);

    bit_arr_size_type value_len;
    ifs.read(reinterpret_cast<char*>(&value_len), sizeof(value_len));
    std::string value_str(value_len, '\0');
    ifs.read(value_str.data(), value_len);

    return Entry(Bits(key_str), Bits(value_str));
}

// bad
Entry SS_Table::get(Bits& key, bool& found){
    found = false;
    std::string placeholder_key(ENTRY_PLACEHOLDER_KEY);
	std::string placeholder_value(ENTRY_PLACEHOLDER_VALUE);

    std::ifstream ifs(index_file, std::ios::binary);
      if(!ifs){
        std::cerr << "Failed to open data file: " << data_file << std::endl;
		return Entry(Bits(placeholder_key), Bits(placeholder_value));
    }

    while(ifs.peek() != EOF){
        bit_arr_size_type key_len;
        ifs.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));

        std::string current_key(key_len, '\0');
        ifs.read(current_key.data(), key_len);

        bit_arr_size_type offset;
        ifs.read(reinterpret_cast<char*>(&offset), sizeof(offset));

        if(Bits(current_key) == key){
            found = true;
            return read_entry_at_offset(offset);
        }

    }

    return &Entry(Bits(placeholder_key), Bits(placeholder_value));
}

SS_Table::SS_Table(const std::filesystem::path& data, const std::filesystem::path& index)
    : data_file(data), index_file(index){
        

    };

SS_Table:: ~SS_Table(){};

std::filesystem::path SS_Table::data_path(){
    return this -> data_file;
}

std::filesystem::path SS_Table::index_path(){
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

	// check vector size if it doesnt exceed uint16_t
	if(1){

    }

	this -> first_index = entry_vector.front().get_key();
	this -> last_index = entry_vector.back().get_key();
	
	std::ofstream data_out(this -> data_file, std::ios::binary);
	std::ofstream index_out(this -> index_file, std::ios::binary);

	if(!data_out) {
		throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG);
	}

	if(!index_out) {
		throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG);
	}

	uint64_t offset = 0;

	for(uint16_t i = 0; i < entry_vector.size(); ++i) {
        std::ostringstream key_bytes = entry_vector.at(i).get_ostream_key_bytes();
        std::ostringstream value_bytes = entry_vector.at(i).get_ostream_data_bytes();

        std::string key_str = key_bytes.str();
        std::string value_str = value_bytes.str();

        // sizes guaranteed by ENTRY constructor
        uint16_t key_len = key_str.size();
        uint32_t value_len = value_str.size();

        // write the index length
        index_out.write(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        // write the key itself
        index_out.write(key_str.data(), key_len);
        // write the offset
        index_out.write(reinterpret_cast<char*>(&offset), sizeof(offset));



        // write the sizeof data
        data_out.write(reinterpret_cast<char*>(&value_len), sizeof(value_len));
        // write the data
        data_out.write(value_str.data(), value_len);

        offset += value_len;
    }

    data_out.close();
    index_out.close();
}

