#include "../include/ss_table.h"
#include <fstream>
#include <iostream>
#include "../include/entry.h"


Entry* SS_Table::read_entry_at_offset(uint64_t offset){
    std::ifstream ifs(data_file, std::ios::binary);
    if(!ifs){
        std::cerr << "Failed to open data file: " << data_file << std::endl;
		std::string key(ENTRY_PLACEHOLDER_KEY);
		std::string value(ENTRY_PLACEHOLDER_VALUE);
		return &Entry(Bits(key), Bits(value));
    }

    ifs.seekg(offset);

    bit_arr_size_type key_len;
    ifs.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
    std::string key_str(key_len, '\0');
    ifs.read(key_str.data(), key_len);

    bit_arr_size_type value_len;
    ifs.read(reinterpret_cast<char*>(&value_len), sizeof(value_len));
    std::string value_str(value_len, '/0');
    ifs.read(value_str.data(), value_len);

    Entry *e = new Entry(Bits(key_str), Bits(value_str));

    return e;
}

Entry* SS_Table::get(Bits& key, bool& found){
    found = false;
    std::string placeholder_key(ENTRY_PLACEHOLDER_KEY);
	std::string placeholder_value(ENTRY_PLACEHOLDER_VALUE);

    std::ifstream ifs(index_file, std::ios::binary);
      if(!ifs){
        std::cerr << "Failed to open data file: " << data_file << std::endl;
		return &Entry(Bits(placeholder_key), Bits(placeholder_value));
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

SS_Table:: SS_Table(const std::filesystem::path& data, const std::filesystem::path& index)
    : data_file(data), index_file(index){
        

    };

SS_Table:: ~SS_Table(){};

std::filesystem::path& SS_Table::data_path(){
    return this -> data_file;
}

std::filesystem::path& SS_Table::index_path(){
    return this -> index_file;
}
