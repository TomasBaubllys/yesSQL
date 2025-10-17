#include "../include/ss_table.h"
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include "../include/entry.h"

std::string SS_Table::read_stream_at_offset(uint64_t& offset) const {
    std::ifstream data_in(this -> data_file, std::ios::binary);
    if(!data_in) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG);
    }

    data_in.seekg(offset, data_in.beg);

    if(data_in.fail()) {
        throw std::runtime_error(SS_TABLE_BAD_OFFSET_ERR_MSG);
    }

    uint64_t data_len = 0;
    data_in.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));
  
    if(data_in.fail()) {
        std::cout<<"1"<<std::endl;
        throw std::runtime_error(SS_TABLE_UNEXPECTED_DATA_EOF_MSG);
    }

    std::string raw_data(data_len, '\0');

    data_in.read(&raw_data[0], data_len);

    if(data_in.fail()) {
        std::cout<<"2"<<std::endl;
        throw std::runtime_error(SS_TABLE_UNEXPECTED_DATA_EOF_MSG);
    }

    return raw_data;
}

// TEST AND check return values
Entry SS_Table::get(const Bits& key, bool& found) const {
    found = false;

    if(key < this -> first_index || key > this ->last_index) {
        return Entry(Bits(ENTRY_PLACEHOLDER_KEY), Bits(ENTRY_PLACEHOLDER_VALUE));
    }

    std::ifstream index_in(this -> index_file, std::ios::binary);
    if(!index_in) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG);
    }

    std::ifstream index_offset_in(this -> index_offset_file, std::ios::binary);
    if(!index_offset_in) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG);
    }

    // first set the index to the middle
    uint64_t binary_search_right = this -> record_count - 1;
    uint64_t binary_search_left = 0;
    uint64_t key_offset = 0;
    uint64_t data_offset = 0;
    key_len_type key_length = 0;
    std::string key_str;

    // binary search for the key
    while(binary_search_left <= binary_search_right) {
        uint64_t binary_search_index = binary_search_left + (binary_search_right - binary_search_left) / 2;

        // read the key offset
        index_offset_in.seekg(binary_search_index * sizeof(uint64_t) , index_offset_in.beg);
        if(index_offset_in.fail()) {
            throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG);
        }


        // read the keys offset
        index_offset_in.read(reinterpret_cast<char*>(&key_offset), sizeof(key_offset));
        if(index_offset_in.fail()) {
            throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG);
        }

        // place the pointer in index_in
        index_in.seekg(key_offset, index_in.beg);
        if(index_in.fail()) {
            throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG);
        }

        // read the size of the key itself
        index_in.read(reinterpret_cast<char*>(&key_length), sizeof(key_length));
        if(index_in.fail()) {
            throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG);
        }

        // now read the key
        key_str.resize(key_length);
        index_in.read(&key_str[0], key_length);
        if(index_in.fail()) {
            throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG);
        }

        // compare the key
        int8_t compare = key.compare_to_str(key_str);

        // std::cout << "Comparing: " << key_string << " and " << key.get_string_char() << " Compare value: " << int(compare) << std::endl;

        // match found
        if(compare == 0) {
            index_in.read(reinterpret_cast<char*>(&data_offset), sizeof(data_offset));
            if(index_in.fail()) {
                throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG);
            }

            found = true;
            break;
        }

        // Key is bigger
        if(compare > 0) {
            if(binary_search_index > this -> record_count) {
                found = false;
                break;
            }

            binary_search_left = binary_search_index + 1;
        }

        // Key is smaller
        if(compare < 0) {
            if(binary_search_index == 0) {
                found = false;
                break;
            }

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
    std::string data_str = this -> read_stream_at_offset(data_offset);

    // construct an entry and return AAAAAAAAA
    return Entry(key_str, data_str);
}

SS_Table::SS_Table(const std::filesystem::path& _data_file, const std::filesystem::path& _index_file, std::filesystem::path& _index_offset_file)
    : data_file(_data_file), index_file(_index_file), index_offset_file(_index_offset_file), first_index(ENTRY_PLACEHOLDER_KEY), last_index((ENTRY_PLACEHOLDER_KEY)), record_count(0), data_file_size(0), index_file_size(0), index_offset_file_size(0) {

    };

SS_Table:: ~SS_Table(){
    remove(this -> data_file);
    remove(this -> index_file);
    remove(this -> index_offset_file);
};

std::filesystem::path SS_Table::data_path() const {
    return this -> data_file;
}

std::filesystem::path SS_Table::index_path() const {
    return this -> index_file;
}


Bits SS_Table::get_last_index() const {
	return this -> last_index;
}

Bits SS_Table::get_first_index() const {
	return this -> first_index;
}

uint64_t SS_Table::fill_ss_table(const std::vector<Entry>& entry_vector) {
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

	for(const Entry& entry : entry_vector) {
        std::string key_in = entry.get_string_key_bytes();
        std::string data_in = entry.get_string_data_bytes();

        key_len_type key_len = key_in.size();
        uint64_t data_len = data_in.size();

        // write to the index offset mapping file
        index_offset_out.write(reinterpret_cast<char*>(&key_offset), sizeof(key_offset));
        if(index_offset_out.fail()) {
            throw std::runtime_error(SS_TABLE_FAILED_INDEX_OFFSET_WRITE_ERR_MSG);
        }

        // write the index length
        index_out.write(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        if(index_out.fail()) {
            throw std::runtime_error(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG);
        }

        index_out.write(key_in.data(), key_len);
        if(index_out.fail()) {
            throw std::runtime_error(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG);
        }

        index_out.write(reinterpret_cast<char*>(&data_offset), sizeof(data_offset));
        if(index_out.fail()) {
            throw std::runtime_error(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG);
        }

        data_out.write(reinterpret_cast<char*>(&data_len), sizeof(data_len));
        if(data_out.fail()) {
            throw std::runtime_error(SS_TABLE_FAILED_DATA_WRITE_ERR_MSG);
        }

        data_out.write(data_in.data(), data_len);
        if(data_out.fail()) {
            throw std::runtime_error(SS_TABLE_FAILED_DATA_WRITE_ERR_MSG);
        }

        data_offset += data_len + sizeof(data_len);
        key_offset += key_len + sizeof(key_len) + sizeof(data_offset);
    }

    this -> data_file_size = data_offset;
    this -> index_file_size = key_offset;
    this -> record_count = entry_vector.size();
    
    return record_count;
}

uint64_t SS_Table::append(const std::vector<Entry>& entry_vector) {
    if(entry_vector.size() == 0) {
        return 0;
    }

    if(this -> first_index == Bits(ENTRY_PLACEHOLDER_KEY)) {
        this -> first_index = entry_vector.front().get_key();
    }

    this -> last_index = entry_vector.back().get_key();

    std::ofstream data_out(this -> data_file, std::ios::binary | std::ios::app);
    std::ofstream index_out(this -> index_file, std::ios::binary | std::ios::app);
    std::ofstream index_offset_out(this ->index_offset_file, std::ios::binary | std::ios::app);

    if(!data_out) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG);
    }

    if(!index_out) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG);
    }

    if(!index_offset_out) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG);
    }

    uint64_t data_offset = this -> data_file_size;
    uint64_t key_offset = this -> index_file_size;

    for(const Entry& entry : entry_vector) {
        std::string key_in = entry.get_string_key_bytes();
        std::string data_in = entry.get_string_data_bytes();

        key_len_type key_len = key_in.size();
        uint64_t data_len = data_in.size();

        // write to the index offset mapping file
        index_offset_out.write(reinterpret_cast<char*>(&key_offset), sizeof(key_offset));
        if(index_offset_out.fail()) {
            throw std::runtime_error(SS_TABLE_FAILED_INDEX_OFFSET_WRITE_ERR_MSG);
        }

        // write the index length
        index_out.write(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        if(index_out.fail()) {
            throw std::runtime_error(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG);
        }

        index_out.write(key_in.data(), key_len);
        if(index_out.fail()) {
            throw std::runtime_error(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG);
        }

        index_out.write(reinterpret_cast<char*>(&data_offset), sizeof(data_offset));
        if(index_out.fail()) {
            throw std::runtime_error(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG);
        }

        data_out.write(reinterpret_cast<char*>(&data_len), sizeof(data_len));
        if(data_out.fail()) {
            throw std::runtime_error(SS_TABLE_FAILED_DATA_WRITE_ERR_MSG);
        }

        data_out.write(data_in.data(), data_len);
        if(data_out.fail()) {
            throw std::runtime_error(SS_TABLE_FAILED_DATA_WRITE_ERR_MSG);
        }

        data_offset += data_len + sizeof(data_len);
        key_offset += key_len + sizeof(key_len) + sizeof(data_offset);
    }

    this -> data_file_size = data_offset;
    this -> index_file_size = key_offset;
    this -> record_count += entry_vector.size();

    return entry_vector.size();
}

SS_Table::Keynator::Keynator(const std::filesystem::path& index_file, const std::filesystem::path& index_offset_file, const std::filesystem::path& data_file, uint64_t record_count) : index_stream(index_file), index_offset_stream(index_offset_file), data_file(data_file), current_data_offset(0), records_read(0), record_count(record_count) {
    if(index_stream.fail()) {
        throw std::runtime_error(SS_TABLE_KEYNATOR_FAILED_OPEN_INDEX_FILE_ERR_MSG);
    }

    if(index_offset_stream.fail()) {
        throw std::runtime_error(SS_TABLE_KEYNATOR_FAILED_OPEN_INDEX_OFFSET_FILE_ERR_MSG);
    }
}

SS_Table::Keynator::~Keynator() {

}

Bits SS_Table::Keynator::get_next_key() {
    // probably complete unnecesarry
    if(this -> records_read >= this -> record_count) {
        return Bits(ENTRY_PLACEHOLDER_KEY);
    }

    uint64_t current_key_offset = 0;

    // std::cout << index_offset_stream.is_open() << std::endl;

    index_offset_stream.read(reinterpret_cast<char*>(&current_key_offset), sizeof(current_key_offset));
    if(index_offset_stream.fail()) {
        throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG);
    }

    this -> index_stream.seekg(current_key_offset, this -> index_stream.beg);
    if(index_stream.fail()) {
        throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG);
    }

    key_len_type current_key_size = 0;
    this -> index_stream.read(reinterpret_cast<char*>(&current_key_size), sizeof(current_key_size));
    if(index_stream.fail()) {
        throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG);
    }

    std::string current_key_str(current_key_size, '\0');
    this -> index_stream.read(&current_key_str[0], current_key_size);
    if(index_stream.fail()) {
        throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG);
    }

    this -> index_stream.read(reinterpret_cast<char*>(& this -> current_data_offset), sizeof(this -> current_data_offset));
    if(index_stream.fail()) {
        throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG);
    }

    ++this -> records_read;
    return Bits(current_key_str);
}

std::string SS_Table::Keynator::get_current_data_string() {
    std::ifstream data_stream(this -> data_file, std::ios::binary);
    if(data_stream.fail()) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG);
    }

    data_stream.seekg(this -> current_data_offset, data_stream.beg);
    if(data_stream.fail()) {
        throw std::runtime_error(SS_TABLE_UNEXPECTED_DATA_EOF_MSG);
    }

    uint64_t data_string_length = 0;
    data_stream.read(reinterpret_cast<char*>(&data_string_length), sizeof(data_string_length));
    if(data_stream.fail()) {
        throw std::runtime_error(SS_TABLE_UNEXPECTED_DATA_EOF_MSG);
    }

    std::string data_string(data_string_length, '\0');
    data_stream.read(&data_string[0], data_string_length);
    if(data_stream.fail()) {
        throw std::runtime_error(SS_TABLE_UNEXPECTED_DATA_EOF_MSG);
    }

    return data_string;
}

SS_Table::Keynator SS_Table::get_keynator() const {
    return Keynator(this -> index_file, this -> index_offset_file, this -> data_file, this -> record_count);
}

int8_t SS_Table::init_writing() {
    this -> data_ofstream.open(this -> data_file, std::ios::binary);
    if(this -> data_ofstream.fail()) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG);
    }

    this -> index_ofstream.open(this -> index_file, std::ios::binary);
    if(this -> index_ofstream.fail()) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG);
    }

    this -> index_offset_ofstream.open(this -> index_offset_file, std::ios::binary);
    if(this ->index_offset_ofstream.fail()) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG);
    }

    return 0;
}

int8_t SS_Table::write(const Bits& key, const std::string& data_string) {
    if(this -> first_index == Bits(ENTRY_PLACEHOLDER_KEY)) {
        this -> first_index = key;
    }

    uint64_t data_offset = this -> data_ofstream.tellp();
    uint64_t key_offset = this -> index_ofstream.tellp();
    uint64_t data_length = data_string.length();
    std::string key_string = key.get_string_char();
    key_len_type key_length = key.size();

    index_offset_ofstream.write(reinterpret_cast<char*>(&key_offset), sizeof(key_offset));
    if(index_offset_ofstream.fail()) {
        throw std::runtime_error(SS_TABLE_FAILED_INDEX_OFFSET_WRITE_ERR_MSG);
    }

    index_ofstream.write(reinterpret_cast<char*>(&key_length), sizeof(key_length));
    if(index_ofstream.fail()) {
        throw std::runtime_error(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG);
    }

    index_ofstream.write(&key_string[0], key_length);
    if(index_ofstream.fail()) {
        throw std::runtime_error(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG);
    }

    index_ofstream.write(reinterpret_cast<char*>(&data_offset), sizeof(data_offset));
    if(index_ofstream.fail()) {
        throw std::runtime_error(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG);
    }

    data_ofstream.write(reinterpret_cast<char*>(&data_length), sizeof(data_length));
    if(data_ofstream.fail()) {
        throw std::runtime_error(SS_TABLE_FAILED_DATA_WRITE_ERR_MSG);
    }

    data_ofstream.write(&data_string[0], data_length);
    if(data_ofstream.fail()) {
        throw std::runtime_error(SS_TABLE_FAILED_DATA_WRITE_ERR_MSG);
    }

    ++this -> record_count;
    this -> last_index = key;
    return 0;
}

int8_t SS_Table::stop_writing() {
    int8_t ret_value = 0;

    this -> data_ofstream.close();
    if(data_ofstream.is_open()) {
        ret_value |= DATA_CLOSE_FAILED;
    }

    this -> index_ofstream.close();
    if(index_ofstream.is_open()) {
        ret_value |= INDEX_CLOSE_FAILED;
    }

    this -> index_offset_ofstream.close();
    if(index_offset_ofstream.is_open()) {
        ret_value |= INDEX_CLOSE_FAILED;
    }

    return ret_value;
}

bool SS_Table::overlap(const Bits& first_index, const Bits& last_index) const {
    return !(last_index < this -> first_index || first_index > this -> last_index);
}


std::vector<Bits> SS_Table::get_all_keys() {
    std::vector<Bits> keys;
    keys.reserve(this -> record_count);

    std::ifstream index_ifstream(this -> index_file, std::ios::binary);
    if(index_ifstream.fail()) {
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG);
    }

    std::ifstream offset_ifstream(this -> index_offset_file, std::ios::binary);
    if(offset_ifstream.fail()){
        throw std::runtime_error(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG);
    }

    uint64_t current_key_offset = 0;

    while(offset_ifstream.read(reinterpret_cast<char*>(&current_key_offset), sizeof(current_key_offset))) {
        index_ifstream.seekg(current_key_offset, index_ifstream.beg);
        if(index_ifstream.fail()) {
            throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG);
        }

        key_len_type current_key_length = 0;
        index_ifstream.read(reinterpret_cast<char*>(&current_key_length), sizeof(current_key_length));
        if(index_ifstream.fail()) {
            throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG);
        }

        std::string current_key(current_key_length, '\0');
        index_ifstream.read(&current_key[0], current_key_length);
        if(index_ifstream.fail()) {
            throw std::runtime_error(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG);
        }

        keys.emplace_back(current_key);
    }

    if(!offset_ifstream.eof()) {
        throw std::runtime_error(SS_TABLE_PARTIAL_READ_ERR_MSG);
    }

    return keys;
}