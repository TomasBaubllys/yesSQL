#include "../include/ss_table.h"
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include "../include/entry.h"
#include "../include/file_exception.h"

std::string SS_Table::read_stream_at_offset(uint64_t& offset) const {
    std::ifstream data_in(this -> data_file, std::ios::binary);
    if(!data_in) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG, this -> data_file.generic_string().c_str());
    }

    data_in.seekg(offset, data_in.beg);

    if(data_in.fail()) {
        throw File_Exception(SS_TABLE_BAD_OFFSET_ERR_MSG, this -> data_file.generic_string().c_str());
    }

    uint64_t data_len = 0;
    data_in.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));
  
    if(data_in.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
    }

    std::string raw_data(data_len, '\0');

    data_in.read(&raw_data[0], data_len);

    if(data_in.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
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
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG, this -> index_file.generic_string().c_str());
    }

    std::ifstream index_offset_in(this -> index_offset_file, std::ios::binary);
    if(!index_offset_in) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG, this -> index_offset_file.generic_string().c_str());
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
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG, this -> index_offset_file.generic_string().c_str());
        }


        // read the keys offset
        index_offset_in.read(reinterpret_cast<char*>(&key_offset), sizeof(key_offset));
        if(index_offset_in.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG, this -> index_offset_file.generic_string().c_str());
        }

        // place the pointer in index_in
        index_in.seekg(key_offset, index_in.beg);
        if(index_in.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        // read the size of the key itself
        index_in.read(reinterpret_cast<char*>(&key_length), sizeof(key_length));
        if(index_in.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        // now read the key
        key_str.resize(key_length);
        index_in.read(&key_str[0], key_length);
        if(index_in.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        // compare the key
        int8_t compare = key.compare_to_str(key_str);

        // std::cout << "Comparing: " << key_string << " and " << key.get_string() << " Compare value: " << int(compare) << std::endl;

        // match found
        if(compare == 0) {
            index_in.read(reinterpret_cast<char*>(&data_offset), sizeof(data_offset));
            if(index_in.fail()) {
                throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
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
        Entry entry(key, value);
        entry.set_tombstone(ENTRY_TOMBSTONE_ON);
        return entry;
    }

    // read the value
    std::string data_str = this -> read_stream_at_offset(data_offset);

    // construct an entry and return AAAAAAAAA
    return Entry(key_str, data_str);
}

// needs a more complicated constructor --> or a reconstruct ss_table method
SS_Table::SS_Table(const std::filesystem::path& _data_file, const std::filesystem::path& _index_file, std::filesystem::path& _index_offset_file)
    : data_file(_data_file), index_file(_index_file), index_offset_file(_index_offset_file), first_index(ENTRY_PLACEHOLDER_KEY), last_index((ENTRY_PLACEHOLDER_KEY)), record_count(0), data_file_size(0), index_file_size(0), index_offset_file_size(0) {

    };

SS_Table:: ~SS_Table(){
    // remove(this -> data_file);
    // remove(this -> index_file);
    // remove(this -> index_offset_file);
};

std::filesystem::path SS_Table::data_path() const {
    return this -> data_file;
}

std::filesystem::path SS_Table::index_path() const {
    return this -> index_file;
}

std::filesystem::path SS_Table::offset_path() const {
    return this -> index_offset_file;
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
		throw File_Exception(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG, this -> data_file.generic_string().c_str());
	}

	if(!index_out) {
		throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG, this -> index_file.generic_string().c_str());
	}

	if(!index_offset_out) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG, this -> index_offset_file.generic_string().c_str());
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
            throw File_Exception(SS_TABLE_FAILED_INDEX_OFFSET_WRITE_ERR_MSG, this -> data_file.generic_string().c_str());
        }

        // write the index length
        index_out.write(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        if(index_out.fail()) {
            throw File_Exception(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG, this -> index_file.generic_string().c_str());
        }

        index_out.write(key_in.data(), key_len);
        if(index_out.fail()) {
            throw File_Exception(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG, this -> index_file.generic_string().c_str());
        }

        index_out.write(reinterpret_cast<char*>(&data_offset), sizeof(data_offset));
        if(index_out.fail()) {
            throw File_Exception(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG, this -> index_file.generic_string().c_str());
        }

        data_out.write(reinterpret_cast<char*>(&data_len), sizeof(data_len));
        if(data_out.fail()) {
            throw File_Exception(SS_TABLE_FAILED_DATA_WRITE_ERR_MSG, this -> data_file.generic_string().c_str());
        }

        data_out.write(data_in.data(), data_len);
        if(data_out.fail()) {
            throw File_Exception(SS_TABLE_FAILED_DATA_WRITE_ERR_MSG, this -> data_file.generic_string().c_str());
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
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG, this -> data_file.generic_string().c_str());
    }

    if(!index_out) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG, this -> index_file.generic_string().c_str());
    }

    if(!index_offset_out) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG, this -> index_offset_file.generic_string().c_str());
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
            throw File_Exception(SS_TABLE_FAILED_INDEX_OFFSET_WRITE_ERR_MSG, this -> index_offset_file.generic_string().c_str());
        }

        // write the index length
        index_out.write(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        if(index_out.fail()) {
            throw File_Exception(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG, this -> index_file.generic_string().c_str());
        }

        index_out.write(key_in.data(), key_len);
        if(index_out.fail()) {
            throw File_Exception(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG, this -> index_file.generic_string().c_str());
        }

        index_out.write(reinterpret_cast<char*>(&data_offset), sizeof(data_offset));
        if(index_out.fail()) {
            throw File_Exception(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG, this -> index_file.generic_string().c_str());
        }

        data_out.write(reinterpret_cast<char*>(&data_len), sizeof(data_len));
        if(data_out.fail()) {
            throw File_Exception(SS_TABLE_FAILED_DATA_WRITE_ERR_MSG, this -> data_file.generic_string().c_str());
        }

        data_out.write(data_in.data(), data_len);
        if(data_out.fail()) {
            throw File_Exception(SS_TABLE_FAILED_DATA_WRITE_ERR_MSG, this -> data_file.generic_string().c_str());
        }

        data_offset += data_len + sizeof(data_len);
        key_offset += key_len + sizeof(key_len) + sizeof(data_offset);
    }

    this -> data_file_size = data_offset;
    this -> index_file_size = key_offset;
    this -> record_count += entry_vector.size();

    return entry_vector.size();
}

SS_Table::Keynator::Keynator(const std::filesystem::path& index_file, const std::filesystem::path& index_offset_file, const std::filesystem::path& data_file, uint64_t record_count) : index_stream(index_file, std::ios::binary), index_offset_stream(index_offset_file, std::ios::binary), data_file(data_file), current_data_offset(0), records_read(0), record_count(record_count) {
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
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG, this -> data_file.generic_string().c_str());
    }

    this -> index_ofstream.open(this -> index_file, std::ios::binary);
    if(this -> index_ofstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG, this -> index_file.generic_string().c_str());
    }

    this -> index_offset_ofstream.open(this -> index_offset_file, std::ios::binary);
    if(this ->index_offset_ofstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG, this -> index_offset_file.generic_string().c_str());
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
    std::string key_string = key.get_string();
    key_len_type key_length = key.size();

    index_offset_ofstream.write(reinterpret_cast<char*>(&key_offset), sizeof(key_offset));
    if(index_offset_ofstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_INDEX_OFFSET_WRITE_ERR_MSG, this -> index_offset_file.generic_string().c_str());
    }

    index_ofstream.write(reinterpret_cast<char*>(&key_length), sizeof(key_length));
    if(index_ofstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG, this -> index_file.generic_string().c_str());
    }

    index_ofstream.write(&key_string[0], key_length);
    if(index_ofstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG, this -> index_file.generic_string().c_str());
    }

    index_ofstream.write(reinterpret_cast<char*>(&data_offset), sizeof(data_offset));
    if(index_ofstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_INDEX_WRITE_ERR_MSG, this -> index_file.generic_string().c_str());
    }

    data_ofstream.write(reinterpret_cast<char*>(&data_length), sizeof(data_length));
    if(data_ofstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_DATA_WRITE_ERR_MSG, this -> data_file.generic_string().c_str());
    }

    data_ofstream.write(&data_string[0], data_length);
    if(data_ofstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_DATA_WRITE_ERR_MSG, this -> data_file.generic_string().c_str());
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

std::vector<Bits> SS_Table::get_all_keys_alive(std::set<Bits> &dead_keys) const {
    return this -> get_all_keys(SS_TABLE_FILTER_ALIVE_ENTRIES, dead_keys);
}

std::vector<Bits> SS_Table::get_all_keys() const {
    std::set<Bits> dead_Keys;
    return this -> get_all_keys(SS_TABLE_FILTER_ALL_ENTRIES, dead_Keys);
}

std::vector<Bits> SS_Table::get_all_keys(SS_Table_Entry_Filter key_filter, std::set<Bits>& dead_keys) const {
    std::vector<Bits> keys;
    
    keys.reserve(this -> record_count);
    std::ifstream index_ifstream(this -> index_file, std::ios::binary);
    if(index_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG, this -> index_file.generic_string().c_str());
    }

    std::ifstream offset_ifstream(this -> index_offset_file, std::ios::binary);
    if(offset_ifstream.fail()){
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG, this -> index_offset_file.generic_string().c_str());
    }

    std::ifstream data_ifstream(this -> data_file, std::ios::binary);
    if(data_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG, this -> data_file.generic_string().c_str());
    }

    uint64_t current_key_offset = 0;

    while(offset_ifstream.read(reinterpret_cast<char*>(&current_key_offset), sizeof(current_key_offset))) {
        index_ifstream.seekg(current_key_offset, index_ifstream.beg);
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        key_len_type current_key_length = 0;
        index_ifstream.read(reinterpret_cast<char*>(&current_key_length), sizeof(current_key_length));
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        std::string current_key(current_key_length, '\0');
        index_ifstream.read(&current_key[0], current_key_length);
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        // read the data offset
        if(key_filter == SS_TABLE_FILTER_ALIVE_ENTRIES) {
            uint64_t current_data_offset = 0;
            index_ifstream.read(reinterpret_cast<char*>(&current_data_offset), sizeof(current_data_offset));
            if(index_ifstream.fail()) {
                throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
            }

            data_ifstream.seekg(current_data_offset, data_ifstream.beg);
            if(data_ifstream.fail()) {
                throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
            }
            // read the data itself
            uint64_t current_data_size = 0;
            data_ifstream.read(reinterpret_cast<char*>(&current_data_size), sizeof(current_data_size));
            if(data_ifstream.fail()) {
                throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
            }

            std::string current_data(current_data_size, '\0');
            data_ifstream.read(&current_data[0], current_data_size);
            if(data_ifstream.fail()) {
                throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
            }
            // check the tombstone flag
            Entry current_entry(current_key, current_data);
            Bits curr_key = current_entry.get_key();
            if(current_entry.is_deleted()) {
                dead_keys.emplace(curr_key);
            }
            else if(dead_keys.find(curr_key) != dead_keys.end()) {
                keys.emplace_back(curr_key);
            }
        }
        else {
            keys.emplace_back(current_key);
        }
    }

    if(!offset_ifstream.eof()) {
        throw std::runtime_error(SS_TABLE_PARTIAL_READ_ERR_MSG);
    }
    return keys;
}

uint64_t SS_Table::binary_search_nearest(std::ifstream& index_ifstream, std::ifstream& offset_ifstream, const Bits& target_key, SS_Table_Binary_Search_Type search_type) const {
    if(!index_ifstream.is_open()) {
        index_ifstream.open(this -> index_file, std::ios::binary);

        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG, this -> index_file.generic_string().c_str());
        }
    }

    if(!offset_ifstream.is_open()) {
        offset_ifstream.open(this -> index_offset_file, std::ios::binary);

        if(offset_ifstream.fail()) {
            throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG, this -> index_offset_file.generic_string().c_str());
        }
    }
    // binary search to find the first key larger than or equal to key
    uint64_t binary_search_left = 0;
    uint64_t binary_search_right = this -> record_count;

    // read all the keys from there and return as a vector
    while(binary_search_left < binary_search_right) {
        uint64_t binary_search_middle = (binary_search_left + binary_search_right) / 2;
        offset_ifstream.seekg(binary_search_middle * sizeof(uint64_t), std::ios::beg);
        if(offset_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG, this -> index_offset_file.generic_string().c_str());
        }

        uint64_t search_key_offset = 0;
        offset_ifstream.read(reinterpret_cast<char*>(&search_key_offset), sizeof(search_key_offset));
        if(offset_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG, this -> index_offset_file.generic_string().c_str());
        }

        index_ifstream.seekg(search_key_offset, index_ifstream.beg);
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        key_len_type key_length = 0;
        index_ifstream.read(reinterpret_cast<char*>(&key_length), sizeof(key_length));
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        std::string current_key_string(key_length, '\0');
        index_ifstream.read(&current_key_string[0], key_length);
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        Bits current_key(current_key_string);
        switch(search_type) {
            case SS_TABLE_LARGER_OR_EQUAL:
                if(current_key < target_key) {
                    binary_search_left = binary_search_middle + 1;
                }
                else {
                    binary_search_right = binary_search_middle;
                }
                break;
            case SS_TABLE_SMALLER_OR_EQUAL:
                if(current_key <= target_key) {
                    binary_search_left = binary_search_middle + 1;
                }
                else {
                    binary_search_right = binary_search_middle;
                }
                break;
            default:
                throw std::runtime_error(SS_TABLE_BINARY_SEARCH_UNKNOWN_TYPE_ERR_MSG);
                break;
        }
    }

    return binary_search_left;
}

std::vector<Entry> SS_Table::get_n_entries(SS_Table_Entry_Filter key_filter, uint32_t count, std::set<Bits>& dead_keys) const {
    std::vector<Entry> entries;
    
    entries.reserve(count);
    std::ifstream index_ifstream(this -> index_file, std::ios::binary);
    if(index_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG, this -> index_file.generic_string().c_str());
    }

    std::ifstream offset_ifstream(this -> index_offset_file, std::ios::binary);
    if(offset_ifstream.fail()){
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG, this -> index_offset_file.generic_string().c_str());
    }

    std::ifstream data_ifstream(this -> data_file, std::ios::binary);
    if(data_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG, this -> data_file.generic_string().c_str());
    }

    uint64_t current_key_offset = 0;

    while(offset_ifstream.read(reinterpret_cast<char*>(&current_key_offset), sizeof(current_key_offset))) {
        index_ifstream.seekg(current_key_offset, index_ifstream.beg);
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        key_len_type current_key_length = 0;
        index_ifstream.read(reinterpret_cast<char*>(&current_key_length), sizeof(current_key_length));
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        std::string current_key(current_key_length, '\0');
        index_ifstream.read(&current_key[0], current_key_length);
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        // read the data offset
        uint64_t current_data_offset = 0;
        index_ifstream.read(reinterpret_cast<char*>(&current_data_offset), sizeof(current_data_offset));
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        data_ifstream.seekg(current_data_offset, data_ifstream.beg);
        if(data_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
        }
        // read the data itself
        uint64_t current_data_size = 0;
        data_ifstream.read(reinterpret_cast<char*>(&current_data_size), sizeof(current_data_size));
        if(data_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
        }

        std::string current_data(current_data_size, '\0');
        data_ifstream.read(&current_data[0], current_data_size);
        if(data_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this ->data_file.generic_string().c_str());
        }
        // check the tombstone flag
        Entry current_entry(current_key, current_data);
        Bits curr_key = current_entry.get_key();

        if(key_filter == SS_Table_Entry_Filter::SS_TABLE_FILTER_ALL_ENTRIES) {
            entries.emplace_back(current_entry);
        }
        else if(key_filter == SS_Table_Entry_Filter::SS_TABLE_FILTER_ALIVE_ENTRIES) {
            if(current_entry.is_deleted()) {
                dead_keys.emplace(curr_key);
            }
            else if(dead_keys.find(curr_key) != dead_keys.end()) {
                entries.emplace_back(current_entry);
            }
        }

        if(entries.size() == count) {
            return entries;
        }

        entries.emplace_back(current_entry);
    }

    if(!offset_ifstream.eof()) {
        throw std::runtime_error(SS_TABLE_PARTIAL_READ_ERR_MSG);
    }

    return entries;
}

std::vector<Entry> SS_Table::get_entries_key_smaller_or_equal(const Bits& target_key, SS_Table_Entry_Filter key_filter, uint32_t count, std::set<Bits>& dead_keys) const {
    std::vector<Entry> entries;
    if(target_key < this -> first_index) {
        return entries;
    }

    if(this -> last_index <= target_key) {
        return this -> get_n_entries(key_filter, count, dead_keys);
    }

    entries.reserve(this -> record_count);

    std::ifstream index_ifstream(this -> index_file, std::ios::binary);
    if(index_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG, this -> index_file.generic_string().c_str());
    }

    std::ifstream offset_ifstream(this -> index_offset_file, std::ios::binary);
    if(offset_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG, this -> index_offset_file.generic_string().c_str());
    }

    uint64_t smaller_equal_index = this -> binary_search_nearest(index_ifstream, offset_ifstream, target_key, SS_TABLE_SMALLER_OR_EQUAL);

    // read all the keys from start to current offset and write to vector
    // left is > target
    // left - 1 is <= target 
    if(smaller_equal_index == 0) {
        throw std::runtime_error(SS_TABLE_KEYS_SMALLER_THAN_FAILED_ERR_MSG);
    }

    offset_ifstream.seekg(0, offset_ifstream.beg);
    if(offset_ifstream.fail()) {
        throw File_Exception(SS_TABLE_INDEX_OFFSET_SEEK0_FAILED_ERR_MSG, this -> index_offset_file.generic_string().c_str());
    }

    if(index_ifstream.fail()) {
        throw File_Exception(SS_TABLE_INDEX_SEEK0_FAILED_ERR_MSG, this -> index_file.generic_string().c_str());
    }

    // open the data file for checking if the value is alive
    std::ifstream data_ifstream(this -> data_file, std::ios::binary);
    if(data_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG, this -> data_file.generic_string().c_str());
    }

    uint64_t current_key_offset = 0;
    index_ifstream.seekg(0, index_ifstream.beg);
    for(uint64_t i = 0; i < smaller_equal_index; ++i) {
        offset_ifstream.read(reinterpret_cast<char*>(&current_key_offset), sizeof(current_key_offset));
        if(offset_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG, this -> index_offset_file.generic_string().c_str());
        }

        index_ifstream.seekg(current_key_offset, index_ifstream.beg);
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        key_len_type current_key_length = 0;
        index_ifstream.read(reinterpret_cast<char*>(&current_key_length), sizeof(current_key_length));
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        std::string current_key(current_key_length, '\0');
        index_ifstream.read(&current_key[0], current_key_length);
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        // read the data offset
        uint64_t current_data_offset = 0;
        index_ifstream.read(reinterpret_cast<char*>(&current_data_offset), sizeof(current_data_offset));
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        data_ifstream.seekg(current_data_offset, data_ifstream.beg);
        if(data_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
        }

        // read the data length
        uint64_t current_data_length = 0;
        data_ifstream.read(reinterpret_cast<char*>(&current_data_length), sizeof(current_data_length));
        if(data_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
        }

        // read the data itself
        std::string current_data(current_data_length, '\0');
        data_ifstream.read(&current_data[0], current_data_length);
        if(data_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
        }

        // check tombstone
        Entry current_entry(current_key, current_data);
        Bits curr_key = current_entry.get_key();
        if(key_filter == SS_Table_Entry_Filter::SS_TABLE_FILTER_ALL_ENTRIES) {
            entries.emplace_back(current_entry);
        }
        else if(key_filter == SS_Table_Entry_Filter::SS_TABLE_FILTER_ALIVE_ENTRIES) {
            if(current_entry.is_deleted()) {
                dead_keys.emplace(curr_key);
            }
            else if(dead_keys.find(curr_key) != dead_keys.end()) {
                entries.emplace_back(current_entry);
            }
        }
    }

    if(entries.size() <= count) {
        return entries;
    }
    size_t start_index = entries.size() - count;
    std::vector<Entry> entries_partial(entries.begin() + start_index, entries.end());
    return entries_partial;
}

std::vector<Entry> SS_Table::get_entries_key_larger_or_equal(const Bits& target_key, SS_Table_Entry_Filter entry_filter, uint32_t count, std::set<Bits>& dead_keys) const {
    std::vector<Entry> entries;
    if(target_key > this -> last_index) {
        return entries;
    }

    if(target_key <= this -> first_index) {
        return this -> get_n_entries(entry_filter, count, dead_keys);
    }

    entries.reserve(count);

    std::ifstream index_ifstream(this -> index_file, std::ios::binary);
    if(index_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG, this -> index_file.generic_string().c_str());
    }

    std::ifstream offset_ifstream(this -> index_offset_file, std::ios::binary);
    if(offset_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG, this -> index_offset_file.generic_string().c_str());
    }

    uint64_t larger_equal_index = this -> binary_search_nearest(index_ifstream, offset_ifstream, target_key, SS_TABLE_LARGER_OR_EQUAL);

    // left == record_count -> not found HARD ERROR MEANS LAST_INDEX WAS BAD
    if(larger_equal_index == this -> record_count) {
        throw std::runtime_error(SS_TABLE_KEYS_LARGER_THAN_FAILED_ERR_MSG);
    }

    uint64_t current_key_offset = 0;
    offset_ifstream.seekg(larger_equal_index * sizeof(uint64_t), offset_ifstream.beg);
    if(offset_ifstream.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG, this -> index_offset_file.generic_string().c_str());
    }

    std::ifstream data_ifstream(this -> data_file, std::ios::binary);
    if(data_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG, this -> data_file.generic_string().c_str());
    }

    while(offset_ifstream.read(reinterpret_cast<char*>(&current_key_offset), sizeof(current_key_offset))) {
        index_ifstream.seekg(current_key_offset, index_ifstream.beg);
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        key_len_type current_key_length = 0;
        index_ifstream.read(reinterpret_cast<char*>(&current_key_length), sizeof(current_key_length));
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        std::string current_key(current_key_length, '\0');
        index_ifstream.read(&current_key[0], current_key_length);
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }
        // read the data offset
        uint64_t current_data_offset = 0;
        index_ifstream.read(reinterpret_cast<char*>(&current_data_offset), sizeof(current_data_offset));
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        data_ifstream.seekg(current_data_offset, data_ifstream.beg);
        if(data_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
        }

        // read the data length
        uint64_t current_data_length = 0;
        data_ifstream.read(reinterpret_cast<char*>(&current_data_length), sizeof(current_data_length));
        if(data_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
        }

        // read the data itself
        std::string current_data(current_data_length, '\0');
        data_ifstream.read(&current_data[0], current_data_length);
        if(data_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
        }

        // check tombstone
        Entry current_entry(current_key, current_data);
        if(entry_filter == SS_Table_Entry_Filter::SS_TABLE_FILTER_ALL_ENTRIES) {
            entries.push_back(current_entry);
        }
        else if(entry_filter == SS_Table_Entry_Filter::SS_TABLE_FILTER_ALIVE_ENTRIES) {
            Bits curr_key = current_entry.get_key();

            if(current_entry.is_deleted()) {
                dead_keys.emplace(curr_key);
            }
            else if(dead_keys.find(curr_key) != dead_keys.end()) {
                entries.push_back(current_entry);
            }
        }

        if(entries.size() == count) {
            return entries;
        }
    }

    return entries;
}

std::vector<Entry> SS_Table::get_entries_key_smaller_or_equal(const Bits& target_key, uint32_t count) const {
    std::set<Bits> dead_keys;
    return this -> get_entries_key_smaller_or_equal(target_key, SS_TABLE_FILTER_ALL_ENTRIES, count, dead_keys);
}

std::vector<Entry> SS_Table::get_entries_key_larger_or_equal(const Bits& target_key, uint32_t count) const {
    std::set<Bits> dead_keys;
    return this -> get_entries_key_larger_or_equal(target_key, SS_TABLE_FILTER_ALL_ENTRIES, count, dead_keys);
}

std::vector<Entry> SS_Table::get_entries_key_smaller_or_equal_alive(const Bits& target_key, uint32_t count, std::set<Bits>& dead_keys) const {
       return this -> get_entries_key_smaller_or_equal(target_key, SS_TABLE_FILTER_ALIVE_ENTRIES, count, dead_keys);
}

std::vector<Entry> SS_Table::get_entries_key_larger_or_equal_alive(const Bits& target_key, uint32_t count, std::set<Bits>& dead_keys) const {
    return this -> get_entries_key_larger_or_equal(target_key, SS_TABLE_FILTER_ALIVE_ENTRIES, count, dead_keys);
}

std::vector<Entry> SS_Table::get_n_entries_alive(uint32_t count, std::set<Bits>& dead_keys) const {
    return this -> get_n_entries(SS_TABLE_FILTER_ALIVE_ENTRIES, count, dead_keys);
}

std::vector<Entry> SS_Table::get_n_entries(uint32_t count) const {
    std::set<Bits> dead_keys;
    return this -> get_n_entries(SS_TABLE_FILTER_ALL_ENTRIES, count, dead_keys);
}

void SS_Table::reconstruct_ss_table(){
    // atsidaryt offset filea, nuskaityt pirmo rakto offseta
    // atsidaryt index file ir seekg tuo offsetu -> set first index
    // atsidaryt offset filea, eit i pati gala - sizeofoffset record, nuskaityt
    // seekg tuo offsetu index file -> set last index
    // record count --> offset ir dalint is record size
    // visu failu dydzius nuskaityt

    uint64_t first_key_offset = 0;
    uint64_t last_key_offset = 0;
    key_len_type key_length = 0;
    std::string key_str;


    std::ifstream index_in(this -> index_file, std::ios::binary);
    if(!index_in) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG, (this -> index_file).generic_string().c_str());
    }

    std::ifstream index_offset_in(this -> index_offset_file, std::ios::binary);
    if(!index_offset_in) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG, (this -> index_file).generic_string().c_str());
    }

    // FIRST INDEX
    // SEEK the key offset
    index_offset_in.seekg(0, std::ios::beg);
    if(index_offset_in.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG, this -> index_offset_file.generic_string().c_str());
    }


    // read the keys offset
    index_offset_in.read(reinterpret_cast<char*>(&first_key_offset), sizeof(first_key_offset));
    if(index_offset_in.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG, this -> index_offset_file.generic_string().c_str());
    }

    // place the pointer in index_in
    index_in.seekg(first_key_offset, index_in.beg);
    if(index_in.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
    }

    // read the size of the key itself
    index_in.read(reinterpret_cast<char*>(&key_length), sizeof(key_length));
    if(index_in.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
    }

    // now read the key
    key_str.resize(key_length);
    index_in.read(&key_str[0], key_length);
    if(index_in.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
    }

    this -> first_index = Bits(key_str);

    // LAST INDEX

     // SEEK the key offset
    index_offset_in.seekg(-SS_TABLE_KEY_OFFSET_RECORD_SIZE, std::ios::end);
    if(index_offset_in.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG, this -> index_offset_file.generic_string().c_str());
    }


    // read the keys offset
    index_offset_in.read(reinterpret_cast<char*>(&last_key_offset), sizeof(last_key_offset));
    if(index_offset_in.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG, this -> index_offset_file.generic_string().c_str());
    }

    // place the pointer in index_in
    index_in.seekg(last_key_offset, index_in.beg);
    if(index_in.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
    }

    // read the size of the key itself
    index_in.read(reinterpret_cast<char*>(&key_length), sizeof(key_length));
    if(index_in.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
    }

    // now read the key
    key_str.resize(key_length);
    index_in.read(&key_str[0], key_length);
    if(index_in.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
    }

    this -> last_index = Bits(key_str);

    this -> data_file_size = std::filesystem::file_size(this -> data_file);
    this -> index_file_size = std::filesystem::file_size(this -> index_file);
    this -> index_offset_file_size = std::filesystem::file_size(this -> index_offset_file);

    this -> record_count = index_offset_file_size / SS_TABLE_KEY_OFFSET_RECORD_SIZE;
}

std::vector<Bits> SS_Table::get_n_next_keys_alive(const Bits& target_key, uint32_t count, std::set<Bits>& dead_keys) const {
    return this -> get_n_next_keys(target_key, SS_Table_Entry_Filter::SS_TABLE_FILTER_ALIVE_ENTRIES, count, dead_keys);
}

std::vector<Bits> SS_Table::get_n_next_keys(const Bits& target_key, uint32_t count) const {
    std::set<Bits> dead_keys;
    return this -> get_n_next_keys(target_key, SS_Table_Entry_Filter::SS_TABLE_FILTER_ALL_ENTRIES, count, dead_keys);
}

std::vector<Bits> SS_Table::get_n_next_keys(const Bits& target_key, SS_Table_Entry_Filter entry_filter, uint32_t count, std::set<Bits>& dead_keys) const {
    std::vector<Bits> keys;
    if(target_key > this -> last_index) {
        return keys;
    }

    keys.reserve(count);

    std::ifstream index_ifstream(this -> index_file, std::ios::binary);
    if(index_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_FILE_MSG, this -> index_file.generic_string().c_str());
    }

    std::ifstream offset_ifstream(this -> index_offset_file, std::ios::binary);
    if(offset_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_INDEX_OFFSET_FILE_MSG, this -> index_offset_file.generic_string().c_str());
    }

    uint64_t larger_equal_index = this -> binary_search_nearest(index_ifstream, offset_ifstream, target_key, SS_TABLE_LARGER_OR_EQUAL);

    // left == record_count -> not found HARD ERROR MEANS LAST_INDEX WAS BAD
    if(larger_equal_index == this -> record_count) {
        throw std::runtime_error(SS_TABLE_KEYS_LARGER_THAN_FAILED_ERR_MSG);
    }

    uint64_t current_key_offset = 0;
    offset_ifstream.seekg(larger_equal_index * sizeof(uint64_t), offset_ifstream.beg);
    if(offset_ifstream.fail()) {
        throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_OFFSET_EOF_MSG, this -> index_offset_file.generic_string().c_str());
    }

    std::ifstream data_ifstream(this -> data_file, std::ios::binary);
    if(data_ifstream.fail()) {
        throw File_Exception(SS_TABLE_FAILED_TO_OPEN_DATA_FILE_MSG, this -> data_file.generic_string().c_str());
    }

    while(offset_ifstream.read(reinterpret_cast<char*>(&current_key_offset), sizeof(current_key_offset))) {
        index_ifstream.seekg(current_key_offset, index_ifstream.beg);
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        key_len_type current_key_length = 0;
        index_ifstream.read(reinterpret_cast<char*>(&current_key_length), sizeof(current_key_length));
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        std::string current_key(current_key_length, '\0');
        index_ifstream.read(&current_key[0], current_key_length);
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }
        // read the data offset
        uint64_t current_data_offset = 0;
        index_ifstream.read(reinterpret_cast<char*>(&current_data_offset), sizeof(current_data_offset));
        if(index_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_INDEX_EOF_MSG, this -> index_file.generic_string().c_str());
        }

        data_ifstream.seekg(current_data_offset, data_ifstream.beg);
        if(data_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
        }

        // read the data length
        uint64_t current_data_length = 0;
        data_ifstream.read(reinterpret_cast<char*>(&current_data_length), sizeof(current_data_length));
        if(data_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
        }

        // read the data itself
        std::string current_data(current_data_length, '\0');
        data_ifstream.read(&current_data[0], current_data_length);
        if(data_ifstream.fail()) {
            throw File_Exception(SS_TABLE_UNEXPECTED_DATA_EOF_MSG, this -> data_file.generic_string().c_str());
        }

        // check tombstone
        Entry current_entry(current_key, current_data);
        Bits curr_key(current_key);
        if(entry_filter == SS_Table_Entry_Filter::SS_TABLE_FILTER_ALL_ENTRIES) {
            keys.push_back(curr_key);
        }
        else if(entry_filter == SS_Table_Entry_Filter::SS_TABLE_FILTER_ALIVE_ENTRIES) {
            if(current_entry.is_deleted()) {
                dead_keys.emplace(curr_key);
            }
            else if(dead_keys.find(curr_key) != dead_keys.end()) {
                keys.push_back(curr_key);
            }
        }

        if(keys.size() == count) {
            return keys;
        }        
    }

    return keys;
}
