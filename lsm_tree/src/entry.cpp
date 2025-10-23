#include "../include/entry.h"
#include <cstring>
#include <stdexcept>

void Entry::calculate_checksum(){
    std::string key_string = key.get_string();
    std::string value_string = value.get_string();
    
    std::string string_to_hash = key_string;
    string_to_hash += value_string;
    uint32_t hashed_string = crc32(string_to_hash);

    checksum = hashed_string;

    return;
};

void Entry::calculate_entry_length(){
    entry_length =  sizeof(uint64_t)
                    + sizeof(tombstone_flag)
                    + sizeof(key.size())
                    + key.size()
                    + sizeof(value.size())
                    + value.size()
                    + sizeof(checksum);
};

Entry::Entry(Bits _key, Bits _value) : key(ENTRY_PLACEHOLDER_KEY), value(ENTRY_PLACEHOLDER_VALUE){
    if(_key.size() > ENTRY_MAX_KEY_LEN) {
      throw std::length_error(ENTRY_MAX_KEY_LEN_EXCEEDED_ERR_MSG);  
    }

    if(_value.size() > ENTRY_MAX_VALUE_LEN) {
      throw std::length_error(ENTRY_MAX_VALUE_LEN_EXCEEDED_ERR_MSG);
    }

    this -> key = _key;
    this -> value = _value;

    entry_length = 0;
    tombstone_flag = 0;
    checksum = 0;

    calculate_checksum();
    calculate_entry_length();
};

Entry::~Entry(){
};

Entry::Entry(const Entry& other) : key(other.key), value(other.value){
    entry_length = other.entry_length;
    tombstone_flag = other.tombstone_flag;
    checksum = other.checksum;
}

uint64_t Entry::get_entry_length(){
    return entry_length;
};

bool Entry::is_deleted(){
    return tombstone_flag;
};

Bits Entry::get_key() const {
    return key;
};

Bits Entry::get_value() const {
    return value;
};

uint32_t Entry::get_checksum(){
    return checksum;
};

void Entry::set_tombstone(){
    tombstone_flag = tombstone_flag == ENTRY_TOMBSTONE_OFF? ENTRY_TOMBSTONE_ON : ENTRY_TOMBSTONE_OFF;
    return;
};

void Entry::set_tombstone(bool _tombstone_flag){
    tombstone_flag = _tombstone_flag? ENTRY_TOMBSTONE_ON : ENTRY_TOMBSTONE_OFF;
    return;
};

void Entry::update_value(Bits _value){
    value = _value;
    calculate_checksum();
    calculate_entry_length();
    return;
};

Entry Entry::operator=(const Entry& other){
    entry_length = other.entry_length;
    tombstone_flag = other.tombstone_flag;
    key = other.key;
    value = other.value;
    checksum = other.checksum;

    return *this;
};

std::ostringstream Entry::get_ostream_bytes(){
    std::ostringstream ostream_bytes;

    key_len_type key_size = key.size();
    value_len_type value_size = value.size();

    ostream_bytes.write(reinterpret_cast<const char*>(&entry_length), sizeof(entry_length));
    ostream_bytes.write(reinterpret_cast<const char*>(&tombstone_flag), sizeof(tombstone_flag));
    ostream_bytes.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
    ostream_bytes.write(key.get_string().data(), key_size);
    ostream_bytes.write(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
    ostream_bytes.write(value.get_string().data(), value_size);
    ostream_bytes.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
    

    return ostream_bytes;
};

std::string Entry::get_string_data_bytes() const {

    value_len_type value_len = this -> value.size();
    std::string value_str = this -> value.get_string();

    // key_len_type key_size = this -> key.size();
    // value_len_type value_size = this -> value.size();

    uint64_t total_record_size = sizeof(tombstone_flag) +
                                sizeof(value_len) +
                                value_len +
                                sizeof(checksum);


    std::string raw_bytes(total_record_size, '\0');
    char* ptr = raw_bytes.data();

    // write the tombstone_flag
    memcpy(ptr, &tombstone_flag, sizeof(tombstone_flag));
    ptr += sizeof(tombstone_flag);

    // write the value size
    memcpy(ptr, &value_len, sizeof(value_len));
    ptr += sizeof(value_len);

    // write the value
    memcpy(ptr, &value_str[0], value_len);
    ptr += value_len;

    // write the checksum
    memcpy(ptr, &checksum, sizeof(checksum));
    return raw_bytes;
}

std::string Entry::get_string_key_bytes() const {
    key_len_type key_size = this -> key.size();
    std::string key_str = this -> key.get_string();
    std::string raw_bytes(key_size, '\0');

    char* ptr = raw_bytes.data();

    memcpy(ptr, &key_str[0], key_size);

    return raw_bytes;
}

// ADD ERROR CHECKING FOR UNEXPECTED EOF
Entry::Entry(std::stringstream& file_entry) : key(ENTRY_PLACEHOLDER_KEY), value(ENTRY_PLACEHOLDER_VALUE){
    if(!file_entry.read(reinterpret_cast<char*>(&entry_length), sizeof(entry_length))) {
        throw std::runtime_error(ENTRY_FAILED_READ_VALUE_MSG);
    }

    if(!file_entry.read(reinterpret_cast<char*>(&tombstone_flag), sizeof(tombstone_flag))) {
        throw std::runtime_error(ENTRY_FAILED_READ_TOMBSTONE_FLAG_MSG);
    }

    key_len_type key_size;
    if(!file_entry.read(reinterpret_cast<char*>(&key_size), sizeof(key_size))) {
        throw std::runtime_error(ENTRY_FAILED_READ_KEY_LENGTH_MSG);
    }

    std::string key_str(key_size, '\0');
    if(!file_entry.read(&key_str[0], key_size)) {
        throw std::runtime_error(ENTRY_FAILED_READ_KEY_MSG);
    }

    key = Bits(key_str);
    value_len_type value_size;

    if(!file_entry.read(reinterpret_cast<char*>(&value_size), sizeof(value_size))) {
        throw std::runtime_error(ENTRY_FAILED_READ_VALUE_LENGTH_MSG);
    }

    std::string value_str(value_size, '\0');

    if(!file_entry.read(&value_str[0], value_size)) {
        throw std::runtime_error(ENTRY_FAILED_READ_VALUE_MSG);
    }

    value = Bits(value_str);
    if(!file_entry.read(reinterpret_cast<char*>(&checksum), sizeof(checksum))) {
        throw std::runtime_error(ENTRY_FAILED_READ_CHECKSUM_MSG);
    }
}

// ADD ERROR CHECKING FOR UNEXPECTED EOF
Entry::Entry(std::string& file_entry_key, std::string& file_entry_data) : key(ENTRY_PLACEHOLDER_KEY), value(ENTRY_PLACEHOLDER_VALUE) {
    // check if key is somewhat valid
    if(file_entry_key.empty()) {
        throw std::runtime_error(ENTRY_FAILED_READ_KEY_MSG);
    }

    if(file_entry_data.size() < sizeof(tombstone_flag) + sizeof(value_len_type)) {
        throw std::runtime_error(ENTRY_DATA_TOO_SHORT_ERR_MSG);
    }

    char* data_ptr = file_entry_data.data();
    memcpy(&tombstone_flag, &data_ptr, sizeof(tombstone_flag));
    data_ptr += sizeof(tombstone_flag);

    // read the size of the data
    value_len_type value_len = 0;
    memcpy(&value_len, data_ptr, sizeof(value_len));
    data_ptr += sizeof(value_len);

    if(file_entry_data.size() < sizeof(tombstone_flag) + sizeof(value_len_type) + value_len) {
        throw std::runtime_error(ENTRY_DATA_TOO_SHORT_ERR_MSG);
    }

    // read the data
    std::string value_str(value_len, '\0');
    memcpy(&value_str[0], data_ptr, value_len);
    data_ptr += value_len;

    if(file_entry_data.size() < sizeof(tombstone_flag) + sizeof(value_len_type) + value_len + sizeof(checksum)) {
        throw std::runtime_error(ENTRY_DATA_TOO_SHORT_ERR_MSG);
    }

    // read the checksum
    memcpy(&checksum, data_ptr, sizeof(checksum));

    this -> key = Bits(file_entry_key);
    this -> value = Bits(value_str);
}


bool Entry::check_checksum() {
    std::string string_to_hash = this -> key.get_string();
    string_to_hash += this -> value.get_string();
    uint32_t new_checksum = crc32(string_to_hash);

    return this -> checksum == new_checksum;
};

key_len_type Entry::get_key_length() const {
    return this -> key.size();
}
