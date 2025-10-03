#include "../include/entry.h"

void Entry::calculate_checksum(){
    std::string key_string = key.get_string_char();
    std::string value_string = value.get_string_char();
    
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

// FIX FIX FIX
// ADD LIMITS TO KEY SIZE TO 16 UINT
// ADD LIMITS TO VALUE SIZE TO 32 UINT
Entry::Entry(Bits _key, Bits _value) : key(_key), value(_value){
    entry_length = 0;
    tombstone_flag = 0;
    checksum = 0;

    calculate_checksum();
    calculate_entry_length();
};

// Entry::Entry(std::stringstream fileEntry){

// };

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

Bits Entry::get_key(){
    return key;
};

Bits Entry::get_value(){
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

    bit_arr_size_type key_size = key.size();
    bit_arr_size_type value_size = value.size();

    ostream_bytes.write(reinterpret_cast<const char*>(&entry_length), sizeof(entry_length));
    ostream_bytes.write(reinterpret_cast<const char*>(&tombstone_flag), sizeof(tombstone_flag));
    ostream_bytes.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
    ostream_bytes.write(key.get_string_char().data(), key_size);
    ostream_bytes.write(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
    ostream_bytes.write(value.get_string_char().data(), value_size);
    ostream_bytes.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
    

    return ostream_bytes;
};

Entry::Entry(std::stringstream& fileEntry) : key(ENTRY_PLACEHOLDER_KEY), value(ENTRY_PLACEHOLDER_VALUE){
    fileEntry.read(reinterpret_cast<char*>(&entry_length), sizeof(entry_length));
    fileEntry.read(reinterpret_cast<char*>(&tombstone_flag), sizeof(tombstone_flag));
    bit_arr_size_type key_size;
    fileEntry.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
    std::string key_str(key_size, '\0');
    fileEntry.read(&key_str[0], key_size);
    key = Bits(key_str);
    bit_arr_size_type value_size;
    fileEntry.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));
    std::string value_str(value_size, '\0');
    fileEntry.read(&value_str[0], value_size);
    value = Bits(value_str);
    fileEntry.read(reinterpret_cast<char*>(&checksum), sizeof(checksum));
}

bool Entry::check_checksum() {
    std::string string_to_hash = this -> key.get_string_char();
    string_to_hash += this -> value.get_string_char();
    uint32_t new_checksum = crc32(string_to_hash);

    return this -> checksum == new_checksum;
};

bit_arr_size_type Entry::get_key_length() {
    return this -> key.size();
}
