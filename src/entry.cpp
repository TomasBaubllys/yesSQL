#include "../include/entry.h"

void Entry::calculate_checksum(){
    std::string key_string = key.get_string_char();
    std::string value_string = value.get_string_char();
    
    std::string string_to_hash = key_string + value_string;
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
Entry::Entry(Bits _key, Bits _value){
    entry_length = 0;
    tombstone_flag = 0;
    key = _key;
    value = _value;
    checksum = 0;

    calculate_checksum();
    calculate_entry_length();
};

// Entry::Entry(std::stringstream fileEntry){

// };

Entry::~Entry(){
};

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
    tombstone_flag = -tombstone_flag;
    return;
};

void Entry::set_tombstone(bool _tombstone_flag){
    tombstone_flag = _tombstone_flag;
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

std::ostringstream Entry::get_ostream_bits(){
    std::ostringstream ostream_bytes;

    ostream_bytes<<entry_length
                 <<tombstone_flag
                 <<key.size()
                 <<key.get_string_char()
                 <<value.size()
                 <<value.get_string_char()
                 <<checksum;

    return ostream_bytes;
};