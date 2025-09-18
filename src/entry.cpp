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
                    + sizeof(uint8_t)
                    + key.size()
                    + sizeof(uint16_t)
                    + value.size()
                    + sizeof(checksum);
};

Entry::Entry(Bits _key, Bits _value){
    entry_length = 0;
    tombstone_flag = 0;
    key = _key;
    value = _value;
    checksum = 0;

    calculate_checksum();
    calculate_entry_length();
};

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
}

void Entry::update_value(Bits _value){
    value = _value;
    calculate_checksum();
    calculate_entry_length();
    return;
}