#ifndef YSQL_ENTRY_H_INCLUDED
#define YSQL_ENTRY_H_INCLUDED

#include "crc32.h"
#include <sstream>

#define ENTRY_CHECKSUM_MISMATCH "Entry was corrupted - checksum missmatch encountered\n"
#define ENTRY_PLACEHOLDER_VALUE "_placeholder_value"
#define ENTRY_PLACEHOLDER_KEY "_placeholder_key" 

class Entry {
  private:
    uint64_t entry_length;
    uint8_t tombstone_flag;
    Bits key;
    Bits value;
    uint32_t checksum;

    // uses cr32 hashing to calculate the ckecsum by concatenating the string conversions of key and value 
    void calculate_checksum();

    // calculates the length of the entry in Bytes
    void calculate_entry_length();

  public:

    // constructor, no default constructor exists
    Entry (Bits _key, Bits _value);

    // copy constructor
    Entry(const Entry& other);

    // stringstream constructor
    Entry (std::stringstream fileEntry);

    // desctructor
    ~Entry();

    // returns the size of the entry length in Bytes
    uint64_t get_entry_length();

    // returns true if the entry is marked for deletion
    bool is_deleted();

    // returns key as Bits class
    Bits get_key();
    
    // returns value as Bits class
    Bits get_value();

    // returns checksum as Bytes
    uint32_t get_checksum();

    // function inverts current tombstone_flag value
    void set_tombstone();

    // function sets tombstone_flag to is_deleted value
    void set_tombstone(bool _tombstone_flag);

    // sets new value and calculates new checksum
    void update_value(Bits _value);

    // comparison operators 
    inline bool operator==(const Entry& other) {
			return this -> key == other.key;
		};

		inline bool operator>(const Entry& other) {
			return this -> key > other.key;
		};

		inline bool operator<(const Entry& other) {
			return this -> key < other.key;
		};	

		inline bool operator!=(const Entry& other) {
			return this -> key != other.key;
		};	

		inline bool operator<=(const Entry& other) {
			return this -> key <= other.key;
		};

		inline bool operator>=(const Entry& other) {
			return this -> key >= other.key;
    };

    // copy operator
    Entry operator=(const Entry& other);

    //function to dump bytes
    std::ostringstream get_ostream_bytes();

    // returns true if checksum is still valid, false if data corruption appeared
    bool check_checksum();

    // returns the length of the saved key
    bit_arr_size_type get_key_length();
};

#endif
