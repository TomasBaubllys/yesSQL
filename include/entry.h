#ifndef YSQL_ENTRY_H_INCLUDED
#define YSQL_ENTRY_H_INCLUDED

#include "crc32.h"
#include <sstream>
#include <stdexcept>

#define ENTRY_CHECKSUM_MISMATCH "Entry was corrupted - checksum missmatch encountered\n"
#define ENTRY_PLACEHOLDER_VALUE "_placeholder_value"
#define ENTRY_PLACEHOLDER_KEY "_placeholder_key" 
// limit size of the key to uint16_t
#define ENTRY_MAX_KEY_LEN 0xffff
#define ENTRY_MAX_KEY_LEN_EXCEEDED_ERR_MSG "Maximum key length exceeded\n"

// limit the size of the value to uint32_t
#define ENTRY_MAX_VALUE_LEN 0xffffffff
#define ENTRY_MAX_VALUE_LEN_EXCEEDED_ERR_MSG "Entry maximum value length exceeded\n"
#define ENTRY_FAILED_READ_LENGTH_MSG "Entry failed to read entry_length\n"
#define ENTRY_FAILED_READ_KEY_MSG "Entry failed to read key\n"
#define ENTRY_FAILED_READ_KEY_LENGTH_MSG "Entry failed to read key_length\n"
#define ENTRY_FAILED_READ_VALUE_MSG "Entry failed to read value\n"
#define ENTRY_FAILED_READ_VALUE_LENGTH_MSG "Entry failed to read value_length\n"
#define ENTRY_FAILED_READ_TOMBSTONE_FLAG_MSG "Entry failed to read tombstone_flag\n"
#define ENTRY_FAILED_READ_CHECKSUM_MSG "Entry failed to read checksum\n"
#define ENTRY_DATA_TOO_SHORT_ERR_MSG "Entry data string is too short\n"

#define ENTRY_TOMBSTONE_OFF 0
#define ENTRY_TOMBSTONE_ON 1

using key_len_type = uint16_t;
using value_len_type = uint32_t;

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
    // THROWS
    Entry (Bits _key, Bits _value);

    // copy constructor
    Entry(const Entry& other);

    // THROWS
    Entry(std::stringstream& file_entry);

    // THROWS
    Entry(std::string& file_entry_key, std::string& file_entry_data);

    // desctructor
    ~Entry();

    // returns the size of the entry length in Bytes
    uint64_t get_entry_length();

    // returns true if the entry is marked for deletion
    bool is_deleted();

    // returns key as Bits class
    Bits get_key() const;
    
    // returns value as Bits class
    Bits get_value() const;

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

    // function to dump bytes except key
    std::string get_string_data_bytes() const;

    // function to dump key bytes only
    std::string get_string_key_bytes() const;

    // returns true if checksum is still valid, false if data corruption appeared
    bool check_checksum();

    // returns the length of the saved key
    key_len_type get_key_length() const;
};

#endif
