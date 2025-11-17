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

// localisation for constant size_type between different systems
using key_len_type = uint16_t;
using value_len_type = uint32_t;

class Entry {
	private:
		uint64_t entry_length;
		uint8_t tombstone_flag;
		Bits key;
		Bits value;
		uint32_t checksum;

		//@brief uses crc32 hashing to calculate the checksum by concatenating the string conversions of key and value
		//@note updates the checksum member variable
		void calculate_checksum();

		//@brief calculates the length of the entry in Bytes
		//@note includes sizes of: entry_length, tombstone_flag, key_size, key data, value_size, value data, and checksum
		void calculate_entry_length();

	public:
		
		// NO DEFAULT CONSTRUCTOR
		// Constructors
		// -------------------------------------

		//THROWS
		//@brief constructs Entry from Bits key and value
		//@throws std::length_error if _key.size() > ENTRY_MAX_KEY_LEN
		//@throws std::length_error if _value.size() > ENTRY_MAX_VALUE_LEN
		//@note automatically calculates checksum and entry_length, sets tombstone_flag to 0
		Entry(Bits _key, Bits _value);
		// Copy constructor
		Entry(const Entry& other);
		//THROWS
		//@brief constructs Entry from stringstream containing serialized entry data
		//@throws std::runtime_error if any field fails to read from stream
		//@note reads in order: entry_length, tombstone_flag, key_size, key data, value_size, value data, checksum
		Entry(std::stringstream& file_entry);
		//THROWS
		//@brief constructs Entry from separate key and data strings
		//@throws std::runtime_error if file_entry_key is empty
		//@throws std::runtime_error if file_entry_data is too short for expected fields
		//@note data string contains: tombstone_flag, value_size, value data, checksum (excludes key data)
		Entry(std::string& file_entry_key, std::string& file_entry_data);
		// -------------------------------------
			
		~Entry();

		// METHODS
		// -------------------------------------

		//@returns the size of the entry length in Bytes
		uint64_t get_entry_length();
		//@returns true if the entry is marked for deletion (tombstone_flag is set)
		bool is_deleted();
		//@returns key as Bits class
		Bits get_key() const;
		//@returns value as Bits class
		Bits get_value() const;
		//@returns checksum as uint32_t
		uint32_t get_checksum();
		//@brief inverts current tombstone_flag value
		//@note toggles between ENTRY_TOMBSTONE_ON and ENTRY_TOMBSTONE_OFF
		void set_tombstone();
		//@brief sets tombstone_flag to _tombstone_flag value
		//@note converts bool to ENTRY_TOMBSTONE_ON (true) or ENTRY_TOMBSTONE_OFF (false)
		void set_tombstone(bool _tombstone_flag);
		//@brief sets new value and recalculates checksum and entry_length
		void update_value(Bits _value);
		//@returns true if checksum is still valid, false if data corruption appeared
		//@note recalculates checksum from current key and value, compares with stored checksum
		bool check_checksum();
		//@returns the length of the saved key as key_len_type (uint16_t)
		key_len_type get_key_length() const;

		value_len_type get_value_length() const;

		std::string get_value_string() const;

		std::string get_key_string() const; 

		//@brief serializes complete entry to ostringstream
		//@returns ostringstream containing: entry_length, tombstone_flag, key_size, key data, value_size, value data, checksum
		std::ostringstream get_ostream_bytes();
		//@brief serializes entry data without key to string using memcpy
		//@returns string containing: tombstone_flag, value_size, value data, checksum
		//@note excludes entry_length and key fields
		std::string get_string_data_bytes() const;
		//@brief serializes key bytes only to string using memcpy
		//@returns string containing raw key data
		std::string get_string_key_bytes() const;
		// -------------------------------------

		// OPERATORS
		// -------------------------------------

		//@brief compares Entry objects using key
		//@note uses Bits == operator
		inline bool operator==(const Entry& other) const {
			return this -> key == other.key;
		}
		//@brief compares Entry objects using key
		//@note uses Bits != operator
		inline bool operator!=(const Entry& other) const {
			return this -> key != other.key;
		}
		//@brief compares Entry objects using key
		//@note uses Bits > operator
		inline bool operator>(const Entry& other) const {
			return this -> key > other.key;
		}
		//@brief compares Entry objects using key
		//@note uses Bits < operator
		inline bool operator<(const Entry& other) const {
			return this -> key < other.key;
		}
		//@brief compares Entry objects using key
		//@note uses Bits <= operator
		inline bool operator<=(const Entry& other) const {
			return this -> key <= other.key;
		}
		//@brief compares Entry objects using key
		//@note uses Bits >= operator
		inline bool operator>=(const Entry& other) const {
			return this -> key >= other.key;
		}
		//@brief copy assignment operator
		//@returns reference to this Entry
		Entry& operator=(const Entry& other);
		// -------------------------------------
};

#endif
