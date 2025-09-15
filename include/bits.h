#ifndef YSQL_BITS_H_INCLUDED
#define YSQL_BITS_H_INCLUDED

#include <string>
#include <algorithm>
#include <cmath>
#include <vector>
#include <cstdint>

using bit_arr_size_type = std::vector<uint8_t>::size_type;

class Bits {
	private:
		std::vector<uint8_t> arr;
	public:
		// constructor, no default constructor exists
		Bits(bit_arr_size_type &size);
	
		// desctructor
		~Bits(); 

		// returns the size of the saved bit array
		bit_arr_size_type size();
		
		// returns the bits as a character vector
		std::vector<uint8_t> get_char_vector();

		// returns the bits as a string
		std::string get_string_char();

		// returns bits as a string containing only 0 and 1s
		std::string get_string_bits();

		// returns the bits as and integer vector, if bits dont fill the last integer it will be padded with 0 bits to be interpreted as little endian
		std::vector<uint32_t> get_int_array();   
		
		inline bool operator==(const Bits& other);
};

#endif
