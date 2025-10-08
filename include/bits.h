/*

Last updated 9/18/2025
*/

#ifndef YSQL_BITS_H_INCLUDED
#define YSQL_BITS_H_INCLUDED

#define BIT_LEN_UPDATE_ERR "Bit stream lengths do not match!"

#include <string>
#include <algorithm>
#include <cmath>
#include <vector>
#include <cstdint>
#include <stdexcept>

using bit_arr_size_type = std::vector<uint8_t>::size_type;

class Bits {
	private:
		std::vector<uint8_t> arr;
	public:
		// constructors, no default constructor exists
		Bits(std::vector<uint8_t>& bitstream);
	
		Bits(std::vector<char>& bitstream);
	
		Bits(std::vector<uint32_t>& bitstream);
		
		Bits(std::string bitstream);
	
		Bits(const Bits& org);
			
		// desctructor
		~Bits(); 

		// returns the size of the saved bit array
		bit_arr_size_type size();
		
		// returns the bits as a character vector
		std::vector<char> get_char_vector();

		// returns the bits as a string
		std::string get_string_char();

		// returns bits as a string containing only 0 and 1s
		// std::string get_string_bits();

		// returns the bits as and integer vector, if bits dont fill the last integer it will be padded with 0 bits to be interpreted as little endian
		std::vector<uint32_t> get_int_array();   
		
		// compares to bit objects bit by bit
		inline bool operator==(const Bits& other) {
			return this -> arr == other.arr;
		}

		inline bool operator>(const Bits& other) {
			return this -> arr > other.arr;
		}	

		inline bool operator<(const Bits& other) {
			return this -> arr < other.arr;
		}		

		inline bool operator!=(const Bits& other) {
			return this -> arr != other.arr;
		}		

		inline bool operator<=(const Bits& other) {
			return this -> arr <= other.arr;
		}	

		inline bool operator>=(const Bits& other) {
			return this -> arr >= other.arr;
		}	

		// returns 0 if equal, 1 if Bits is greater, -1 if string is greater
		int8_t compare_to_str(const std::string& other);

		// update the bitstream !throws!
		void update_bits(std::vector<uint8_t>& new_bits);
};

#endif
