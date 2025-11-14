#ifndef YSQL_BITS_H_INCLUDED
#define YSQL_BITS_H_INCLUDED

#include <string>
#include <algorithm>
#include <cmath>
#include <vector>
#include <cstdint>
#include <stdexcept>

#define BIT_LEN_UPDATE_ERR "Bit stream lengths do not match!"

// localisation for constant size_type between different systems
using bit_arr_size_type = std::vector<uint8_t>::size_type;

class Bits {
	private:
		//Content Bytes
		// std::vector<uint8_t> arr;
		std::string arr;
	public:
		
		// NO DEFAULT CONSTRUCTOR
		// Constructors
		// -------------------------------------

		Bits(std::vector<uint8_t>& bitstream);
		Bits(std::vector<char>& bitstream);
		//@note converts uint32_t to uint8_t
		Bits(std::vector<uint32_t>& bitstream);
		Bits(std::string bytestream);
		// Copy constructor
		Bits(const Bits& org);
		// -------------------------------------
			
		~Bits(); 

		// METHODS
		// -------------------------------------

		//@returns the size of the saved bit array
		bit_arr_size_type size() const;
		//@brief casts saved uint8_t values as char
		//@returns saved bits as character std::vector
		std::vector<char> get_char_vector() const;
		//@brief casts saved uint8_t values as char and appends to string
		//@returns saved bits as string
		std::string get_string() const;
		//@returns std::vector<unsigned int>
		//@note if bits dont fill the last integer it will be padded with 0 bits to be interpreted as little endian 
		std::vector<uint32_t> get_int_vector() const;
		//@brief compares Bits object to std::string
		//@returns 0 if equal, 1 if Bits is greater, -1 if string is greater
		int8_t compare_to_str(const std::string& other) const;
		//THROWS
		//@brief copies input vector<unit8_t> to stored bits
		//throws std::length_error if input vector.size() is not equal to stored bits arr.size() 
		//@note new_bits must be the same length as stored bits array length
		void update_bits(std::vector<uint8_t> new_bits);
		// -------------------------------------
		
		// OPERATORS
		// -------------------------------------

		//@brief uses std::vector == operator
		inline bool operator==(const Bits& other) const {
			return this -> arr == other.arr;
		}
		//@brief uses std::vector != operator
		inline bool operator!=(const Bits& other) const {
			return this -> arr != other.arr;
		}		
		//@brief compares std::vector.size() between Bits
		//@returns true if this.arr.size() > other.arr.size()
		//@note if size of Bits arr is equal, compares values
		inline bool operator>(const Bits& other) const {
			return this -> arr > other.arr;
		}	
		//@brief compares std::vector.size() between Bits
		//@returns true if this.arr.size() < other.arr.size()
		//@note if size of Bits arr is equal, compares values
		inline bool operator<(const Bits& other) const {
			return this -> arr < other.arr;
		}		
		//@brief compares std::vector.size() between Bits
		//@returns true if this.arr.size() < other.arr.size()
		//@note if size of Bits arr is equal, compares values using std::vector <= operator
		inline bool operator<=(const Bits& other) const {
			return this -> arr <= other.arr;
		}	
		//@brief compares std::vector.size() between Bits
		//@returns true if this.arr.size() > other.arr.size()
		//@note if size of Bits arr is equal, compares values using std::vector >= operator
		inline bool operator>=(const Bits& other) const {
			return this -> arr >= other.arr;

		}
		// -------------------------------------
};

#endif
