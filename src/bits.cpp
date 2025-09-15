#include "../include/bits.h"

Bits::Bits(bit_arr_size_type &size) {
	arr.reserve(size);
};
	
Bits::~Bits() {
}

bit_arr_size_type Bits::size() {
	return this -> size;
}
		
//std::vector<uint8_t> Bits::get_char_vector() {
//	return std::vector<char>;
//}

std::string Bits::get_string_char() {
	return std::string;
}

std::vector<uint32_t> Bits::get_int_array() {
	uint8_t padding_size = this -> arr.size() % 4;
	uint32_t int_vec_size = this -> arr.size() / 4;

	std::vector<uint32_t> uint_vec;
	uint_vec.reserve(int_vec_size + (padding_size == 0? 0 : 1));	
	uint32_t curr_uint32 = 0;

	for(uint32_t i = 0; i < int_vec_size; i += 4) {
		curr_uint32 = 0;
		for(uint8_t j = 0; j < 4; ++j) {
			curr_uint32 |= this -> arr[i * 4 + j] << (8 * j);
		}
		uint_vec.push_back(curr_uint32);
	}
	
	if(padding_size != 0) {
		curr_uint32 = 0;
		for(uint8_t k = 0; k < padding_size; ++k) {
			curr_uint32 |= this -> arr[int_vec_size + k] << (8 * k); 
		}
	
		uint_vec.push_back(curr_uint32);	
	}

	return uint_vec;
	
}   

inline bool Bits::operator==(const Bits& other) {
	return this -> arr == other.arr;
}
