#include "../include/bits.h"

Bits::Bits(std::vector<uint8_t>& bitstream) : arr(bitstream) {

};
	
Bits::Bits(std::vector<char>& bitstream) {
	this -> arr.reserve(bitstream.size());

	for(bit_arr_size_type i = 0; i < bitstream.size(); ++i) {
		this -> arr.push_back(static_cast<uint8_t>(bitstream[i]));
	}
};

Bits::Bits(std::vector<uint32_t>& bitstream) {
	this -> arr.reserve(bitstream.size() * 4);
	
	for(bit_arr_size_type i = 0; i < bitstream.size(); ++i) {
		this -> arr.push_back(static_cast<uint8_t>(bitstream[i] & 0xff));
		this -> arr.push_back(static_cast<uint8_t>((bitstream[i] >> 8) & 0xff));
		this -> arr.push_back(static_cast<uint8_t>((bitstream[i] >> 16) & 0xff));
		this -> arr.push_back(static_cast<uint8_t>((bitstream[i] >> 24) & 0xff));
	}
};


Bits::Bits(std::string& bitstream) {
	this -> arr.reserve(bitstream.length());
	for(bit_arr_size_type i = 0; i < bitstream.length(); ++i) {
		this -> arr.push_back(static_cast<uint8_t>(bitstream[i]));
	}
};

Bits::Bits(Bits& org) {
	this -> arr = org.arr;
}

Bits::~Bits() {
}

bit_arr_size_type Bits::size() {
	return this -> arr.size();
}
		
std::vector<char> Bits::get_char_vector() {
	std::vector<char> ch_arr;
	ch_arr.reserve(this -> arr.size());

	for(bit_arr_size_type i = 0; i < this -> arr.size(); ++i) {
		ch_arr.push_back(static_cast<char>(this -> arr[i]));
	}

	return ch_arr;
}

std::string Bits::get_string_char() {
	std::string str;
	
	for(bit_arr_size_type i = 0; i < this -> arr.size(); ++i) {
		str += static_cast<char>(this -> arr[i]);
	}	

	return str;
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

void Bits::update_bits(std::vector<uint8_t>& new_bits) {
	if(new_bits.size() != this -> arr.size()) {
		throw std::length_error(BIT_LEN_UPDATE_ERR);
	}

	this -> arr = new_bits;
}
