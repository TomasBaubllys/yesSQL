#include "../include/file_exception.h"

File_Exception::File_Exception(const char* message, const char* filename) : message(message), filename(filename) {
	
}

File_Exception::~File_Exception() {

}

const char* File_Exception::what() const noexcept {
	return this -> message.c_str();
}

const char* File_Exception::what_file() const noexcept {
	return this -> filename.c_str();
} 
		
