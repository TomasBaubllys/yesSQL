#include "../include/bad_file_exception.h"

Bad_File_Exception::Bad_File_Exception(const char* message, const char* filename) : message(message), filename(filename) {
	
}

Bad_File_Exception::~Bad_File_Exception() {

}

const char* Bad_File_Exception::what() const noexcept {
	return this -> message.c_str();
}

const char* Bad_File_Exception::what_file() const noexcept {
	return this -> filename.c_str();
} 
		
