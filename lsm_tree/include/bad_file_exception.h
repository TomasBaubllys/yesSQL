#ifndef YSQL_FILE_EXCEPTION_H_INCLUDED
#define YSQL_FILE_EXCEPTION_H_INCLUDED

#include <exception>
#include <string>

// Used by SS_Table, gets thrown when an error was found in a file, or the file name was bad
class File_Exception : public std::exception {
	private:
		std::string message;
		std::string filename;
	public:
		File_Exception(const char* message, const char* filename);

		~File_Exception();

		const char* what() const noexcept;
		const char* what_file() const noexcept; 
		
};

#endif // YSQL_FILE_EXCEPTION_H_INCLUDED
