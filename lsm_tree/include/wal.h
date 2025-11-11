#ifndef YSQL_WAL_H_INCLUDED
#define YSQL_WAL_H_INCLUDED

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <filesystem>

#define WAL_FOLDER_PATH "./data/val/wal/"

class Wal{
    private:
        std::string wal_name;
        std::string wal_file_location;
        unsigned int entry_count;
        bool is_read_only;

		std::ofstream wal_file;
    public:
        public:
		
		// Constructors
		// -------------------------------------

		//@brief constructs Wal using default file structure
		//@note sets wal_name to "wal.log", wal_file_location to DEFAULT_WAL_FOLDER_LOCATION + wal_name
		//@note initializes entry_count to 0 and is_read_only to false
		Wal();
		//@brief constructs Wal using custom file structure
		//@note initializes entry_count to 0 and is_read_only to false
		Wal(std::string _wal_name, std::string _wal_file_location);
		// -------------------------------------
			
		~Wal();

		// METHODS
		// -------------------------------------

		//@returns wal file name as string
		std::string get_wal_name();
		//@returns wal file location as string
		std::string get_wal_file_location();
		//@returns entry count as unsigned int
		unsigned int get_entry_count();
		//@returns true if file is in read only mode
		bool get_is_read_only();
		//@brief appends an entry to the wal file
		//@note opens file in binary append mode and writes ostringstream buffer
		void append_entry(std::ostringstream& entry);
		//@brief removes all entries from the wal file
		void clear_entries();
		// -------------------------------------
};

#endif