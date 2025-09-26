#ifndef YSQL_WAL_H_INCLUDED
#define YSQL_WAL_H_INCLUDED

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>

#define DEFAULT_WAL_FOLDER_LOCATION "../data/wal/"

class Wal{
    private:
        std::string wal_name;
        std::string wal_file_location;
        unsigned int entry_count;
        bool is_read_only;
    public:
        // constructor using default file structure ../data/wal
        Wal();

        // constructor using custom file structure
        Wal(std::string _wal_name, std::string _wal_file_location);

        // destructor
        ~Wal();

        // returns wal file name as string
        std::string get_wal_name();

        // returns wal file location as string
        std::string get_wal_file_location();
        
        // returns entry count as an unsigned int
        unsigned int get_entry_count();

        // returns true if file is in read only mode
        bool get_is_read_only();

        // appends an entry to the wal file
        void append_entry(std::ostringstream entry);

        // removes all entries from the wal file
        void clear_entries();

        // method to retu top entry
};

#endif