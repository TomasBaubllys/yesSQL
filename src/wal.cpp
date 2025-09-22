#include "../include/wal.h"

Wal::Wal(){
    wal_name = "wal.log";
    wal_file_location = DEFAULT_WAL_FOLDER_LOCATION + wal_name;
    entry_count = 0;
    is_read_only = false;
};

Wal::Wal(std::string _wal_name, std::string _wal_file_location){
    wal_name = _wal_name;
    wal_file_location = _wal_file_location;
    entry_count = 0;
    is_read_only = false;
};

Wal::~Wal(){
    clear_entries();
};

std::string Wal::get_wal_name(){
    return wal_name;
};

std::string Wal::get_wal_file_location(){
    return wal_file_location;
};

unsigned int Wal::get_entry_count(){
    return entry_count;
};

bool Wal::get_is_read_only(){
    return is_read_only;
};

void Wal::append_entry(std::ostringstream entry){
    std::ofstream wal_file(wal_file_location, std::ios::binary | std::ios::app);

    wal_file<<entry.rdbuf();

    return;
};

void Wal::clear_entries(){
    std::ofstream wal_file(wal_file_location, std::ios::binary | std::ios::trunc);
};