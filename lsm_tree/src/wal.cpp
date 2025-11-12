#include "../include/wal.h"

Wal::Wal(){
   
    // move to define
    std::filesystem::path wal_dir = WAL_FOLDER_PATH;
    if (!std::filesystem::exists(wal_dir)) {
        std::filesystem::create_directories(wal_dir);
    }

    wal_name = "wal.log";
    wal_file_location = WAL_FOLDER_PATH + wal_name;
    entry_count = 0;
    is_read_only = false;

    wal_file.open(wal_file_location, std::ios::binary | std::ios::app);
    if (!wal_file.is_open()) {
        std::cerr << "Failed to open WAL file: " << wal_file_location << std::endl;
    }
}

Wal::Wal(std::string _wal_name, std::string _wal_file_location){
    wal_name = _wal_name;
    wal_file_location = _wal_file_location;
    entry_count = 0;
    is_read_only = false;

    wal_file.open(wal_file_location, std::ios::binary | std::ios::app);
    if (!wal_file.is_open()) {
        std::cerr << "Failed to open WAL file: " << wal_file_location << std::endl;
    }
}

Wal::~Wal(){
    clear_entries();
    if (wal_file.is_open()) {
        wal_file.close();
    }
}

std::string Wal::get_wal_name(){
    return wal_name;
}

std::string Wal::get_wal_file_location(){
    return wal_file_location;
}

unsigned int Wal::get_entry_count(){
    return entry_count;
}

bool Wal::get_is_read_only(){
    return is_read_only;
}

void Wal::append_entry(std::ostringstream& entry){
    if (!wal_file.is_open()) {
        std::cerr << "WAL file is not open" << std::endl;
        return;
    }
    
    if (entry.tellp() == 0) {
        std::cerr << "Entry stream is empty" << std::endl;
        return;
    }
    
    std::string content = entry.str();
    
    wal_file.write(content.c_str(), content.size());
    
    if (!wal_file.good()) {
        std::cerr << "Write failed" << std::endl;
    }
    
    wal_file.flush();
}

void Wal::clear_entries(){
    entry_count = 0;
    
    if (wal_file.is_open()) {
        wal_file.close();
    }
    
    wal_file.open(wal_file_location, std::ios::binary | std::ios::trunc);
    
    if (!wal_file.is_open()) {
        std::cerr << "Failed to reopen WAL file after clearing" << std::endl;
    }
}