#include "../include/wal.h"

Wal::Wal(){
    wal_name = "wal.log";
    wal_file_location = DEFAULT_WAL_FOLDER_LOCATION + wal_name;
    entry_count = 0;
    is_read_only = false;

    std::ofstream wal_file(wal_file_location, std::ios::binary | std::ios::app);
    wal_file.close();
};

Wal::Wal(std::string _wal_name, std::string _wal_file_location){
    wal_name = _wal_name;
    wal_file_location = _wal_file_location;
    entry_count = 0;
    is_read_only = false;

    std::ofstream wal_file(wal_file_location, std::ios::binary | std::ios::app);
    wal_file.close();
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

// void Wal::append_entry(std::ostringstream& entry){
//     std::ofstream wal_file(wal_file_location, std::ios::binary | std::ios::app);
    
//     if (!wal_file.is_open()) {
//         std::cerr << "Failed to open WAL file: " << wal_file_location << std::endl;
//         return;
//     }
    
//     if (entry.tellp() == 0) {
//         std::cerr << "Entry stream is empty" << std::endl;
//         return;
//     }
    
//     std::string content = entry.str();
//     std::cout<<content<<std::endl;
//     wal_file.write(content.c_str(), content.size());
    
//     if (!wal_file.good()) {
//         std::cerr << "Write failed" << std::endl;
//     }
//     wal_file.close(); 
// };
void Wal::append_entry(std::ostringstream& entry){
    std::ofstream wal_file(wal_file_location, std::ios::binary | std::ios::app);
    
    if (!wal_file.is_open()) {
        std::cerr << "Failed to open WAL file: " << wal_file_location << std::endl;
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
    wal_file.close();
};

void Wal::clear_entries(){
    entry_count = 0;
    std::ofstream wal_file(wal_file_location, std::ios::binary | std::ios::trunc);
    wal_file.close();
};