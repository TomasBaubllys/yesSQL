#include "../include/mem_table.h"

Mem_Table::Mem_Table(){
    avl_tree = AVL_Tree();
    entry_array_length = 0;
    total_mem_table_size = 0;
};

Mem_Table::Mem_Table(Wal& wal){
    avl_tree = AVL_Tree();
    entry_array_length = 0;
    total_mem_table_size = 0;

    std::string wal_path = wal.get_wal_file_location();

    std::ostringstream buffer;
    {
        std::ifstream input(wal_path, std::ios::binary);
        if (!input.is_open()) {
            throw std::runtime_error("Failed to open WAL file: " + wal_path);
        }
        buffer << input.rdbuf();
    }

    std::istringstream in(buffer.str());
    in.seekg(0, std::ios::beg);

    uint64_t entry_length = 0;
    while (in.read(reinterpret_cast<char*>(&entry_length), sizeof(entry_length))) {

        if (entry_length <= sizeof(entry_length)) {
            throw std::runtime_error("Invalid entry length in WAL file.");
        }

        // Read remaining entry bytes (already know total size)
        std::string entry_bytes(entry_length - sizeof(entry_length), '\0');
        if (!in.read(entry_bytes.data(), entry_bytes.size())) {
            throw std::runtime_error("Unexpected EOF while reading entry data.");
        }

        // Reconstruct full serialized entry (prepend length field)
        std::string full_entry;
        full_entry.reserve(entry_length);
        full_entry.append(reinterpret_cast<char*>(&entry_length), sizeof(entry_length));
        full_entry.append(entry_bytes);

        std::stringstream entry_stream(full_entry);

        try {
            Entry entry(entry_stream);

            avl_tree.insert(entry.get_key(), entry.get_value());
            entry_array_length++;
            total_mem_table_size += entry.get_entry_length();

        } catch (const std::exception& e) {
            throw std::runtime_error(std::string("Error reading WAL entry: ") + e.what());
        }
    }
};

Mem_Table::~Mem_Table(){
    //destroy AVL
};

int Mem_Table::get_entry_array_length(){
    return entry_array_length;
};

uint64_t Mem_Table::get_total_mem_table_size(){
    return total_mem_table_size;
};

bool Mem_Table::insert_entry(Entry entry){
    try{
        avl_tree.insert(entry);
        entry_array_length++;
        total_mem_table_size += entry.get_entry_length();
        return true;
    }
    catch(const std::exception& e){
        std::cerr << e.what() << std::endl;
        return false;
    }
    
};

bool Mem_Table::remove_find_entry(Bits key){
    bool is_entry_found;
    try{
        // for now create a copy, we can play with memory in the future
        Entry entry = avl_tree.search(key, is_entry_found);
        if(is_entry_found && !entry.is_deleted()){
            entry.set_tombstone(true);
            avl_tree.insert(entry);
        };
        return true;
    }
    catch(const std::exception& e){
        return false;
    }
    
};

bool Mem_Table::remove_entry(Entry& entry){
    try{
        if(!entry.is_deleted()){
            entry.set_tombstone(true);
            avl_tree.insert(entry);
        };
        return true;
    }
    catch(const std::exception& e){
        return false;
    }
    
};

bool Mem_Table::reconstruct_from_Wal(){

};

Entry Mem_Table::find(Bits key, bool& found){
    Entry found_entry = avl_tree.search(key, found);

    return found_entry;
};

bool Mem_Table::is_full(){

    if(total_mem_table_size >= MEM_TABLE_BYTES_MAX_SIZE){
        return true;
    }

    return false;
};

std::vector<Bits> Mem_Table::get_keys(){
    std::vector<Bits> keys;

    for(Entry entry : avl_tree.inorder()){
        keys.emplace_back(entry.get_key());
    }

    return keys;
};

std::vector<Entry> Mem_Table::dump_entries(){
    std::vector<Entry> results;
    results = avl_tree.inorder();

    return results;
};

void Mem_Table::make_empty() {
    this -> avl_tree.make_empty();
    this -> entry_array_length = 0;
    this -> total_mem_table_size = 0;
}