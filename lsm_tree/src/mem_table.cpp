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
    std::ifstream input(wal_path, std::ios::binary);
    
    if (!input.is_open()) {
        throw std::runtime_error("Failed to open WAL file: " + wal_path);
    }

    uint64_t entry_length = 0;
    
    while (input.read(reinterpret_cast<char*>(&entry_length), sizeof(entry_length))) {
        
        if (entry_length <= sizeof(entry_length)) {
            throw std::runtime_error("Invalid entry length in WAL file.");
        }

        std::string full_entry_bytes(entry_length, '\0');
        
        memcpy(full_entry_bytes.data(), &entry_length, sizeof(entry_length));
        
        if (!input.read(full_entry_bytes.data() + sizeof(entry_length), 
                       entry_length - sizeof(entry_length))) {
            throw std::runtime_error("Unexpected EOF while reading entry data.");
        }

        std::stringstream entry_stream(full_entry_bytes);

        try {
            Entry entry(entry_stream);

            bool entry_key_exists = false;
            Bits entry_key = entry.get_key();
            Entry found_entry = avl_tree.search(entry_key, entry_key_exists);

            if(entry_key_exists){
                total_mem_table_size -= found_entry.get_entry_length();
                total_mem_table_size += entry.get_entry_length();
            }else{
                entry_array_length++;
                total_mem_table_size+=entry.get_entry_length();
            }

            avl_tree.insert(entry);

        } catch (const std::exception& e) {
            throw std::runtime_error(std::string("Error reading WAL entry: ") + e.what());
        }
    }
}

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
        bool entry_key_exists = false;
        Bits entry_key = entry.get_key();
        Entry found_entry = avl_tree.search(entry_key, entry_key_exists);

        if(entry_key_exists){
            total_mem_table_size -= found_entry.get_entry_length();
            total_mem_table_size += entry.get_entry_length();
        }else{
            entry_array_length++;
            total_mem_table_size+=entry.get_entry_length();
        }

        avl_tree.insert(entry);
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
};

std::vector<Bits> Mem_Table::get_keys_larger_than_alive(const Bits& key, uint32_t count, std::set<Bits>& dead_keys){
    return this -> avl_tree.get_keys_larger_than_alive(key, count, dead_keys);
};

std::vector<Entry> Mem_Table::get_entries_larger_than_alive(const Bits& key, uint32_t count, std::set<Bits>& dead_keys){
    return this -> avl_tree.get_entries_larger_than_alive(key, count, dead_keys);
};
