#include <iostream>
#include <cassert>
#include <fstream>
#include <filesystem>
#include <sstream>
#include <cstring>
#include "../include/wal.h"

namespace fs = std::filesystem;

// Helper functions
std::string read_file_contents(const std::string& filepath) {
    std::ifstream file(filepath, std::ios::binary);
    std::ostringstream ss;
    ss << file.rdbuf();
    return ss.str();
}

bool file_exists(const std::string& filepath) {
    return fs::exists(filepath);
}

void cleanup_test_file(const std::string& filepath) {
    if (fs::exists(filepath)) {
        fs::remove(filepath);
    }
}

void setup_test_directory(const std::string& dir) {
    fs::create_directories(dir);
}

// Test functions
void test_default_constructor() {
    std::cout << "Running test_default_constructor..." << std::endl;
    
    Wal wal;
    
    assert(wal.get_wal_name() == "wal.log");
    assert(wal.get_entry_count() == 0);
    assert(wal.get_is_read_only() == false);
    
    std::string expected_location = DEFAULT_WAL_FOLDER_LOCATION;
    expected_location += "wal.log";
    assert(file_exists(expected_location));
    
    cleanup_test_file(expected_location);
    std::cout << "PASSED: test_default_constructor" << std::endl;
}

void test_parameterized_constructor() {
    std::cout << "Running test_parameterized_constructor..." << std::endl;
    
    std::string test_wal_name = "test_wal.log";
    std::string test_wal_location = "./test_wal_data/";
    std::string test_wal_full_path = test_wal_location + test_wal_name;
    
    setup_test_directory(test_wal_location);
    cleanup_test_file(test_wal_full_path);
    
    Wal wal(test_wal_name, test_wal_full_path);
    
    assert(wal.get_wal_name() == test_wal_name);
    assert(wal.get_wal_file_location() == test_wal_full_path);
    assert(wal.get_entry_count() == 0);
    assert(wal.get_is_read_only() == false);
    assert(file_exists(test_wal_full_path));
    
    cleanup_test_file(test_wal_full_path);
    std::cout << "PASSED: test_parameterized_constructor" << std::endl;
}

void test_getters() {
    std::cout << "Running test_getters..." << std::endl;
    
    std::string test_wal_name = "getter_test.log";
    std::string test_wal_full_path = "./test_wal_data/" + test_wal_name;
    
    setup_test_directory("./test_wal_data/");
    cleanup_test_file(test_wal_full_path);
    
    Wal wal(test_wal_name, test_wal_full_path);
    
    assert(wal.get_wal_name() == test_wal_name);
    assert(wal.get_wal_file_location() == test_wal_full_path);
    assert(wal.get_entry_count() == 0);
    assert(wal.get_is_read_only() == false);
    
    cleanup_test_file(test_wal_full_path);
    std::cout << "PASSED: test_getters" << std::endl;
}

void test_append_single_entry() {
    std::cout << "Running test_append_single_entry..." << std::endl;
    
    std::string test_wal_full_path = "./test_wal_data/append_test.log";
    setup_test_directory("./test_wal_data/");
    cleanup_test_file(test_wal_full_path);
    
    Wal wal("append_test.log", test_wal_full_path);
    
    std::ostringstream entry;
    entry << "Test entry 1\n";
    wal.append_entry(entry);
    
    std::string contents = read_file_contents(test_wal_full_path);
    assert(contents == "Test entry 1\n");
    
    cleanup_test_file(test_wal_full_path);
    std::cout << "PASSED: test_append_single_entry" << std::endl;
}

void test_append_multiple_entries() {
    std::cout << "Running test_append_multiple_entries..." << std::endl;
    
    std::string test_wal_full_path = "./test_wal_data/multiple_test.log";
    setup_test_directory("./test_wal_data/");
    cleanup_test_file(test_wal_full_path);
    
    Wal wal("multiple_test.log", test_wal_full_path);
    
    std::ostringstream entry1, entry2, entry3;
    entry1 << "Entry 1\n";
    entry2 << "Entry 2\n";
    entry3 << "Entry 3\n";
    
    wal.append_entry(entry1);
    wal.append_entry(entry2);
    wal.append_entry(entry3);
    
    std::string contents = read_file_contents(test_wal_full_path);
    assert(contents == "Entry 1\nEntry 2\nEntry 3\n");
    
    cleanup_test_file(test_wal_full_path);
    std::cout << "PASSED: test_append_multiple_entries" << std::endl;
}

void test_append_binary_data() {
    std::cout << "Running test_append_binary_data..." << std::endl;
    
    std::string test_wal_full_path = "./test_wal_data/binary_test.log";
    setup_test_directory("./test_wal_data/");
    cleanup_test_file(test_wal_full_path);
    
    Wal wal("binary_test.log", test_wal_full_path);
    
    std::ostringstream entry;
    unsigned char binary_data[] = {0x01, 0x02, 0x03, 0x04, 0x00, 0xFF};
    entry.write(reinterpret_cast<char*>(binary_data), sizeof(binary_data));
    
    wal.append_entry(entry);
    
    std::ifstream file(test_wal_full_path, std::ios::binary);
    unsigned char read_data[sizeof(binary_data)];
    file.read(reinterpret_cast<char*>(read_data), sizeof(binary_data));
    file.close();
    
    assert(std::memcmp(binary_data, read_data, sizeof(binary_data)) == 0);
    
    cleanup_test_file(test_wal_full_path);
    std::cout << "PASSED: test_append_binary_data" << std::endl;
}

void test_clear_entries() {
    std::cout << "Running test_clear_entries..." << std::endl;
    
    std::string test_wal_full_path = "./test_wal_data/clear_test.log";
    setup_test_directory("./test_wal_data/");
    cleanup_test_file(test_wal_full_path);
    
    Wal wal("clear_test.log", test_wal_full_path);
    
    std::ostringstream entry1, entry2;
    entry1 << "Entry 1\n";
    entry2 << "Entry 2\n";
    wal.append_entry(entry1);
    wal.append_entry(entry2);
    
    std::string contents_before = read_file_contents(test_wal_full_path);
    assert(!contents_before.empty());
    
    wal.clear_entries();
    
    std::string contents_after = read_file_contents(test_wal_full_path);
    assert(contents_after.empty());
    
    cleanup_test_file(test_wal_full_path);
    std::cout << "PASSED: test_clear_entries" << std::endl;
}

void test_entries_persist() {
    std::cout << "Running test_entries_persist..." << std::endl;
    
    std::string test_wal_full_path = "./test_wal_data/persist_test.log";
    setup_test_directory("./test_wal_data/");
    cleanup_test_file(test_wal_full_path);
    
    {
        Wal wal("persist_test.log", test_wal_full_path);
        std::ostringstream entry;
        entry << "Persistent entry\n";
        wal.append_entry(entry);
    }
    
    std::string contents = read_file_contents(test_wal_full_path);
    assert(contents == "Persistent entry\n");
    
    cleanup_test_file(test_wal_full_path);
    std::cout << "PASSED: test_entries_persist" << std::endl;
}

void test_append_empty_entry() {
    std::cout << "Running test_append_empty_entry..." << std::endl;
    
    std::string test_wal_full_path = "./test_wal_data/empty_test.log";
    setup_test_directory("./test_wal_data/");
    cleanup_test_file(test_wal_full_path);
    
    Wal wal("empty_test.log", test_wal_full_path);
    
    std::ostringstream empty_entry;
    wal.append_entry(empty_entry);
    
    std::string contents = read_file_contents(test_wal_full_path);
    assert(contents.empty());
    
    cleanup_test_file(test_wal_full_path);
    std::cout << "PASSED: test_append_empty_entry" << std::endl;
}

void test_multiple_clear_operations() {
    std::cout << "Running test_multiple_clear_operations..." << std::endl;
    
    std::string test_wal_full_path = "./test_wal_data/multi_clear_test.log";
    setup_test_directory("./test_wal_data/");
    cleanup_test_file(test_wal_full_path);
    
    Wal wal("multi_clear_test.log", test_wal_full_path);
    
    std::ostringstream entry1, entry2;
    entry1 << "Test\n";
    wal.append_entry(entry1);
    wal.clear_entries();
    
    entry2 << "Test2\n";
    wal.append_entry(entry2);
    wal.clear_entries();
    
    std::string contents = read_file_contents(test_wal_full_path);
    assert(contents.empty());
    
    cleanup_test_file(test_wal_full_path);
    std::cout << "PASSED: test_multiple_clear_operations" << std::endl;
}

void test_large_entry() {
    std::cout << "Running test_large_entry..." << std::endl;
    
    std::string test_wal_full_path = "./test_wal_data/large_test.log";
    setup_test_directory("./test_wal_data/");
    cleanup_test_file(test_wal_full_path);
    
    Wal wal("large_test.log", test_wal_full_path);
    
    std::ostringstream large_entry;
    std::string large_string(10000, 'A');
    large_entry << large_string;
    
    wal.append_entry(large_entry);
    
    std::string contents = read_file_contents(test_wal_full_path);
    assert(contents.size() == 10000);
    assert(contents == large_string);
    
    cleanup_test_file(test_wal_full_path);
    std::cout << "PASSED: test_large_entry" << std::endl;
}

void test_special_characters() {
    std::cout << "Running test_special_characters..." << std::endl;
    
    std::string test_wal_full_path = "./test_wal_data/special_test.log";
    setup_test_directory("./test_wal_data/");
    cleanup_test_file(test_wal_full_path);
    
    Wal wal("special_test.log", test_wal_full_path);
    
    std::ostringstream entry;
    entry << "Special: \n\t\r!@#$%^&*()";
    
    wal.append_entry(entry);
    
    std::string contents = read_file_contents(test_wal_full_path);
    assert(contents == "Special: \n\t\r!@#$%^&*()");
    
    cleanup_test_file(test_wal_full_path);
    std::cout << "PASSED: test_special_characters" << std::endl;
}

int main() {
    std::cout << "Starting WAL Unit Tests...\n" << std::endl;
    
    try {
        test_default_constructor();
        test_parameterized_constructor();
        test_getters();
        test_append_single_entry();
        test_append_multiple_entries();
        test_append_binary_data();
        test_clear_entries();
        test_entries_persist();
        test_append_empty_entry();
        test_multiple_clear_operations();
        test_large_entry();
        test_special_characters();
        
        std::cout << "\n==================================" << std::endl;
        std::cout << "All tests passed successfully!" << std::endl;
        std::cout << "==================================" << std::endl;
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}