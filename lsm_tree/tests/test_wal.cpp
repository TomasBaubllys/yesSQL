#include "../include/mem_table.h"
#include "../include/wal.h"
#include <cassert>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <vector>
#include <string>

using namespace std;
namespace fs = std::filesystem;

// Helper function to create test entries
vector<Entry> generate_test_entries(size_t start_index, size_t count, size_t value_size = 100) {
    vector<Entry> entries;
    entries.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        size_t key_num = start_index + i;
        string key_str = "key_" + to_string(key_num);
        string val_str = "value_" + to_string(key_num) + "_" + string(value_size, 'x');
        Bits key_bits(key_str);
        Bits value_bits(val_str);
        entries.emplace_back(key_bits, value_bits);
    }

    sort(entries.begin(), entries.end(), [](const Entry& a, const Entry& b) {
        return a.get_key() < b.get_key();
    });

    return entries;
}

// Helper function to write entries to WAL
void write_entries_to_wal(Wal& wal, vector<Entry>& entries) {
    for (Entry& entry : entries) {
        ostringstream entry_stream = entry.get_ostream_bytes();
        wal.append_entry(entry_stream);
    }
}

// Test 1: Empty WAL file
bool test_empty_wal() {
    cout << "\n=== Test 1: Empty WAL File ===" << endl;
    
    Wal wal;
    wal.clear_entries();
    Mem_Table mem_table(wal);
    
    // Verify empty memtable
    std::cout<<mem_table.get_entry_array_length()<<std::endl;
    assert(mem_table.get_entry_array_length() == 0);
    assert(mem_table.get_total_mem_table_size() == 0);
    
    // Cleanup
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Empty WAL test passed" << endl;
    return true;
}

// Test 2: Single entry recovery
bool test_single_entry() {
    cout << "\n=== Test 2: Single Entry Recovery ===" << endl;

    Wal wal;
    wal.clear_entries();
    
    vector<Entry> entries = generate_test_entries(0, 1);
    write_entries_to_wal(wal, entries);

    std::cout << "WAL file location: " << wal.get_wal_file_location() << std::endl;
    std::cout << "Absolute path: " << fs::absolute(wal.get_wal_file_location()) << std::endl;
    std::ifstream verify(wal.get_wal_file_location(), std::ios::binary | std::ios::ate);
    std::cout << "File size before cin: " << verify.tellg() << " bytes" << std::endl;
    verify.close();

    // Recover from WAL
    Mem_Table mem_table(wal);
    // Verify
    assert(mem_table.get_entry_array_length() == 1);
    assert(mem_table.get_total_mem_table_size() == entries[0].get_entry_length());
    bool temp;
    Entry recovered = mem_table.find(entries[0].get_key(), temp);
    assert(recovered.get_key() == entries[0].get_key());
    assert(recovered.get_value() == entries[0].get_value());
    
    // Cleanup
    wal.clear_entries();
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Single entry test passed" << endl;
    return true;
}

// Test 3: Multiple entries recovery
bool test_multiple_entries() {
    cout << "\n=== Test 3: Multiple Entries Recovery ===" << endl;
    
    
    const size_t ENTRY_COUNT = 100;
    
    // Create WAL with multiple entries
    Wal wal;
    wal.clear_entries();
    
    vector<Entry> entries = generate_test_entries(0, ENTRY_COUNT);
    write_entries_to_wal(wal, entries);
    
    // Recover from WAL
    Mem_Table mem_table(wal);
    
    // Verify count and size
    assert(mem_table.get_entry_array_length() == ENTRY_COUNT);
    
    uint64_t expected_size = 0;
    for (Entry& entry : entries) {
        expected_size += entry.get_entry_length();
    }
    assert(mem_table.get_total_mem_table_size() == expected_size);
    
    bool temp;
    // Verify all entries can be retrieved
    for (const Entry& entry : entries) {
        Entry recovered = mem_table.find(entry.get_key(), temp);
        assert(recovered.get_key() == entry.get_key());
        assert(recovered.get_value() == entry.get_value());
    }
    
    // Cleanup
    wal.clear_entries();
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Multiple entries test passed" << endl;
    return true;
}

// Test 4: Large entries recovery
bool test_large_entries() {
    cout << "\n=== Test 4: Large Entries Recovery ===" << endl;
    
    
    const size_t VALUE_SIZE = 10000; // 10KB values
    const size_t ENTRY_COUNT = 10;
    
    // Create WAL with large entries
    Wal wal;
    wal.clear_entries();
    
    vector<Entry> entries = generate_test_entries(0, ENTRY_COUNT, VALUE_SIZE);
    write_entries_to_wal(wal, entries);
    
    // Recover from WAL
    Mem_Table mem_table(wal);
    
    // Verify
    assert(mem_table.get_entry_array_length() == ENTRY_COUNT);
    bool temp;
    for (const Entry& entry : entries) {
        Entry recovered = mem_table.find(entry.get_key(), temp);
        assert(recovered.get_key() == entry.get_key());
        assert(recovered.get_value() == entry.get_value());
    }
    
    // Cleanup
    wal.clear_entries();
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Large entries test passed" << endl;
    return true;
}

// Test 5: Non-overlapping key ranges
bool test_non_overlapping_keys() {
    cout << "\n=== Test 5: Non-Overlapping Key Ranges ===" << endl;
    
    
    // Create WAL with entries from different key ranges
    Wal wal;
    wal.clear_entries();
    
    vector<Entry> entriesA = generate_test_entries(0, 50);
    vector<Entry> entriesB = generate_test_entries(1000, 50);
    vector<Entry> entriesC = generate_test_entries(2000, 50);
    
    write_entries_to_wal(wal, entriesA);
    write_entries_to_wal(wal, entriesB);
    write_entries_to_wal(wal, entriesC);
    
    // Recover from WAL
    Mem_Table mem_table(wal);
    
    // Verify
    assert(mem_table.get_entry_array_length() == 150);
    
    // Verify all ranges
    vector<vector<Entry>*> all_entries = {&entriesA, &entriesB, &entriesC};
    bool temp;
    for (auto* entries : all_entries) {
        for (const Entry& entry : *entries) {
            Entry recovered = mem_table.find(entry.get_key(), temp);
            assert(recovered.get_key() == entry.get_key());
            assert(recovered.get_value() == entry.get_value());
        }
    }
    
    // Cleanup
    wal.clear_entries();
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Non-overlapping keys test passed" << endl;
    return true;
}

// Test 6: Overlapping keys (updates)
bool test_overlapping_keys() {
    cout << "\n=== Test 6: Overlapping Keys (Updates) ===" << endl;
    
    
    // Create WAL with updates to same keys
    Wal wal;
    wal.clear_entries();
    
    // Write initial entries
    vector<Entry> initial_entries = generate_test_entries(0, 10);
    write_entries_to_wal(wal, initial_entries);
    
    // Write updated entries with same keys but different values
    vector<Entry> updated_entries;
    for (size_t i = 0; i < 10; ++i) {
        string key_str = "key_" + to_string(i);
        string val_str = "updated_value_" + to_string(i);
        Bits key_bits(key_str);
        Bits value_bits(val_str);
        updated_entries.emplace_back(key_bits, value_bits);
    }
    write_entries_to_wal(wal, updated_entries);
    
    // Recover from WAL
    Mem_Table mem_table(wal);
    
    // Verify that latest values are present (updates should overwrite)
    assert(mem_table.get_entry_array_length() == 10); // Should have 10 unique keys
    
    for (const Entry& entry : updated_entries) {
        bool temp;
        Entry recovered = mem_table.find(entry.get_key(), temp);
        assert(recovered.get_key() == entry.get_key());
        assert(recovered.get_value() == entry.get_value());
    }
    
    // Cleanup
    wal.clear_entries();
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Overlapping keys test passed" << endl;
    return true;
}

// Test 7: Missing WAL file (error case)
bool test_missing_wal_file() {
    cout << "\n=== Test 7: Missing WAL File ===" << endl;
    
    
    Wal wal;
    
    bool exception_thrown = false;
    try {
        Mem_Table mem_table(wal);
    } catch (const runtime_error& e) {
        exception_thrown = true;
        string error_msg(e.what());
        assert(error_msg.find("Failed to open WAL file") != string::npos);
    }
    
    assert(exception_thrown);
    
    cout << "✓ Missing WAL file test passed" << endl;
    return true;
}

// Test 8: Corrupted WAL (invalid entry length)
bool test_corrupted_wal_invalid_length() {
    cout << "\n=== Test 8: Corrupted WAL (Invalid Length) ===" << endl;
    
    string wal_location = DEFAULT_WAL_FOLDER_LOCATION;
    wal_location += "wal.log";
    // Create corrupted WAL with invalid entry length
    {
        ofstream wal_file(wal_location, ios::binary | ios::trunc);
        uint64_t invalid_length = 4; // Less than sizeof(uint64_t)
        wal_file.write(reinterpret_cast<const char*>(&invalid_length), sizeof(invalid_length));
    }
    
    Wal wal;
    
    bool exception_thrown = false;
    try {
        Mem_Table mem_table(wal);
    } catch (const runtime_error& e) {
        exception_thrown = true;
        string error_msg(e.what());
        assert(error_msg.find("Invalid entry length") != string::npos);
    }
    
    assert(exception_thrown);
    
    // Cleanup
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Corrupted WAL test passed" << endl;
    return true;
}

// Test 9: Partial entry (unexpected EOF)
bool test_partial_entry() {
    cout << "\n=== Test 9: Partial Entry (Unexpected EOF) ===" << endl;
    
    string wal_location = DEFAULT_WAL_FOLDER_LOCATION;
    wal_location += "wal.log";
    // Create WAL with partial entry
    {
        ofstream wal_file(wal_location, ios::binary | ios::trunc);
        uint64_t entry_length = 100;
        wal_file.write(reinterpret_cast<const char*>(&entry_length), sizeof(entry_length));
        // Write only partial data (less than promised)
        string partial_data = "incomplete";
        wal_file.write(partial_data.data(), partial_data.size());
    }
    
    Wal wal;
    
    bool exception_thrown = false;
    try {
        Mem_Table mem_table(wal);
    } catch (const runtime_error& e) {
        exception_thrown = true;
        string error_msg(e.what());
        assert(error_msg.find("Unexpected EOF") != string::npos);
    }
    
    assert(exception_thrown);
    
    // Cleanup
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Partial entry test passed" << endl;
    return true;
}

// Test 10: Stress test with many entries
bool test_stress_many_entries() {
    cout << "\n=== Test 10: Stress Test (1000 entries) ===" << endl;
    
    
    const size_t ENTRY_COUNT = 1000;
    
    // Create WAL with many entries
    Wal wal;
    wal.clear_entries();
    
    vector<Entry> entries = generate_test_entries(0, ENTRY_COUNT);
    write_entries_to_wal(wal, entries);
    
    // Recover from WAL
    auto start = chrono::high_resolution_clock::now();
    Mem_Table mem_table(wal);
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
    
    cout << "Recovery time: " << duration.count() << "ms" << endl;
    
    // Verify
    assert(mem_table.get_entry_array_length() == ENTRY_COUNT);
    
    // Spot check some entries
    bool temp;
    for (size_t i = 0; i < ENTRY_COUNT; i += 100) {
        Entry recovered = mem_table.find(entries[i].get_key(), temp);
        assert(recovered.get_key() == entries[i].get_key());
        assert(recovered.get_value() == entries[i].get_value());
    }
    
    // Cleanup
    wal.clear_entries();
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Stress test passed" << endl;
    return true;
}

// Test 11: Entries with tombstone flags
bool test_tombstone_entries() {
    cout << "\n=== Test 11: Tombstone Entries ===" << endl;
    
    
    // Create WAL with tombstone entries
    Wal wal;
    wal.clear_entries();
    
    // Create entries and mark some as deleted
    vector<Entry> entries = generate_test_entries(0, 10);
    
    // Mark entries 2, 5, 7 as tombstones
    entries[2].set_tombstone(true);
    entries[5].set_tombstone(true);
    entries[7].set_tombstone(true);
    
    write_entries_to_wal(wal, entries);
    
    // Recover from WAL
    Mem_Table mem_table(wal);
    bool temp;
    // Verify tombstone flags are preserved
    Entry recovered2 = mem_table.find(entries[2].get_key(), temp);
    assert(recovered2.is_deleted());
    
    Entry recovered5 = mem_table.find(entries[5].get_key(), temp);
    assert(recovered5.is_deleted());
    
    Entry recovered7 = mem_table.find(entries[7].get_key(), temp);
    assert(recovered7.is_deleted());
    
    // Verify non-tombstone entries
    Entry recovered0 = mem_table.find(entries[0].get_key(), temp);
    assert(!recovered0.is_deleted());
    
    // Cleanup
    wal.clear_entries();
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Tombstone entries test passed" << endl;
    return true;
}

// Test 12: Checksum verification
bool test_checksum_verification() {
    cout << "\n=== Test 12: Checksum Verification ===" << endl;
    
    
    // Create WAL with valid entries
    Wal wal;
    wal.clear_entries();
    
    vector<Entry> entries = generate_test_entries(0, 10);
    write_entries_to_wal(wal, entries);
    
    // Recover from WAL
    Mem_Table mem_table(wal);
    
    // Verify checksums for all recovered entries
    bool temp;
    for (Entry& original : entries) {
        Entry recovered = mem_table.find(original.get_key(), temp);
        assert(recovered.check_checksum());
        assert(recovered.get_checksum() == original.get_checksum());
    }
    
    // Cleanup
    wal.clear_entries();
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Checksum verification test passed" << endl;
    return true;
}

// Test 13: Entry length calculation
bool test_entry_length_calculation() {
    cout << "\n=== Test 13: Entry Length Calculation ===" << endl;
    
    
    // Create WAL with entries of various sizes
    Wal wal;
    wal.clear_entries();
    
    vector<Entry> small_entries = generate_test_entries(0, 5, 10);
    vector<Entry> medium_entries = generate_test_entries(100, 5, 1000);
    vector<Entry> large_entries = generate_test_entries(200, 5, 5000);
    
    write_entries_to_wal(wal, small_entries);
    write_entries_to_wal(wal, medium_entries);
    write_entries_to_wal(wal, large_entries);
    
    // Recover from WAL
    Mem_Table mem_table(wal);
    
    // Verify entry lengths are preserved
    uint64_t total_expected_size = 0;
    vector<vector<Entry>*> all = {&small_entries, &medium_entries, &large_entries};
    bool temp;
    for (auto* entries : all) {
        for (Entry& original : *entries) {
            Entry recovered = mem_table.find(original.get_key(), temp);
            assert(recovered.get_entry_length() == original.get_entry_length());
            total_expected_size += original.get_entry_length();
        }
    }
    
    assert(mem_table.get_total_mem_table_size() == total_expected_size);
    
    // Cleanup
    wal.clear_entries();
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Entry length calculation test passed" << endl;
    return true;
}

// Test 14: Sequential recovery (multiple Mem_Table constructions)
bool test_sequential_recovery() {
    cout << "\n=== Test 14: Sequential Recovery ===" << endl;
    
    
    // Create WAL with entries
    Wal wal;
    wal.clear_entries();
    
    vector<Entry> entries = generate_test_entries(0, 50);
    write_entries_to_wal(wal, entries);
    
    // Recover multiple times to ensure consistency
    for (int i = 0; i < 3; ++i) {
        cout << "Recovery iteration " << (i + 1) << endl;
        Mem_Table mem_table(wal);
        
        assert(mem_table.get_entry_array_length() == 50);
        
        // Verify all entries
        bool temp;
        for (Entry& entry : entries) {
            Entry recovered = mem_table.find(entry.get_key(), temp);
            assert(recovered.get_key() == entry.get_key());
            assert(recovered.get_value() == entry.get_value());
            assert(recovered.get_checksum() == entry.get_checksum());
        }
    }
    
    // Cleanup
    wal.clear_entries();
    fs::remove(wal.get_wal_file_location());
    
    cout << "✓ Sequential recovery test passed" << endl;
    return true;
}

int main() {
    cout << "========================================" << endl;
    cout << "Mem_Table WAL Constructor Unit Tests" << endl;
    cout << "========================================" << endl;
    
    // Create test directory if it doesn't exist
    fs::create_directories(DEFAULT_WAL_FOLDER_LOCATION);
    
    try {
        assert(test_empty_wal());
        assert(test_single_entry());
        assert(test_multiple_entries());
        assert(test_large_entries());
        assert(test_non_overlapping_keys());
        assert(test_overlapping_keys());
        assert(test_missing_wal_file());
        assert(test_corrupted_wal_invalid_length());
        assert(test_partial_entry());
        assert(test_stress_many_entries());
        assert(test_tombstone_entries());
        assert(test_checksum_verification());
        assert(test_entry_length_calculation());
        assert(test_sequential_recovery());
        
        cout << "\n========================================" << endl;
        cout << "✓ ALL TESTS PASSED (14/14)" << endl;
        cout << "========================================" << endl;
        
    } catch (const exception& e) {
        cout << "\n✗ TEST FAILED: " << e.what() << endl;
        return 1;
    }
    
    return 0;
}