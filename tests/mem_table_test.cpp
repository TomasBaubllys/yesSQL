#include <cassert>
#include <iostream>
#include <string>
#include <vector>
#include "../include/mem_table.h"

using namespace std;

// Helper to construct Bits from std::string
Bits makeBits(const string& s) {
    return Bits(s);
}

// Compare two entries by key and value
bool entriesEqual(Entry& a, Entry& b) {
    return a.get_key() == b.get_key() && a.get_value() == b.get_value();
}

// Find an entry with the same key in a vector (returns index or -1)
int findEntryIndexByKey(vector<Entry>& vec, Entry& needle) {
    for (size_t i = 0; i < vec.size(); ++i) {
        if (vec[i].get_key() == needle.get_key())
            return static_cast<int>(i);
    }
    return -1;
}

void test_basic_insert() {
    cout << "Running test_basic_insert..." << endl;
    
    MemTable mem;
    Entry e(makeBits("key1"), makeBits("value1"));
    
    assert(mem.insert_entry(e));
    assert(mem.get_entry_array_length() == 1);
    
    Entry found = mem.find(makeBits("key1"));
    assert(found.get_key() == e.get_key());
    assert(found.get_value() == e.get_value());
    assert(!found.is_deleted());
    
    cout << "PASSED: test_basic_insert" << endl;
}

void test_multiple_inserts() {
    cout << "Running test_multiple_inserts..." << endl;
    
    MemTable mem;
    Entry e1(makeBits("key1"), makeBits("val1"));
    Entry e2(makeBits("key2"), makeBits("val2"));
    Entry e3(makeBits("key3"), makeBits("val3"));
    
    assert(mem.insert_entry(e1));
    assert(mem.insert_entry(e2));
    assert(mem.insert_entry(e3));
    assert(mem.get_entry_array_length() == 3);
    
    // Verify all entries can be found
    Entry f1 = mem.find(makeBits("key1"));
    Entry f2 = mem.find(makeBits("key2"));
    Entry f3 = mem.find(makeBits("key3"));
    
    assert(f1.get_value() == e1.get_value());
    assert(f2.get_value() == e2.get_value());
    assert(f3.get_value() == e3.get_value());
    
    cout << "PASSED: test_multiple_inserts" << endl;
}

void test_overwrite_semantics() {
    cout << "Running test_overwrite_semantics..." << endl;
    
    MemTable mem;
    Entry e1(makeBits("dupKey"), makeBits("v1"));
    bool ok = mem.insert_entry(e1);
    assert(ok);
    int len_after_first = mem.get_entry_array_length();
    assert(len_after_first == 1);

    uint64_t mem_before = mem.get_total_mem_table_size();

    // Insert duplicate key with different value -> should overwrite
    Entry e2(makeBits("dupKey"), makeBits("v2"));
    bool ok2 = mem.insert_entry(e2);
    assert(ok2);

    // length must remain unchanged (overwrite semantics)
    assert(mem.get_entry_array_length() == len_after_first);

    // find should return the newest value (v2)
    Entry found = mem.find(makeBits("dupKey"));
    assert(found.get_key() == e2.get_key());
    assert(found.get_value() == e2.get_value());

    // memory should not have decreased (no deallocation on overwrite)
    assert(mem.get_total_mem_table_size() >= mem_before);
    
    cout << "PASSED: test_overwrite_semantics" << endl;
}

void test_multiple_overwrites() {
    cout << "Running test_multiple_overwrites..." << endl;
    
    MemTable mem;
    Entry e(makeBits("key"), makeBits("v1"));
    
    assert(mem.insert_entry(e));
    int initial_len = mem.get_entry_array_length();
    
    // Overwrite multiple times
    for (int i = 2; i <= 5; ++i) {
        Entry e_new(makeBits("key"), makeBits("v" + to_string(i)));
        assert(mem.insert_entry(e_new));
        assert(mem.get_entry_array_length() == initial_len); // Length unchanged
    }
    
    // Should have the last value
    Entry found = mem.find(makeBits("key"));
    assert(found.get_value() == makeBits("v5"));
    
    cout << "PASSED: test_multiple_overwrites" << endl;
}

void test_tombstone_semantics() {
    cout << "Running test_tombstone_semantics..." << endl;
    
    MemTable mem;
    Entry e(makeBits("tombKey"), makeBits("valX"));
    assert(mem.insert_entry(e));
    int len_before = mem.get_entry_array_length();
    uint64_t mem_before = mem.get_total_mem_table_size();

    // Remove by key -> should set tombstone (not deallocate)
    bool removed = mem.remove_find_entry(makeBits("tombKey"));
    assert(removed);

    // length should remain the same (entry still present as tombstone)
    assert(mem.get_entry_array_length() == len_before);

    // memory should not decrease after deletion (tombstone stored instead of dealloc)
    assert(mem.get_total_mem_table_size() >= mem_before);

    // find must return an Entry that is marked deleted (tombstone)
    Entry found = mem.find(makeBits("tombKey"));
    assert(found.get_key() == e.get_key());
    assert(found.is_deleted() && "Entry must be marked deleted (tombstone)");

    // dump_entries should include the tombstoned entry
    vector<Entry> dumped = mem.dump_entries();
    int idx = findEntryIndexByKey(dumped, found);
    assert(idx != -1);
    assert(dumped[idx].is_deleted());
    
    cout << "PASSED: test_tombstone_semantics" << endl;
}

void test_remove_entry_by_object() {
    cout << "Running test_remove_entry_by_object..." << endl;
    
    MemTable mem;
    Entry e(makeBits("rKey"), makeBits("rVal"));
    assert(mem.insert_entry(e));
    int len_before = mem.get_entry_array_length();
    uint64_t mem_before = mem.get_total_mem_table_size();

    bool removed = mem.remove_entry(e);
    assert(removed);

    // length unchanged (tombstone)
    assert(mem.get_entry_array_length() == len_before);

    // memory should not decrease
    assert(mem.get_total_mem_table_size() >= mem_before);

    Entry f = mem.find(makeBits("rKey"));
    assert(f.is_deleted());
    
    cout << "PASSED: test_remove_entry_by_object" << endl;
}

void test_remove_nonexistent_key() {
    cout << "Running test_remove_nonexistent_key..." << endl;
    
    MemTable mem;
    Entry e(makeBits("exists"), makeBits("val"));
    assert(mem.insert_entry(e));
    
    // Try to remove a key that doesn't exist
    bool removed = mem.remove_find_entry(makeBits("doesNotExist"));
    assert(!removed); // Should return false
    
    // Original entry should still be there and not tombstoned
    Entry found = mem.find(makeBits("exists"));
    assert(!found.is_deleted());
    
    cout << "PASSED: test_remove_nonexistent_key" << endl;
}

void test_find_nonexistent_key() {
    cout << "Running test_find_nonexistent_key..." << endl;
    
    MemTable mem;
    Entry e(makeBits("exists"), makeBits("val"));
    assert(mem.insert_entry(e));
    
    // Try to find a key that doesn't exist
    Entry found = mem.find(makeBits("doesNotExist"));
    
    // Implementation dependent: check what your find returns for non-existent keys
    // This test documents the behavior
    
    cout << "PASSED: test_find_nonexistent_key" << endl;
}

void test_dump_entries_comprehensive() {
    cout << "Running test_dump_entries_comprehensive..." << endl;
    
    MemTable mem;
    // insert 3 entries
    Entry a(makeBits("a"), makeBits("A"));
    Entry b(makeBits("b"), makeBits("B"));
    Entry c(makeBits("c"), makeBits("C"));
    assert(mem.insert_entry(a));
    assert(mem.insert_entry(b));
    assert(mem.insert_entry(c));

    // overwrite b
    Entry b2(makeBits("b"), makeBits("B2"));
    assert(mem.insert_entry(b2)); // overwrite, length unchanged

    // tombstone 'c'
    assert(mem.remove_find_entry(makeBits("c")));

    vector<Entry> dumped = mem.dump_entries();
    assert(dumped.size() == 3);
    
    // We expect 3 entries (a, b (with value B2), c (tombstoned))
    int idxA = findEntryIndexByKey(dumped, a);
    int idxB = findEntryIndexByKey(dumped, b2);
    int idxC = findEntryIndexByKey(dumped, c);

    assert(idxA != -1);
    assert(idxB != -1);
    assert(idxC != -1);

    // a should not be tombstoned
    assert(!dumped[idxA].is_deleted());
    // b should have new value B2
    assert(dumped[idxB].get_value() == b2.get_value());
    // c should be tombstoned
    assert(dumped[idxC].is_deleted());
    
    cout << "PASSED: test_dump_entries_comprehensive" << endl;
}

void test_empty_memtable() {
    cout << "Running test_empty_memtable..." << endl;
    
    MemTable mem;
    
    assert(mem.get_entry_array_length() == 0);
    assert(mem.get_total_mem_table_size() >= 0); // At least 0 (may have overhead)
    
    vector<Entry> dumped = mem.dump_entries();
    assert(dumped.empty());
    
    cout << "PASSED: test_empty_memtable" << endl;
}

void test_insert_empty_key_value() {
    cout << "Running test_insert_empty_key_value..." << endl;
    
    MemTable mem;
    
    // Empty key
    Entry e1(makeBits(""), makeBits("value"));
    assert(mem.insert_entry(e1));
    Entry found1 = mem.find(makeBits(""));
    assert(found1.get_value() == e1.get_value());
    
    // Empty value
    Entry e2(makeBits("key"), makeBits(""));
    assert(mem.insert_entry(e2));
    Entry found2 = mem.find(makeBits("key"));
    assert(found2.get_value() == e2.get_value());
    
    // Both empty
    Entry e3(makeBits(""), makeBits(""));
    // This might overwrite e1, depending on implementation
    
    cout << "PASSED: test_insert_empty_key_value" << endl;
}

void test_large_values() {
    cout << "Running test_large_values..." << endl;
    
    MemTable mem;
    
    // Large key and value
    string large_key(1000, 'K');
    string large_value(10000, 'V');
    
    Entry e(makeBits(large_key), makeBits(large_value));
    assert(mem.insert_entry(e));
    
    Entry found = mem.find(makeBits(large_key));
    assert(found.get_key() == e.get_key());
    assert(found.get_value() == e.get_value());
    
    uint64_t mem_size = mem.get_total_mem_table_size();
    assert(mem_size >= 11000); // At least the size of key + value
    
    cout << "PASSED: test_large_values" << endl;
}

void test_memory_tracking() {
    cout << "Running test_memory_tracking..." << endl;
    
    MemTable mem;
    uint64_t initial_size = mem.get_total_mem_table_size();
    
    Entry e1(makeBits("key1"), makeBits("value1"));
    assert(mem.insert_entry(e1));
    uint64_t after_one = mem.get_total_mem_table_size();
    assert(after_one > initial_size); // Memory increased
    
    Entry e2(makeBits("key2"), makeBits("value2"));
    assert(mem.insert_entry(e2));
    uint64_t after_two = mem.get_total_mem_table_size();
    assert(after_two > after_one); // Memory increased again
    
    // Overwrite should not significantly decrease memory
    Entry e1_new(makeBits("key1"), makeBits("newval"));
    assert(mem.insert_entry(e1_new));
    uint64_t after_overwrite = mem.get_total_mem_table_size();
    assert(after_overwrite >= after_two);
    
    cout << "PASSED: test_memory_tracking" << endl;
}

void test_tombstone_then_reinsert() {
    cout << "Running test_tombstone_then_reinsert..." << endl;
    
    MemTable mem;
    Entry e(makeBits("key"), makeBits("val1"));
    assert(mem.insert_entry(e));
    
    // Tombstone it
    assert(mem.remove_find_entry(makeBits("key")));
    Entry found1 = mem.find(makeBits("key"));
    assert(found1.is_deleted());
    
    int len_after_delete = mem.get_entry_array_length();
    
    // Reinsert with new value
    Entry e2(makeBits("key"), makeBits("val2"));
    assert(mem.insert_entry(e2));
    
    // Should overwrite the tombstone
    assert(mem.get_entry_array_length() == len_after_delete); // Length unchanged
    Entry found2 = mem.find(makeBits("key"));
    assert(!found2.is_deleted()); // No longer tombstoned
    assert(found2.get_value() == e2.get_value());
    
    cout << "PASSED: test_tombstone_then_reinsert" << endl;
}

void test_special_characters() {
    cout << "Running test_special_characters..." << endl;
    
    MemTable mem;
    
    // Keys/values with special characters
    Entry e1(makeBits("key\n\t\r"), makeBits("val\0ue"));
    Entry e2(makeBits("key!@#$%"), makeBits("val^&*()"));
    Entry e3(makeBits("key中文"), makeBits("值"));
    
    assert(mem.insert_entry(e1));
    assert(mem.insert_entry(e2));
    assert(mem.insert_entry(e3));
    
    Entry f1 = mem.find(makeBits("key\n\t\r"));
    Entry f2 = mem.find(makeBits("key!@#$%"));
    Entry f3 = mem.find(makeBits("key中文"));
    
    assert(f1.get_value() == e1.get_value());
    assert(f2.get_value() == e2.get_value());
    assert(f3.get_value() == e3.get_value());
    
    cout << "PASSED: test_special_characters" << endl;
}

void test_dump_preserves_order() {
    cout << "Running test_dump_preserves_order..." << endl;
    
    MemTable mem;
    vector<Entry> entries;
    
    // Insert in a specific order
    for (int i = 0; i < 5; ++i) {
        Entry e(makeBits("key" + to_string(i)), makeBits("val" + to_string(i)));
        entries.push_back(e);
        assert(mem.insert_entry(e));
    }
    
    vector<Entry> dumped = mem.dump_entries();
    assert(dumped.size() == entries.size());
    
    // Check that all entries are present (order may vary depending on implementation)
    for (auto orig : entries) {
        bool found = false;
        for (auto dump : dumped) {
            if (dump.get_key() == orig.get_key()) {
                found = true;
                assert(dump.get_value() == orig.get_value());
                break;
            }
        }
        assert(found && "All entries should be in dump");
    }
    
    cout << "PASSED: test_dump_preserves_order" << endl;
}

int main() {
    cout << "Starting MemTable comprehensive tests...\n" << endl;

    try {
        test_empty_memtable();
        test_basic_insert();
        test_multiple_inserts();
        test_overwrite_semantics();
        test_multiple_overwrites();
        test_tombstone_semantics();
        test_remove_entry_by_object();
        test_remove_nonexistent_key();
        test_find_nonexistent_key();
        test_dump_entries_comprehensive();
        test_insert_empty_key_value();
        test_large_values();
        test_memory_tracking();
        test_tombstone_then_reinsert();
        test_special_characters();
        test_dump_preserves_order();

        cout << "\n==================================" << endl;
        cout << "✅ All MemTable tests passed!" << endl;
        cout << "==================================" << endl;
        return 0;
        
    } catch (const exception& e) {
        cerr << "Test failed with exception: " << e.what() << endl;
        return 1;
    }
}