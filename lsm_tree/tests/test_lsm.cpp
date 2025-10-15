#include "../include/lsm_tree.h"
#include <cassert>
#include <iostream>
#include <random>

#define ROUND1_ENTRY_COUNT 1000
#define ROUND2_ENTRY_COUNT 1000

using namespace std;

// generates a vector of random entries
// Helper to generate random alphanumeric strings
string random_string(size_t length) {
    static const string charset =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static random_device rd;
    static mt19937 gen(rd());
    static uniform_int_distribution<size_t> dist(0, charset.size() - 1);

    string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result.push_back(charset[dist(gen)]);
    }
    return result;
}

// Generate a unsorted vector of Entries
vector<Entry> generate_test_entries(size_t count, size_t key_size = 400, size_t value_size = 1000) {
    vector<Entry> entries;
    entries.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        string key_str = "key_" + to_string(i) + "_" + random_string(key_size);
        string val_str = "value_" + to_string(i) + "_" + random_string(value_size);
        //cout << key_str.length() << endl;
        Bits key_bits(key_str);
        //cout << key_bits.get_string_char().length() << endl;
        Bits value_bits(val_str);
        entries.emplace_back(key_bits, value_bits);
    }

    /*sort(entries.begin(), entries.end(),
         [&](const Entry& a, const Entry& b) {
             return a.get_key() < b.get_key();
         });
    */

    return entries;
}


// test if avl tree is filled correctly
bool test_mem_table(vector<Entry>& entries, LsmTree& lsm_tree) {
    cout << "Adding random entries to the lsm tree" << endl;
    for(const Entry& entry : entries) {
        // bruuh???
        //cout << "here" << endl;
        lsm_tree.set(entry.get_key().get_string_char(), entry.get_value().get_string_char());
    }
    cout << "Done" << endl;

    // look for the keys
    cout << "Looking for all the entries in the lsm tree" << endl;
    for(const Entry& entry : entries) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }
    cout << "Done" << endl;

    return true;
}

bool test_mem_table_flush() {


}

int main(int argc, char* argv[]) {
    cout << "Generating random test entries (1)" << endl;
    vector<Entry> entries1 = generate_test_entries(ROUND1_ENTRY_COUNT, 321, 12321);
    cout << "Done" << endl;
    cout << "Creating empty lsm tree" << endl;
    LsmTree lsm_tree;
    cout << "Done" << endl;
    

    // fill mem table
    assert(test_mem_table(entries1, lsm_tree));
    assert(test_mem_table(entries1, lsm_tree));
    assert(test_mem_table(entries1, lsm_tree));

    // assert(false);

    // merge level 0
    cout << "Compacting level 0..." << endl;
    assert(lsm_tree.compact_level(0));

    cout << "Generating random test entries (2)" << endl;
    vector<Entry> entries2 = generate_test_entries(ROUND2_ENTRY_COUNT, 121, 22991);
    cout << "Done" << endl;

    assert(test_mem_table(entries2, lsm_tree));
    assert(test_mem_table(entries2, lsm_tree));
    assert(test_mem_table(entries1, lsm_tree));
    assert(test_mem_table(entries2, lsm_tree));

    assert(lsm_tree.compact_level(0));

    assert(lsm_tree.compact_level(1));

    cout << "Looking for all the entries in the lsm tree (1)" << endl;
    for(const Entry& entry : entries1) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }

    cout << "Looking for all the entries in the lsm tree (2)" << endl;
    for(const Entry& entry : entries2) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }
    cout << "Done" << endl;

    // look for all the entries

}