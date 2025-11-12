// test.cpp - comprehensive tests for Bits, Entry and AVL_tree
// Compile with: g++ -std=c++17 test.cpp <your_other_sources>.cpp -o test
// Example: g++ -std=c++17 test.cpp avl_tree.cpp entry.cpp bits.cpp crc32.cpp -o test

#include <iostream>
#include <vector>
#include <sstream>
#include <cassert>
#include <stdexcept>
#include <algorithm>

#include "../include/avl_tree.h"

using namespace std;

static void print_header(const char* h) {
    cout << "================ " << h << " ================\n";
}

Bits makeBitsFromString(const string &s) {
    // Bits has a constructor Bits(std::string&), but it takes non-const reference in header.
    // Create a non-const temporary to call it.
    string tmp = s;
    return Bits(tmp);
}

Entry makeEntry(const string &k, const string &v) {
    Bits kb = makeBitsFromString(k);
    Bits vb = makeBitsFromString(v);
    return Entry(kb, vb);
}

void test_bits_basic() {
    print_header("Bits: basic construction & comparisons");

    Bits a = makeBitsFromString("a");
    Bits b = makeBitsFromString("b");
    Bits aa = makeBitsFromString("aa");
    Bits a_copy(a);

    // basic comparisons (relies on lexicographic/order defined in header)
    assert(a == a_copy);
    assert(a != b);
    assert((a < b) || (a > b)); // one of them must be true
    assert(a.size() > 0u);

    // get_char_vector / get_string_char should exist; verify round-trip-ish
    string s = a.get_string_char();
    vector<char> vc = a.get_char_vector();
    assert(!s.empty() || !vc.empty());

    // test update_bits length mismatch triggers error (engine throws)
    bool caught = false;
    try {
        // build a new vector with very different length
        vector<uint8_t> wrong{1,2,3,4,5,6,7,8,9,10};
        a.update_bits(wrong);
    } catch (const std::exception& e) {
        caught = true;
    } catch (...) {
        caught = true;
    }
    // It's allowed that update_bits throws; ensure we did not silently accept corrupted state
    assert(caught);
    cout << "Bits basic tests passed.\n\n";
}

void test_entry_basic_and_checksum() {
    print_header("Entry: construction, checksum & serialization");

    Entry e1 = makeEntry("key1", "value1");
    Entry e2 = makeEntry("key2", "value2");

    // Entry equality is based on key per header operators
    assert(!(e1 == e2));
    assert(e1 != e2);

    // get_entry_length should be > 0
    uint64_t len1 = e1.get_entry_length();
    assert(len1 > 0);

    // checksum must be retrievable and check_checksum should return true for original entry
    uint32_t ck = e1.get_checksum();
    (void)ck; // silence unused
    assert(e1.check_checksum());

    // serialization round-trip: get bytes and reconstruct Entry using stringstream ctor
    std::ostringstream out = e1.get_ostream_bytes();
    std::string raw = out.str();
    std::stringstream in(raw);
    Entry e1copy(in);
    // the new object should validate checksum
    assert(e1copy.check_checksum());

    // now corrupt the serialized bytes slightly and ensure checksum fails (or check_checksum false)
    std::string corrupted = raw;
    if (!corrupted.empty()) {
        // flip one byte in the middle
        size_t p = corrupted.size() / 2;
        corrupted[p] = (char)(corrupted[p] ^ 0xFF);
        std::stringstream in2(corrupted);
        Entry ecor(in2);
        bool ok = ecor.check_checksum();
        // the checksum should be invalid after corruption
        assert(!ok);
    }

    // test tombstone toggles and set/get
    bool beforeDeleted = e1.is_deleted();
    e1.set_tombstone();
    assert(e1.is_deleted() != beforeDeleted);
    e1.set_tombstone(false);
    assert(e1.is_deleted() == false);

    // update_value should change checksum and value
    uint32_t old_ck = e2.get_checksum();
    e2.update_value(makeBitsFromString("newval"));
    uint32_t new_ck = e2.get_checksum();
    assert(old_ck != new_ck);
    assert(e2.check_checksum());

    cout << "Entry basic tests passed.\n\n";
}

void test_avl_tree_operations() {
    print_header("AVL_tree: insert, inorder, search, remove, pop_last, make_empty");

    // Prepare a list of entries with non-trivial ordering (varying key lengths)
    vector<pair<string,string>> kvs = {
        {"m", "val_m"},
        {"a", "val_a"},
        {"z", "val_z"},
        {"aa", "val_aa"},
        {"ab", "val_ab"},
        {"mm", "val_mm"},
        {"k", "val_k"},
        {"y", "val_y"},
        {"b", "val_b"}
    };

    // create AVL tree using default ctor
    AVL_Tree tree;

    // Insert entries and keep originals for deletion tests
    vector<Entry> entries;
    for (auto &p : kvs) {
        entries.push_back(makeEntry(p.first, p.second));
    }

    // Insert in a shuffled order to exercise rotations
    vector<int> order = {3,1,5,0,2,7,4,8,6}; // arbitrary order
    for (int idx : order) {
        tree.insert(entries[idx]);
    }

    // inorder() should return sorted entries by key
    vector<Entry> inord = tree.inorder();
    assert(inord.size() == entries.size());

    // check that inorder keys are strictly increasing by using Entry comparison operators
    for (size_t i = 1; i < inord.size(); ++i) {
        // inord[i-1] < inord[i]
        assert(inord[i-1] < inord[i]);
    }

    // test search for present and absent keys
    bool found = false;
	Bits b1 = makeBitsFromString("aa");
    Entry s = tree.search(b1, found);
    assert(found);
    assert(s.get_key() == makeBitsFromString("aa"));

    found = false;
	Bits b2 = makeBitsFromString("nonexistent");
    Entry s2 = tree.search(b2, found);
    assert(!found);

    // test remove by Entry (remove 'm')
    tree.remove(entries[0]); // entries[0] corresponds to key "m"
    // ensure size decreased by 1
    vector<Entry> afterRem = tree.inorder();
    assert(afterRem.size() == entries.size() - 1);

    // test remove by Bits (remove key "z")
	Bits b3 = makeBitsFromString("z");
    tree.remove(b3);
    afterRem = tree.inorder();
    assert(afterRem.size() == entries.size() - 2);

    // test pop_last: should return the (current) maximum element
    Entry last = tree.pop_last();
    // verify last is >= everything else
    vector<Entry> afterPop = tree.inorder();
    for (auto &e : afterPop) {
        assert(e < last || e == last); // last is greatest
    }

    // test make_empty: after clearing, inorder() must be empty
    tree.make_empty();
    vector<Entry> cleared = tree.inorder();
	cout << (cleared.size()) << endl;
    assert(cleared.empty());

    cout << "AVL_tree operation tests passed.\n\n";
}

void test_misc_copies_and_streams() {
    print_header("Misc: copies, assignment, key length retrieval");

    Entry e = makeEntry("k1", "v1");
    Entry ecopy = e; // copy constructor
    assert(ecopy.get_key() == e.get_key());
    assert(ecopy.get_entry_length() == e.get_entry_length());
    // assignment
    Entry e2 = makeEntry("k2","v2");
    e2 = e;
    assert(e2.get_key() == e.get_key());

    // key length
    auto klen = e.get_key_length();
    (void)klen;
    assert(klen > 0);

    cout << "Misc tests passed.\n\n";
}


int main() {
    cout << "Starting comprehensive test suite for Bits, Entry and AVL_tree...\n\n";
    try {
        test_bits_basic();
        test_entry_basic_and_checksum();
        test_avl_tree_operations();
        test_misc_copies_and_streams();
    } catch (const std::exception &ex) {
        cerr << "Test caught exception: " << ex.what() << "\n";
        return 2;
    } catch (...) {
        cerr << "Test caught unknown exception\n";
        return 3;
    }

    cout << "ALL TESTS PASSED SUCCESSFULLY.\n";
    return 0;
}
