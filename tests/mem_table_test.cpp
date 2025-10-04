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

int main() {
    cout << "Running MemTable tests (overwrite + tombstone semantics)..." << endl;

    // Test A: Insert then overwrite same key -> value updated, length unchanged
    {
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
    }

    // Test B: Delete (tombstone) semantics: tombstone set, entry still present, memory not freed
    {
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
    }

    // Test C: remove_entry(Entry&) should set tombstone for existing entry
    {
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
    }

    // Test D: dump_entries keeps tombstoned entries and preserves overwrite-visible state
    {
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
        // We expect 3 entries (a, b (with value B2), c (tombstoned))
        // find indices and check properties
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
    }

    cout << "✅ MemTable overwrite + tombstone tests passed." << endl;
    return 0;
}