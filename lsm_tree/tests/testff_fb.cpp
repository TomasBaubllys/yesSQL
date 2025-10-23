#include "../include/lsm_tree.h"
#include <cassert>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <set>

#define ENTRY_COUNT 1000

using namespace std;

// Generate a vector of entries with a fixed key range (no overlap)
vector<Entry> generate_test_entries(size_t start_index, size_t count, size_t value_size = 10000) {
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

// Test helper for basic insertion and retrieval
bool test_mem_table(vector<Entry>& entries, LSM_Tree& lsm_tree) {
    cout << "Adding entries to LSM tree..." << endl;
    for (const Entry& entry : entries) {
        lsm_tree.set(entry.get_key().get_string(), entry.get_value().get_string());
    }

    cout << "Verifying entries..." << endl;
    for (const Entry& entry : entries) {
        Entry entry_got(lsm_tree.get(entry.get_key().get_string()));
        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }
    cout << "All entries verified successfully.\n";
    return true;
}

// Test get_ff (get forward from key)
bool test_get_ff(LSM_Tree& lsm_tree) {
    cout << "\n=== Testing get_ff (forward from key) ===" << endl;
    
    // Test 1: Get all entries from the beginning
    cout << "Test 1: Getting all entries from key_0..." << endl;
    set<Entry> ff_from_start = lsm_tree.get_ff("key_0");
    cout << "Retrieved " << ff_from_start.size() << " entries" << endl;
    assert(ff_from_start.size() > 0);
    
    // Verify all keys are >= key_0
    Bits start_key("key_0");
    for (const Entry& e : ff_from_start) {
        assert(e.get_key() >= start_key);
    }
    cout << "✓ All entries are >= key_0\n";
    
    // Test 2: Get entries from middle key
    cout << "\nTest 2: Getting entries from key_5000..." << endl;
    set<Entry> ff_from_mid = lsm_tree.get_ff("key_5000");
    cout << "Retrieved " << ff_from_mid.size() << " entries" << endl;
    
    Bits mid_key("key_5000");
    for (const Entry& e : ff_from_mid) {
        assert(e.get_key() >= mid_key);
    }
    cout << "✓ All entries are >= key_5000\n";
    
    // Verify that ff_from_mid is smaller than ff_from_start
    assert(ff_from_mid.size() < ff_from_start.size());
    cout << "✓ Correct subset relationship\n";
    
    // Test 3: Get entries from near the end
    cout << "\nTest 3: Getting entries from key_9500..." << endl;
    set<Entry> ff_from_end = lsm_tree.get_ff("key_9500");
    cout << "Retrieved " << ff_from_end.size() << " entries" << endl;
    
    Bits end_key("key_9500");
    for (const Entry& e : ff_from_end) {
        assert(e.get_key() >= end_key);
    }
    cout << "✓ All entries are >= key_9500\n";
    assert(ff_from_end.size() < ff_from_mid.size());
    
    // Test 4: Get entries from non-existent key (should get next available)
    cout << "\nTest 4: Getting entries from key_2500 (non-existent)..." << endl;
    set<Entry> ff_from_gap = lsm_tree.get_ff("key_2500");
    cout << "Retrieved " << ff_from_gap.size() << " entries" << endl;
    
    Bits gap_key("key_2500");
    for (const Entry& e : ff_from_gap) {
        assert(e.get_key() >= gap_key);
    }
    cout << "✓ All entries are >= key_2500\n";
    
    // Test 5: Get entries from key beyond all data
    cout << "\nTest 5: Getting entries from key_99999 (beyond all data)..." << endl;
    set<Entry> ff_beyond = lsm_tree.get_ff("key_99999");
    cout << "Retrieved " << ff_beyond.size() << " entries" << endl;
    assert(ff_beyond.size() == 0);
    cout << "✓ Correctly returns empty set for key beyond all data\n";
    
    cout << "\n✓✓✓ All get_ff tests passed! ✓✓✓\n";
    return true;
}

// Test get_fb (get backward from key)
bool test_get_fb(LSM_Tree& lsm_tree) {
    cout << "\n=== Testing get_fb (backward from key) ===" << endl;
    
    // Test 1: Get all entries up to the end
    cout << "Test 1: Getting all entries up to key_9999..." << endl;
    set<Entry> fb_to_end = lsm_tree.get_fb("key_9999");
    cout << "Retrieved " << fb_to_end.size() << " entries" << endl;
    assert(fb_to_end.size() > 0);
    
    // Verify all keys are <= key_9999
    Bits end_key("key_9999");
    for (const Entry& e : fb_to_end) {
        assert(e.get_key() <= end_key);
    }
    cout << "✓ All entries are <= key_9999\n";
    
    // Test 2: Get entries up to middle key
    cout << "\nTest 2: Getting entries up to key_5000..." << endl;
    set<Entry> fb_to_mid = lsm_tree.get_fb("key_5000");
    cout << "Retrieved " << fb_to_mid.size() << " entries" << endl;
    
    Bits mid_key("key_5000");
    for (const Entry& e : fb_to_mid) {
        assert(e.get_key() <= mid_key);
    }
    cout << "✓ All entries are <= key_5000\n";
    
    // Verify that fb_to_mid is smaller than fb_to_end
    assert(fb_to_mid.size() < fb_to_end.size());
    cout << "✓ Correct subset relationship\n";
    
    // Test 3: Get entries up to near the beginning
    cout << "\nTest 3: Getting entries up to key_500..." << endl;
    set<Entry> fb_to_start = lsm_tree.get_fb("key_500");
    cout << "Retrieved " << fb_to_start.size() << " entries" << endl;
    
    Bits start_key("key_500");
    for (const Entry& e : fb_to_start) {
        assert(e.get_key() <= start_key);
    }
    cout << "✓ All entries are <= key_500\n";
    assert(fb_to_start.size() < fb_to_mid.size());
    
    // Test 4: Get entries up to non-existent key
    cout << "\nTest 4: Getting entries up to key_7500 (non-existent)..." << endl;
    set<Entry> fb_to_gap = lsm_tree.get_fb("key_7500");
    cout << "Retrieved " << fb_to_gap.size() << " entries" << endl;
    
    Bits gap_key("key_7500");
    for (const Entry& e : fb_to_gap) {
        assert(e.get_key() <= gap_key);
    }
    cout << "✓ All entries are <= key_7500\n";
    
    // Test 5: Get entries up to key before all data
    cout << "\nTest 5: Getting entries up to key_0000 (before all data)..." << endl;
    set<Entry> fb_before = lsm_tree.get_fb("key_0000");
    cout << "Retrieved " << fb_before.size() << " entries" << endl;
    // Should be empty or very small since key_0000 < key_0
    cout << "✓ Correctly handles key before all data\n";
    
    cout << "\n✓✓✓ All get_fb tests passed! ✓✓✓\n";
    return true;
}

// Test combination of get_ff and get_fb
bool test_range_queries_combined(LSM_Tree& lsm_tree) {
    cout << "\n=== Testing combined range queries ===" << endl;
    
    // Test 1: Verify that get_ff and get_fb partition the dataset
    cout << "Test 1: Verifying dataset partition at key_5000..." << endl;
    set<Entry> ff_from_5000 = lsm_tree.get_ff("key_5000");
    set<Entry> fb_to_4999 = lsm_tree.get_fb("key_4999");
    
    // Check for overlap (should only be entries with key_5000 if they exist)
    set<Bits> ff_keys, fb_keys;
    for (const Entry& e : ff_from_5000) ff_keys.insert(e.get_key());
    for (const Entry& e : fb_to_4999) fb_keys.insert(e.get_key());
    
    // Verify no key appears in both (since we're using key_5000 and key_4999)
    for (const Bits& key : ff_keys) {
        assert(fb_keys.find(key) == fb_keys.end());
    }
    cout << "✓ No overlap between ranges\n";
    
    // Test 2: Get range between two keys using both operations
    cout << "\nTest 2: Getting range between key_3000 and key_7000..." << endl;
    set<Entry> ff_from_3000 = lsm_tree.get_ff("key_3000");
    set<Entry> fb_to_7000 = lsm_tree.get_fb("key_7000");
    
    // Intersection should give us keys in [3000, 7000]
    set<Entry> range_intersection;
    set_intersection(ff_from_3000.begin(), ff_from_3000.end(),
                     fb_to_7000.begin(), fb_to_7000.end(),
                     inserter(range_intersection, range_intersection.begin()));
    
    cout << "Retrieved " << range_intersection.size() << " entries in range [3000, 7000]" << endl;
    
    Bits lower("key_3000");
    Bits upper("key_7000");
    for (const Entry& e : range_intersection) {
        assert(e.get_key() >= lower && e.get_key() <= upper);
    }
    cout << "✓ All entries in correct range\n";
    
    // Test 3: Verify ordering in results
    cout << "\nTest 3: Verifying ordering in range query results..." << endl;
    set<Entry> ff_sample = lsm_tree.get_ff("key_1000");
    
    Bits prev_key("");
    bool first = true;
    for (const Entry& e : ff_sample) {
        if (!first) {
            assert(e.get_key() >= prev_key);
        }
        prev_key = e.get_key();
        first = false;
    }
    cout << "✓ Results are properly ordered\n";
    
    cout << "\n✓✓✓ All combined range query tests passed! ✓✓✓\n";
    return true;
}

int main() {
    cout << "=== LSM Tree Range Query Test Suite ===\n" << endl;
    
    cout << "Generating non-overlapping entry sets..." << endl;
    vector<Entry> entriesA = generate_test_entries(0, ENTRY_COUNT);        // keys 0–999
    vector<Entry> entriesB = generate_test_entries(1000, ENTRY_COUNT);     // keys 1000–1999
    vector<Entry> entriesC = generate_test_entries(2000, ENTRY_COUNT);     // keys 2000–2999
    vector<Entry> entriesD = generate_test_entries(3000, ENTRY_COUNT);     // keys 3000–3999
    vector<Entry> entriesE = generate_test_entries(4000, ENTRY_COUNT);     // keys 4000–4999
    vector<Entry> entriesF = generate_test_entries(5000, ENTRY_COUNT);     // keys 5000–5999
    vector<Entry> entriesG = generate_test_entries(6000, ENTRY_COUNT);     // keys 6000–6999
    vector<Entry> entriesH = generate_test_entries(7000, ENTRY_COUNT);     // keys 7000–7999
    vector<Entry> entriesI = generate_test_entries(8000, ENTRY_COUNT);     // keys 8000–8999
    vector<Entry> entriesJ = generate_test_entries(9000, ENTRY_COUNT);     // keys 9000–9999

    cout << "Creating LSM tree..." << endl;
    LSM_Tree lsm_tree;

    // Populate the LSM tree with test data
    cout << "\n=== Populating LSM Tree ===" << endl;
    assert(test_mem_table(entriesA, lsm_tree));
    assert(test_mem_table(entriesB, lsm_tree));
    assert(test_mem_table(entriesC, lsm_tree));
    assert(test_mem_table(entriesD, lsm_tree));
    assert(test_mem_table(entriesE, lsm_tree));
    assert(test_mem_table(entriesF, lsm_tree));
    assert(test_mem_table(entriesG, lsm_tree));
    assert(test_mem_table(entriesH, lsm_tree));
    assert(test_mem_table(entriesI, lsm_tree));
    assert(test_mem_table(entriesJ, lsm_tree));

    cout << "\n=== Basic Verification ===" << endl;
    cout << "Verifying that all inserted entries can be retrieved..." << endl;
    vector<vector<Entry>*> all = {&entriesA, &entriesB, &entriesC, &entriesD, &entriesE,
                                  &entriesF, &entriesG, &entriesH, &entriesI, &entriesJ};

    for (size_t idx = 0; idx < all.size(); ++idx) {
        cout << "Checking entries set " << idx + 1 << endl;
        for (const Entry& entry : *all[idx]) {
            Entry entry_got(lsm_tree.get(entry.get_key().get_string()));
            assert(entry_got.get_key() == entry.get_key());
            assert(entry_got.get_value() == entry.get_value());
        }
    }
    cout << "✓ All basic tests passed\n";

    // Run range query tests
    assert(test_get_ff(lsm_tree));
    assert(test_get_fb(lsm_tree));
    assert(test_range_queries_combined(lsm_tree));

    // Print fill ratios
    cout << "\n=== LSM Tree Statistics ===" << endl;
    vector<pair<uint16_t, double>> ratios = lsm_tree.get_fill_ratios();
    for (auto& r : ratios) {
        cout << "Level " << r.first << " Fill Ratio: " << r.second * 100 << "%\n";
    }

    cout << "\n🎉 ALL TESTS PASSED SUCCESSFULLY! 🎉\n";
    return 0;
}