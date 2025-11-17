#include "../include/lsm_tree.h"
#include <cassert>
#include <iostream>
#include <vector>
#include <set>
#include <string>
#include <algorithm>
#include <chrono>
#include <random>

using namespace std;
using namespace chrono;

// ANSI color codes
#define RESET   "\033[0m"
#define RED     "\033[31m"
#define GREEN   "\033[32m"
#define YELLOW  "\033[33m"
#define BLUE    "\033[34m"
#define MAGENTA "\033[35m"
#define CYAN    "\033[36m"

const size_t TOTAL_ENTRIES = 2000000;
const size_t PAGE_SIZE = 1000;

// Helper to generate test entries with sequential keys
vector<Entry> generate_sequential_entries(size_t start, size_t count, const string& prefix = "key_") {
    vector<Entry> entries;
    entries.reserve(count);
    
    for (size_t i = 0; i < count; ++i) {
        string key_str = prefix + to_string(start + i);
        string val_str = "value_" + to_string(start + i);
        entries.emplace_back(Bits(key_str), Bits(val_str));
    }
    
    return entries;
}

// Helper to populate LSM tree with progress indicator
void populate_tree(LSM_Tree& tree, size_t total_entries) {
    cout << CYAN << "Populating tree with " << total_entries << " entries..." << RESET << endl;
    
    auto start = high_resolution_clock::now();
    const size_t batch_size = 100000;
    
    for (size_t i = 0; i < total_entries; i += batch_size) {
        size_t current_batch = min(batch_size, total_entries - i);
        vector<Entry> batch = generate_sequential_entries(i, current_batch);
        
        for (const Entry& entry : batch) {
            tree.set(entry.get_key().get_string(), entry.get_value().get_string());
        }
        
        if ((i + current_batch) % 500000 == 0 || i + current_batch == total_entries) {
            auto elapsed = duration_cast<seconds>(high_resolution_clock::now() - start).count();
            cout << "  Progress: " << (i + current_batch) << "/" << total_entries 
                 << " (" << (100.0 * (i + current_batch) / total_entries) << "%) "
                 << "Time: " << elapsed << "s" << endl;
        }
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start).count();
    cout << GREEN << "✓ Populated " << total_entries << " entries in " 
         << duration << "ms" << RESET << endl;
}

// Stress test: Forward pagination through all entries
void stress_test_forward_pagination() {
    cout << "\n" << BLUE << "=== STRESS TEST: Forward Pagination ===" << RESET << endl;
    cout << "Testing get_ff with " << TOTAL_ENTRIES << " entries, page size " << PAGE_SIZE << endl;
    
    LSM_Tree tree;
    populate_tree(tree, TOTAL_ENTRIES);
    
    auto start = high_resolution_clock::now();
    
    string cursor = "key_0";
    size_t page_num = 0;
    size_t total_entries_seen = 0;
    set<string> all_keys_seen;
    size_t duplicate_count = 0;
    size_t gap_count = 0;
    string last_key = "";
    
    while (true) {
        auto result = tree.get_ff(cursor, PAGE_SIZE);
        
        if (result.first.empty()) {
            break;
        }
        
        page_num++;
        
        // Check for duplicates and gaps
        for (const Entry& entry : result.first) {
            string key = entry.get_key().get_string();
            
            if (all_keys_seen.find(key) != all_keys_seen.end()) {
                duplicate_count++;
                if (duplicate_count <= 10) {
                    cout << RED << "  DUPLICATE: " << key << RESET << endl;
                }
            }
            
            all_keys_seen.insert(key);
            
            // Check for gaps (missing keys)
            if (!last_key.empty()) {
                // Extract numeric part
                size_t last_num = stoull(last_key.substr(4));
                size_t curr_num = stoull(key.substr(4));
                
                if (curr_num != last_num + 1) {
                    gap_count++;
                    if (gap_count <= 10) {
                        cout << YELLOW << "  GAP: from " << last_key << " to " << key 
                             << " (missing " << (curr_num - last_num - 1) << " keys)" << RESET << endl;
                    }
                }
            }
            
            last_key = key;
        }
        
        total_entries_seen += result.first.size();
        
        if (page_num % 100 == 0) {
            cout << "  Page " << page_num << ": " << total_entries_seen << " entries seen" << endl;
        }
        
        cursor = result.second;
        if (cursor == "ENTRY_PLACEHOLDER_KEY" || cursor == "_placeholder_key") {
            break;
        }
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start).count();
    
    // Results
    cout << "\n" << CYAN << "Forward Pagination Results:" << RESET << endl;
    cout << "  Total pages: " << page_num << endl;
    cout << "  Total entries seen: " << total_entries_seen << endl;
    cout << "  Unique keys: " << all_keys_seen.size() << endl;
    cout << "  Expected entries: " << TOTAL_ENTRIES << endl;
    cout << "  Duplicates: " << duplicate_count << endl;
    cout << "  Gaps detected: " << gap_count << endl;
    cout << "  Time: " << duration << "ms" << endl;
    
    if (all_keys_seen.size() == TOTAL_ENTRIES && duplicate_count == 0 && gap_count == 0) {
        cout << GREEN << "✓ PASS: Perfect pagination!" << RESET << endl;
    } else {
        cout << RED << "✗ FAIL: Pagination issues detected!" << RESET << endl;
    }
}

// Stress test: Backward pagination through all entries
void stress_test_backward_pagination() {
    cout << "\n" << BLUE << "=== STRESS TEST: Backward Pagination ===" << RESET << endl;
    cout << "Testing get_fb with " << TOTAL_ENTRIES << " entries, page size " << PAGE_SIZE << endl;
    
    LSM_Tree tree;
    populate_tree(tree, TOTAL_ENTRIES);
    
    auto start = high_resolution_clock::now();
    
    string cursor = "key_" + to_string(TOTAL_ENTRIES - 1);
    size_t page_num = 0;
    size_t total_entries_seen = 0;
    set<string> all_keys_seen;
    size_t duplicate_count = 0;
    
    while (true) {
        auto result = tree.get_fb(cursor, PAGE_SIZE);
        
        if (result.first.empty()) {
            break;
        }
        
        page_num++;
        
        for (const Entry& entry : result.first) {
            string key = entry.get_key().get_string();
            
            if (all_keys_seen.find(key) != all_keys_seen.end()) {
                duplicate_count++;
                if (duplicate_count <= 10) {
                    cout << RED << "  DUPLICATE: " << key << RESET << endl;
                }
            }
            
            all_keys_seen.insert(key);
        }
        
        total_entries_seen += result.first.size();
        
        if (page_num % 100 == 0) {
            cout << "  Page " << page_num << ": " << total_entries_seen << " entries seen" << endl;
        }
        
        cursor = result.second;
        if (cursor == "ENTRY_PLACEHOLDER_KEY" || cursor == "_placeholder_key") {
            break;
        }
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start).count();
    
    cout << "\n" << CYAN << "Backward Pagination Results:" << RESET << endl;
    cout << "  Total pages: " << page_num << endl;
    cout << "  Total entries seen: " << total_entries_seen << endl;
    cout << "  Unique keys: " << all_keys_seen.size() << endl;
    cout << "  Expected entries: " << TOTAL_ENTRIES << endl;
    cout << "  Duplicates: " << duplicate_count << endl;
    cout << "  Time: " << duration << "ms" << endl;
    
    if (all_keys_seen.size() == TOTAL_ENTRIES && duplicate_count == 0) {
        cout << GREEN << "✓ PASS: Perfect backward pagination!" << RESET << endl;
    } else {
        cout << RED << "✗ FAIL: Backward pagination issues detected!" << RESET << endl;
    }
}

// Stress test: get_keys_cursor pagination
void stress_test_keys_cursor() {
    cout << "\n" << BLUE << "=== STRESS TEST: Keys Cursor Pagination ===" << RESET << endl;
    cout << "Testing get_keys_cursor with " << TOTAL_ENTRIES << " entries, page size " << PAGE_SIZE << endl;
    
    LSM_Tree tree;
    populate_tree(tree, TOTAL_ENTRIES);
    
    auto start = high_resolution_clock::now();
    
    string cursor = "key_0";
    size_t page_num = 0;
    set<string> all_keys_seen;
    size_t duplicate_count = 0;
    
    while (true) {
        auto result = tree.get_keys_cursor(cursor, PAGE_SIZE);
        
        if (result.first.empty()) {
            break;
        }
        
        page_num++;
        
        for (const Bits& key : result.first) {
            string key_str = key.get_string();
            
            if (all_keys_seen.find(key_str) != all_keys_seen.end()) {
                duplicate_count++;
                if (duplicate_count <= 10) {
                    cout << RED << "  DUPLICATE: " << key_str << RESET << endl;
                }
            }
            
            all_keys_seen.insert(key_str);
        }
        
        if (page_num % 100 == 0) {
            cout << "  Page " << page_num << ": " << all_keys_seen.size() << " unique keys" << endl;
        }
        
        cursor = result.second;
        if (cursor == "ENTRY_PLACEHOLDER_KEY" || cursor == "_placeholder_key") {
            break;
        }
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start).count();
    
    cout << "\n" << CYAN << "Keys Cursor Results:" << RESET << endl;
    cout << "  Total pages: " << page_num << endl;
    cout << "  Unique keys: " << all_keys_seen.size() << endl;
    cout << "  Expected keys: " << TOTAL_ENTRIES << endl;
    cout << "  Duplicates: " << duplicate_count << endl;
    cout << "  Time: " << duration << "ms" << endl;
    
    if (all_keys_seen.size() == TOTAL_ENTRIES && duplicate_count == 0) {
        cout << GREEN << "✓ PASS: Perfect keys cursor pagination!" << RESET << endl;
    } else {
        cout << RED << "✗ FAIL: Keys cursor pagination issues!" << RESET << endl;
    }
}

// Stress test: Random access pagination
void stress_test_random_access() {
    cout << "\n" << BLUE << "=== STRESS TEST: Random Access Pagination ===" << RESET << endl;
    cout << "Testing random starting points with " << TOTAL_ENTRIES << " entries" << endl;
    
    LSM_Tree tree;
    populate_tree(tree, TOTAL_ENTRIES);
    
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<size_t> dist(0, TOTAL_ENTRIES - 1000);
    
    const size_t num_random_tests = 100;
    size_t successful_tests = 0;
    
    auto start = high_resolution_clock::now();
    
    for (size_t test = 0; test < num_random_tests; test++) {
        size_t random_start = dist(gen);
        string cursor = "key_" + to_string(random_start);
        
        set<string> keys_in_test;
        size_t pages = 0;
        bool has_duplicates = false;
        
        // Paginate forward for 10 pages
        for (int page = 0; page < 10; page++) {
            auto result = tree.get_ff(cursor, 100);
            
            if (result.first.empty()) break;
            
            pages++;
            
            for (const Entry& entry : result.first) {
                string key = entry.get_key().get_string();
                if (keys_in_test.find(key) != keys_in_test.end()) {
                    has_duplicates = true;
                }
                keys_in_test.insert(key);
            }
            
            cursor = result.second;
            if (cursor == "ENTRY_PLACEHOLDER_KEY" || cursor == "_placeholder_key") {
                break;
            }
        }
        
        if (!has_duplicates && pages > 0) {
            successful_tests++;
        }
        
        if ((test + 1) % 20 == 0) {
            cout << "  Completed " << (test + 1) << "/" << num_random_tests 
                 << " random tests..." << endl;
        }
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start).count();
    
    cout << "\n" << CYAN << "Random Access Results:" << RESET << endl;
    cout << "  Tests performed: " << num_random_tests << endl;
    cout << "  Successful (no duplicates): " << successful_tests << endl;
    cout << "  Success rate: " << (100.0 * successful_tests / num_random_tests) << "%" << endl;
    cout << "  Time: " << duration << "ms" << endl;
    
    if (successful_tests == num_random_tests) {
        cout << GREEN << "✓ PASS: All random access tests passed!" << RESET << endl;
    } else {
        cout << RED << "✗ FAIL: Some random access tests failed!" << RESET << endl;
    }
}

// Stress test: Prefix pagination with multiple prefixes
void stress_test_prefix_pagination() {
    cout << "\n" << BLUE << "=== STRESS TEST: Prefix Pagination ===" << RESET << endl;
    
    const size_t entries_per_prefix = 100000;
    const size_t num_prefixes = 20;
    
    LSM_Tree tree;
    
    cout << CYAN << "Populating with " << (entries_per_prefix * num_prefixes) 
         << " entries across " << num_prefixes << " prefixes..." << RESET << endl;
    
    auto pop_start = high_resolution_clock::now();
    
    for (size_t p = 0; p < num_prefixes; p++) {
        string prefix = "prefix" + to_string(p) + "_";
        vector<Entry> entries = generate_sequential_entries(0, entries_per_prefix, prefix);
        
        for (const Entry& entry : entries) {
            tree.set(entry.get_key().get_string(), entry.get_value().get_string());
        }
        
        if ((p + 1) % 5 == 0) {
            cout << "  Populated " << (p + 1) << "/" << num_prefixes << " prefixes" << endl;
        }
    }
    
    auto pop_end = high_resolution_clock::now();
    cout << GREEN << "✓ Population complete in " 
         << duration_cast<seconds>(pop_end - pop_start).count() << "s" << RESET << endl;
    
    // Test each prefix
    size_t total_success = 0;
    auto test_start = high_resolution_clock::now();
    
    for (size_t p = 0; p < num_prefixes; p++) {
        string prefix = "prefix" + to_string(p) + "_";
        string cursor = prefix + "0";
        
        set<string> keys_seen;
        size_t wrong_prefix_count = 0;
        
        while (true) {
            auto result = tree.get_keys_cursor_prefix(prefix, cursor, PAGE_SIZE);
            
            if (result.first.empty()) break;
            
            for (const Bits& key : result.first) {
                string key_str = key.get_string();
                if (key_str.substr(0, prefix.length()) != prefix) {
                    wrong_prefix_count++;
                }
                keys_seen.insert(key_str);
            }
            
            cursor = result.second;
            if (cursor == "ENTRY_PLACEHOLDER_KEY" || cursor == "_placeholder_key") {
                break;
            }
        }
        
        if (keys_seen.size() == entries_per_prefix && wrong_prefix_count == 0) {
            total_success++;
        } else {
            cout << RED << "  Prefix '" << prefix << "': " << keys_seen.size() 
                 << "/" << entries_per_prefix << " (wrong: " << wrong_prefix_count << ")" << RESET << endl;
        }
    }
    
    auto test_end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(test_end - test_start).count();
    
    cout << "\n" << CYAN << "Prefix Pagination Results:" << RESET << endl;
    cout << "  Prefixes tested: " << num_prefixes << endl;
    cout << "  Successful: " << total_success << endl;
    cout << "  Success rate: " << (100.0 * total_success / num_prefixes) << "%" << endl;
    cout << "  Time: " << duration << "ms" << endl;
    
    if (total_success == num_prefixes) {
        cout << GREEN << "✓ PASS: All prefix pagination tests passed!" << RESET << endl;
    } else {
        cout << RED << "✗ FAIL: Some prefix pagination tests failed!" << RESET << endl;
    }
}

int main() {
    cout << BLUE << "\n╔═══════════════════════════════════════════════════════════╗" << RESET << endl;
    cout << BLUE << "║       LSM Tree Cursor - STRESS TEST SUITE                ║" << RESET << endl;
    cout << BLUE << "║       Testing with 2,000,000 entries                     ║" << RESET << endl;
    cout << BLUE << "╚═══════════════════════════════════════════════════════════╝" << RESET << endl;
    
    auto overall_start = high_resolution_clock::now();
    
    try {
        stress_test_forward_pagination();
        stress_test_backward_pagination();
        stress_test_keys_cursor();
        stress_test_random_access();
        stress_test_prefix_pagination();
        
        auto overall_end = high_resolution_clock::now();
        auto total_duration = duration_cast<seconds>(overall_end - overall_start).count();
        
        cout << "\n" << GREEN << "═══════════════════════════════════════════════════════════" << RESET << endl;
        cout << GREEN << "  All stress tests completed!" << RESET << endl;
        cout << GREEN << "  Total time: " << total_duration << " seconds" << RESET << endl;
        cout << GREEN << "═══════════════════════════════════════════════════════════" << RESET << endl;
        
    } catch (const exception& e) {
        cerr << "\n" << RED << "✗ Test failed with exception: " << e.what() << RESET << endl;
        return 1;
    } catch (...) {
        cerr << "\n" << RED << "✗ Test failed with unknown exception" << RESET << endl;
        return 1;
    }
    
    return 0;
}