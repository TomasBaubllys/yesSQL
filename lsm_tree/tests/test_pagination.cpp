#include "../include/lsm_tree.h"
#include <cassert>
#include <iostream>
#include <vector>
#include <set>
#include <string>
#include <algorithm>

using namespace std;

// ANSI color codes for better visibility
#define RESET   "\033[0m"
#define RED     "\033[31m"
#define GREEN   "\033[32m"
#define YELLOW  "\033[33m"
#define BLUE    "\033[34m"
#define MAGENTA "\033[35m"
#define CYAN    "\033[36m"

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

// Helper to populate LSM tree
void populate_tree(LSM_Tree& tree, const vector<Entry>& entries) {
    for (const Entry& entry : entries) {
        tree.set(entry.get_key().get_string(), entry.get_value().get_string());
    }
}

// Helper to print a set of keys
void print_keys(const set<Bits>& keys, const string& label, int max_display = 10) {
    cout << CYAN << label << " (" << keys.size() << " keys): " << RESET;
    int count = 0;
    for (const Bits& key : keys) {
        if (count >= max_display) {
            cout << "...";
            break;
        }
        cout << key.get_string();
        if (count < keys.size() - 1 && count < max_display - 1) cout << ", ";
        count++;
    }
    cout << endl;
}

// Helper to print a set of entries
void print_entries(const set<Entry>& entries, const string& label, int max_display = 10) {
    cout << CYAN << label << " (" << entries.size() << " entries): " << RESET;
    int count = 0;
    for (const Entry& entry : entries) {
        if (count >= max_display) {
            cout << "...";
            break;
        }
        cout << entry.get_key().get_string();
        if (count < entries.size() - 1 && count < max_display - 1) cout << ", ";
        count++;
    }
    cout << endl;
}

// Analyze next_key value
void analyze_next_key(const string& next_key, const set<Bits>& returned_keys, const string& test_name) {
    cout << YELLOW << "\n[" << test_name << "] Next Key Analysis:" << RESET << endl;
    cout << "  Next key returned: " << MAGENTA << next_key << RESET << endl;
    
    if (next_key == "ENTRY_PLACEHOLDER_KEY" || next_key == "_placeholder_key") {
        cout << "  Status: " << GREEN << "End of results (placeholder)" << RESET << endl;
        return;
    }
    
    // Check if next_key is in the returned set (BUG if true!)
    bool found_in_results = false;
    for (const Bits& key : returned_keys) {
        if (key.get_string() == next_key) {
            found_in_results = true;
            break;
        }
    }
    
    if (found_in_results) {
        cout << "  Status: " << RED << "ERROR! Next key is IN the returned results!" << RESET << endl;
    } else {
        cout << "  Status: " << GREEN << "OK - Next key not in returned results" << RESET << endl;
    }
    
    // Check ordering relative to returned keys
    if (!returned_keys.empty()) {
        string last_returned = returned_keys.rbegin()->get_string();
        cout << "  Last returned key: " << last_returned << endl;
        
        if (next_key <= last_returned) {
            cout << "  Ordering: " << RED << "ERROR! Next key <= last returned key!" << RESET << endl;
        } else {
            cout << "  Ordering: " << GREEN << "OK - Next key > last returned key" << RESET << endl;
        }
    }
}

// Analyze next_key for entries
void analyze_next_key_entries(const string& next_key, const set<Entry>& returned_entries, const string& test_name) {
    cout << YELLOW << "\n[" << test_name << "] Next Key Analysis:" << RESET << endl;
    cout << "  Next key returned: " << MAGENTA << next_key << RESET << endl;
    
    if (next_key == "ENTRY_PLACEHOLDER_KEY" || next_key == "_placeholder_key") {
        cout << "  Status: " << GREEN << "End of results (placeholder)" << RESET << endl;
        return;
    }
    
    // Check if next_key is in the returned set
    bool found_in_results = false;
    for (const Entry& entry : returned_entries) {
        if (entry.get_key().get_string() == next_key) {
            found_in_results = true;
            break;
        }
    }
    
    if (found_in_results) {
        cout << "  Status: " << RED << "ERROR! Next key is IN the returned results!" << RESET << endl;
    } else {
        cout << "  Status: " << GREEN << "OK - Next key not in returned results" << RESET << endl;
    }
    
    // Check ordering
    if (!returned_entries.empty()) {
        string last_returned = returned_entries.rbegin()->get_key().get_string();
        cout << "  Last returned key: " << last_returned << endl;
        
        if (next_key <= last_returned) {
            cout << "  Ordering: " << RED << "ERROR! Next key <= last returned key!" << RESET << endl;
        } else {
            cout << "  Ordering: " << GREEN << "OK - Next key > last returned key" << RESET << endl;
        }
    }
}

// Test 1: Detailed get_keys_cursor analysis
void test_get_keys_cursor_detailed() {
    cout << "\n" << BLUE << "=== Test: get_keys_cursor detailed analysis ===" << RESET << endl;
    LSM_Tree tree;
    
    vector<Entry> entries = generate_sequential_entries(0, 100);
    populate_tree(tree, entries);
    
    cout << "\nInserted 100 entries: key_0 through key_99" << endl;
    
    // Page 1
    cout << "\n" << GREEN << "--- Page 1: Starting from 'key_0', requesting 10 ---" << RESET << endl;
    auto page1 = tree.get_keys_cursor("key_0", 10);
    print_keys(page1.first, "Returned keys", 20);
    analyze_next_key(page1.second, page1.first, "Page 1");
    
    // Page 2
    if (page1.second != "ENTRY_PLACEHOLDER_KEY" && page1.second != "_placeholder_key") {
        cout << "\n" << GREEN << "--- Page 2: Using cursor '" << page1.second << "', requesting 10 ---" << RESET << endl;
        auto page2 = tree.get_keys_cursor(page1.second, 10);
        print_keys(page2.first, "Returned keys", 20);
        analyze_next_key(page2.second, page2.first, "Page 2");
        
        // Check for overlaps
        cout << "\n" << YELLOW << "Checking for overlaps between Page 1 and Page 2:" << RESET << endl;
        int overlap_count = 0;
        for (const Bits& key : page1.first) {
            if (page2.first.find(key) != page2.first.end()) {
                cout << "  " << RED << "DUPLICATE: " << key.get_string() << RESET << endl;
                overlap_count++;
            }
        }
        if (overlap_count == 0) {
            cout << "  " << GREEN << "No overlaps found!" << RESET << endl;
        } else {
            cout << "  " << RED << "Found " << overlap_count << " duplicates!" << RESET << endl;
        }
    }
}

// Test 2: Detailed get_ff analysis
void test_get_ff_detailed() {
    cout << "\n" << BLUE << "=== Test: get_ff detailed analysis ===" << RESET << endl;
    LSM_Tree tree;
    
    vector<Entry> entries = generate_sequential_entries(0, 100);
    populate_tree(tree, entries);
    
    cout << "\nInserted 100 entries: key_0 through key_99" << endl;
    
    // Page 1
    cout << "\n" << GREEN << "--- Page 1: Starting from 'key_0', requesting 10 ---" << RESET << endl;
    auto page1 = tree.get_ff("key_0", 10);
    print_entries(page1.first, "Returned entries", 20);
    analyze_next_key_entries(page1.second, page1.first, "get_ff Page 1");
    
    // Page 2
    if (page1.second != "ENTRY_PLACEHOLDER_KEY" && page1.second != "_placeholder_key") {
        cout << "\n" << GREEN << "--- Page 2: Using cursor '" << page1.second << "', requesting 10 ---" << RESET << endl;
        auto page2 = tree.get_ff(page1.second, 10);
        print_entries(page2.first, "Returned entries", 20);
        analyze_next_key_entries(page2.second, page2.first, "get_ff Page 2");
        
        // Check for overlaps
        cout << "\n" << YELLOW << "Checking for overlaps between Page 1 and Page 2:" << RESET << endl;
        int overlap_count = 0;
        for (const Entry& entry1 : page1.first) {
            for (const Entry& entry2 : page2.first) {
                if (entry1.get_key() == entry2.get_key()) {
                    cout << "  " << RED << "DUPLICATE: " << entry1.get_key().get_string() << RESET << endl;
                    overlap_count++;
                    break;
                }
            }
        }
        if (overlap_count == 0) {
            cout << "  " << GREEN << "No overlaps found!" << RESET << endl;
        } else {
            cout << "  " << RED << "Found " << overlap_count << " duplicates!" << RESET << endl;
        }
    }
}

// Test 3: Detailed get_fb analysis
void test_get_fb_detailed() {
    cout << "\n" << BLUE << "=== Test: get_fb detailed analysis ===" << RESET << endl;
    LSM_Tree tree;
    
    vector<Entry> entries = generate_sequential_entries(0, 100);
    populate_tree(tree, entries);
    
    cout << "\nInserted 100 entries: key_0 through key_99" << endl;
    
    // Page 1
    cout << "\n" << GREEN << "--- Page 1: Starting from 'key_99', requesting 10 (backward) ---" << RESET << endl;
    auto page1 = tree.get_fb("key_99", 10);
    print_entries(page1.first, "Returned entries", 20);
    analyze_next_key_entries(page1.second, page1.first, "get_fb Page 1");
    
    // Page 2
    if (page1.second != "ENTRY_PLACEHOLDER_KEY" && page1.second != "_placeholder_key") {
        cout << "\n" << GREEN << "--- Page 2: Using cursor '" << page1.second << "', requesting 10 ---" << RESET << endl;
        auto page2 = tree.get_fb(page1.second, 10);
        print_entries(page2.first, "Returned entries", 20);
        analyze_next_key_entries(page2.second, page2.first, "get_fb Page 2");
        
        // Check for overlaps
        cout << "\n" << YELLOW << "Checking for overlaps between Page 1 and Page 2:" << RESET << endl;
        int overlap_count = 0;
        for (const Entry& entry1 : page1.first) {
            for (const Entry& entry2 : page2.first) {
                if (entry1.get_key() == entry2.get_key()) {
                    cout << "  " << RED << "DUPLICATE: " << entry1.get_key().get_string() << RESET << endl;
                    overlap_count++;
                    break;
                }
            }
        }
        if (overlap_count == 0) {
            cout << "  " << GREEN << "No overlaps found!" << RESET << endl;
        } else {
            cout << "  " << RED << "Found " << overlap_count << " duplicates!" << RESET << endl;
        }
    }
}

// Test 4: Multi-page pagination with full trace
void test_multi_page_pagination() {
    cout << "\n" << BLUE << "=== Test: Multi-page pagination (get_ff) ===" << RESET << endl;
    LSM_Tree tree;
    
    vector<Entry> entries = generate_sequential_entries(0, 50);
    populate_tree(tree, entries);
    
    cout << "\nInserted 50 entries: key_0 through key_49" << endl;
    cout << "Will paginate through with page size = 7\n" << endl;
    
    string cursor = "key_0";
    int page_num = 1;
    set<string> all_keys_seen;
    
    while (page_num <= 10) {  // Safety limit
        cout << "\n" << GREEN << "--- Page " << page_num << ": cursor = '" << cursor << "' ---" << RESET << endl;
        
        auto result = tree.get_ff(cursor, 7);
        
        if (result.first.empty()) {
            cout << "  Empty result set - end of data" << endl;
            break;
        }
        
        print_entries(result.first, "  Returned", 10);
        cout << "  Next cursor: " << MAGENTA << result.second << RESET << endl;
        
        // Check for duplicates across pages
        int dup_count = 0;
        for (const Entry& entry : result.first) {
            string key = entry.get_key().get_string();
            if (all_keys_seen.find(key) != all_keys_seen.end()) {
                cout << "    " << RED << "DUPLICATE ACROSS PAGES: " << key << RESET << endl;
                dup_count++;
            }
            all_keys_seen.insert(key);
        }
        
        if (dup_count > 0) {
            cout << "  " << RED << "ERROR: Found " << dup_count << " duplicates!" << RESET << endl;
        }
        
        cursor = result.second;
        if (cursor == "ENTRY_PLACEHOLDER_KEY" || cursor == "_placeholder_key") {
            cout << "\n  " << GREEN << "Reached end marker - pagination complete" << RESET << endl;
            break;
        }
        
        page_num++;
    }
    
    cout << "\n" << CYAN << "Total unique keys seen across all pages: " << all_keys_seen.size() << RESET << endl;
    cout << CYAN << "Expected: 50 keys" << RESET << endl;
    
    if (all_keys_seen.size() == 50) {
        cout << GREEN << "✓ Pagination retrieved all keys exactly once!" << RESET << endl;
    } else {
        cout << RED << "✗ Pagination error - wrong number of keys!" << RESET << endl;
    }
}

// Test 5: Edge case - cursor at exact key boundaries
void test_cursor_boundaries() {
    cout << "\n" << BLUE << "=== Test: Cursor at exact key boundaries ===" << RESET << endl;
    LSM_Tree tree;
    
    vector<Entry> entries = generate_sequential_entries(0, 30);
    populate_tree(tree, entries);
    
    cout << "\nInserted 30 entries: key_0 through key_29" << endl;
    
    // Test starting exactly at an existing key
    cout << "\n" << GREEN << "--- Query from exact key 'key_15' ---" << RESET << endl;
    auto result1 = tree.get_ff("key_15", 5);
    print_entries(result1.first, "Returned", 10);
    
    bool includes_key15 = false;
    for (const Entry& e : result1.first) {
        if (e.get_key().get_string() == "key_15") {
            includes_key15 = true;
            break;
        }
    }
    
    if (includes_key15) {
        cout << "  " << GREEN << "✓ Correctly includes the starting key 'key_15'" << RESET << endl;
    } else {
        cout << "  " << RED << "✗ ERROR: Starting key 'key_15' not in results!" << RESET << endl;
    }
    
    analyze_next_key_entries(result1.second, result1.first, "Boundary test");
}

// Test 6: Prefix pagination detailed
void test_prefix_pagination_detailed() {
    cout << "\n" << BLUE << "=== Test: Prefix pagination detailed ===" << RESET << endl;
    LSM_Tree tree;
    
    vector<Entry> alpha = generate_sequential_entries(0, 30, "alpha_");
    vector<Entry> beta = generate_sequential_entries(0, 30, "beta_");
    vector<Entry> gamma = generate_sequential_entries(0, 30, "gamma_");
    
    populate_tree(tree, alpha);
    populate_tree(tree, beta);
    populate_tree(tree, gamma);
    
    cout << "\nInserted 90 entries: 30 with prefix 'alpha_', 30 with 'beta_', 30 with 'gamma_'" << endl;
    
    // Paginate through beta_ only
    cout << "\n" << GREEN << "--- Paginating through 'beta_' prefix only ---" << RESET << endl;
    
    string cursor = "beta_0";
    int page = 1;
    int total_beta_keys = 0;
    
    while (page <= 5) {
        cout << "\n  Page " << page << ": cursor = '" << cursor << "'" << endl;
        auto result = tree.get_keys_cursor_prefix("beta_", cursor, 8);
        
        if (result.first.empty()) break;
        
        print_keys(result.first, "    Returned", 10);
        
        // Verify all keys have correct prefix
        int wrong_prefix = 0;
        for (const Bits& key : result.first) {
            if (key.get_string().substr(0, 5) != "beta_") {
                cout << "      " << RED << "WRONG PREFIX: " << key.get_string() << RESET << endl;
                wrong_prefix++;
            }
        }
        
        if (wrong_prefix == 0) {
            cout << "    " << GREEN << "✓ All keys have 'beta_' prefix" << RESET << endl;
        }
        
        total_beta_keys += result.first.size();
        cursor = result.second;
        
        if (cursor == "ENTRY_PLACEHOLDER_KEY" || cursor == "_placeholder_key") break;
        
        page++;
    }
    
    cout << "\n" << CYAN << "Total 'beta_' keys retrieved: " << total_beta_keys << RESET << endl;
    cout << CYAN << "Expected: 30" << RESET << endl;
}

// Main test runner
int main() {
    cout << BLUE << "\n╔════════════════════════════════════════════════════════════╗" << RESET << endl;
    cout << BLUE << "║  LSM Tree Cursor Function - In-Depth Analysis Suite       ║" << RESET << endl;
    cout << BLUE << "╚════════════════════════════════════════════════════════════╝" << RESET << endl;
    
    try {
        test_get_keys_cursor_detailed();
        test_get_ff_detailed();
        test_get_fb_detailed();
        test_multi_page_pagination();
        test_cursor_boundaries();
        test_prefix_pagination_detailed();
        
        cout << "\n" << GREEN << "════════════════════════════════════════════════════════════" << RESET << endl;
        cout << GREEN << "  All detailed analysis tests completed!" << RESET << endl;
        cout << GREEN << "════════════════════════════════════════════════════════════" << RESET << endl;
        
    } catch (const exception& e) {
        cerr << "\n" << RED << "✗ Test failed with exception: " << e.what() << RESET << endl;
        return 1;
    } catch (...) {
        cerr << "\n" << RED << "✗ Test failed with unknown exception" << RESET << endl;
        return 1;
    }
    
    return 0;
}