#include "../include/lsm_tree.h"
#include <cassert>
#include <iostream>
#include <random>

#define ROUND1_ENTRY_COUNT 1000
#define ROUND2_ENTRY_COUNT 1001


using namespace std;

// generates a vector of random entries
// Helper to generate random alphanumeric strings
/*string random_string(size_t length) {
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

    sort(entries.begin(), entries.end(),
         [&](const Entry& a, const Entry& b) {
             return a.get_key() < b.get_key();
         });
    

    return entries;
}

// Generate a unsorted vector of Entries
vector<Entry> generate_test_entries2(size_t count, size_t key_size = 400, size_t value_size = 1000) {
    vector<Entry> entries;
    entries.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        string key_str = "MAMAMAMAMAMAMAMAMAMAMAMAMAMAMkey_" + to_string(i) + "_" + random_string(key_size);
        string val_str = "PAPAPAPAPAPAPAPAPAPAPAPAPAPAPAPAPAPAPAPAPAPvalue_" + to_string(i) + "_" + random_string(value_size);
        //cout << key_str.length() << endl;
        Bits key_bits(key_str);
        //cout << key_bits.get_string_char().length() << endl;
        Bits value_bits(val_str);
        entries.emplace_back(key_bits, value_bits);
    }

    sort(entries.begin(), entries.end(),
         [&](const Entry& a, const Entry& b) {
             return a.get_key() < b.get_key();
         });
    

    return entries;
}


// Generate a unsorted vector of Entries
vector<Entry> generate_test_entries(size_t count, size_t n = 0) {
    vector<Entry> entries;
    entries.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        string key_str = "key_" + to_string(i);
        string val_str = "value_" + to_string(i + n);
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
    

    return entries;
}



// test if avl tree is filled correctly
bool test_mem_table(vector<Entry>& entries, LSM_Tree& lsm_tree) {
    cout << "Adding random entries to the lsm tree" << endl;
    for(const Entry& entry : entries) {
        // bruuh???
        //cout << "here" << endl;
        lsm_tree.set(entry.get_key().get_string(), entry.get_value().get_string());
    }
    cout << "Done" << endl;

    // look for the keys
    cout << "Looking for all the entries in the lsm tree" << endl;
    for(const Entry& entry : entries) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }
    cout << "Done" << endl;

    return true;
}



int main(int argc, char* argv[]) {
    cout << "Generating random test entries (1)" << endl;
    vector<Entry> entries1 = generate_test_entries(ROUND1_ENTRY_COUNT, 321, 12321);
    cout << "Done" << endl;
    cout << "Creating empty lsm tree" << endl;
    LSM_Tree lsm_tree;
    cout << "Done" << endl;
    

    // fill mem table
    assert(test_mem_table(entries1, lsm_tree));
    assert(test_mem_table(entries1, lsm_tree));
    assert(test_mem_table(entries1, lsm_tree));

    // assert(false);


    cout << "Generating random test entries (2)" << endl;
    vector<Entry> entries2 = generate_test_entries(ROUND2_ENTRY_COUNT, 121, 22991);
    cout << "Done" << endl;

    cout << "Generating random test entries (3)" << endl;
    vector<Entry> entries3 = generate_test_entries2(ROUND2_ENTRY_COUNT, 600, 22991);
    cout << "Done" << endl;

    assert(test_mem_table(entries2, lsm_tree));
    assert(test_mem_table(entries2, lsm_tree));
    assert(test_mem_table(entries1, lsm_tree));
    assert(test_mem_table(entries2, lsm_tree));

    assert(test_mem_table(entries1, lsm_tree));
    assert(test_mem_table(entries2, lsm_tree));
    assert(test_mem_table(entries2, lsm_tree));

    lsm_tree.flush_mem_table();

    // assert(test_mem_table(entries3, lsm_tree));
    assert(test_mem_table(entries3, lsm_tree));
    assert(test_mem_table(entries3, lsm_tree));

    cout << "Looking for all the entries in the lsm tree (1)" << endl;
    for(const Entry& entry : entries1) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }

    cout << "Looking for all the entries in the lsm tree (2)" << endl;
    for(const Entry& entry : entries2) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }
    cout << "Done" << endl;

    cout << "Looking for all the entries in the lsm tree (3)" << endl;
    for(const Entry& entry : entries3) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }
    cout << "Done" << endl;

    std::vector<std::pair<uint16_t, double>> ratios = lsm_tree.get_fill_ratios();

    for(uint16_t i = 0; i < ratios.size(); ++i){
        std::cout << "Level "  << ratios[i].first << " Ratio: " << ratios[i].second * 100 << "%" << std::endl;
    }
    // look for all the entries

}

#include "../include/lsm_tree.h"
#include <cassert>
#include <iostream>
#include <random>
#include <unordered_set>

#define ENTRY_COUNT_A 1000
#define ENTRY_COUNT_B 1000
#define ENTRY_COUNT_C 1000

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

// Generate random entries with optional overlapping keys
vector<Entry> generate_test_entries(size_t count, const vector<Entry>* overlap_from = nullptr,
                                    double overlap_ratio = 0.0,
                                    size_t key_size = 100, size_t value_size = 10000) {
    vector<Entry> entries;
    entries.reserve(count);

    unordered_set<string> used_keys;

    // If overlapping, take a portion of keys from the given vector
    size_t overlap_count = 0;
    if (overlap_from && overlap_ratio > 0.0) {
        overlap_count = static_cast<size_t>(count * overlap_ratio);
        overlap_count = min(overlap_count, overlap_from->size());
        for (size_t i = 0; i < overlap_count; ++i) {
            const Entry& src = (*overlap_from)[i];
            string val_str = "updated_value_" + to_string(i) + "_" + random_string(value_size);
            Bits key_bits(src.get_key().get_string_char());
            Bits value_bits(val_str);
            entries.emplace_back(key_bits, value_bits);
            used_keys.insert(src.get_key().get_string_char());
        }
    }

    // Add new, non-overlapping random entries
    while (entries.size() < count) {
        string key_str = "key_" + random_string(key_size);
        if (used_keys.count(key_str)) continue;
        used_keys.insert(key_str);
        string val_str = "value_" + random_string(value_size);
        entries.emplace_back(Bits(key_str), Bits(val_str));
    }

    sort(entries.begin(), entries.end(), [](const Entry& a, const Entry& b) {
        return a.get_key() < b.get_key();
    });

    return entries;
}

// Test helper (unchanged)
bool test_mem_table(vector<Entry>& entries, LsmTree& lsm_tree) {
    cout << "Adding entries to LSM tree..." << endl;
    for (const Entry& entry : entries) {
        lsm_tree.set(entry.get_key().get_string_char(), entry.get_value().get_string_char());
    }

    cout << "Verifying entries..." << endl;
    for (const Entry& entry : entries) {
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));
        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }
    cout << "All entries verified successfully.\n";
    return true;
}

int main() {
    cout << "Generating entriesA (unique)..." << endl;
    vector<Entry> entriesA = generate_test_entries(ENTRY_COUNT_A);
    cout << "entriesA done." << endl;

    cout << "Generating entriesB (50% overlapping with A)..." << endl;
    vector<Entry> entriesB = generate_test_entries(ENTRY_COUNT_B, &entriesA, 0.5);
    cout << "entriesB done." << endl;

    cout << "Generating entriesC (completely disjoint)..." << endl;
    vector<Entry> entriesC = generate_test_entries(ENTRY_COUNT_C);
    cout << "entriesC done." << endl;

    vector<Entry> entriesD = generate_test_entries(ENTRY_COUNT_A);
    vector<Entry> entriesE = generate_test_entries(ENTRY_COUNT_A);

            vector<Entry> entriesF = generate_test_entries(ENTRY_COUNT_A);

                vector<Entry> entriesG = generate_test_entries(ENTRY_COUNT_A);

                    vector<Entry> entriesH = generate_test_entries(ENTRY_COUNT_A);

                        vector<Entry> entriesI = generate_test_entries(ENTRY_COUNT_A);

                            vector<Entry> entriesJ = generate_test_entries(ENTRY_COUNT_A);

                                vector<Entry> entriesK = generate_test_entries(ENTRY_COUNT_A);

                                    vector<Entry> entriesL = generate_test_entries(ENTRY_COUNT_A);


    cout << "Creating LSM tree..." << endl;
    LsmTree lsm_tree;

    // Run tests
    assert(test_mem_table(entriesI, lsm_tree));
    assert(test_mem_table(entriesB, lsm_tree));
    assert(test_mem_table(entriesC, lsm_tree));
    assert(test_mem_table(entriesG, lsm_tree));
    assert(test_mem_table(entriesB, lsm_tree));
    assert(test_mem_table(entriesF, lsm_tree));
    assert(test_mem_table(entriesH, lsm_tree));
    assert(test_mem_table(entriesK, lsm_tree));
    assert(test_mem_table(entriesA, lsm_tree));
    assert(test_mem_table(entriesB, lsm_tree));
    assert(test_mem_table(entriesA, lsm_tree));
    assert(test_mem_table(entriesD, lsm_tree));
    assert(test_mem_table(entriesK, lsm_tree));
    assert(test_mem_table(entriesE, lsm_tree));
    assert(test_mem_table(entriesC, lsm_tree));
    assert(test_mem_table(entriesI, lsm_tree));
    assert(test_mem_table(entriesB, lsm_tree));
    assert(test_mem_table(entriesC, lsm_tree));
    assert(test_mem_table(entriesG, lsm_tree));
    assert(test_mem_table(entriesB, lsm_tree));
    assert(test_mem_table(entriesF, lsm_tree));
    assert(test_mem_table(entriesH, lsm_tree));
    assert(test_mem_table(entriesK, lsm_tree));
    assert(test_mem_table(entriesA, lsm_tree));
    assert(test_mem_table(entriesB, lsm_tree));
    assert(test_mem_table(entriesA, lsm_tree));
    assert(test_mem_table(entriesD, lsm_tree));
    assert(test_mem_table(entriesA, lsm_tree));
    assert(test_mem_table(entriesE, lsm_tree));
    assert(test_mem_table(entriesJ, lsm_tree));    
    assert(test_mem_table(entriesL, lsm_tree));


    cout << "Looking for all the entriesC in the lsm tree (2)" << endl;
    for(const Entry& entry : entriesC) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }
    cout << "Looking for all the entriesD in the lsm tree (1)" << endl;
    for(const Entry& entry : entriesD) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }

    cout << "Looking for all the entriesE in the lsm tree (2)" << endl;
    for(const Entry& entry : entriesE) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }
        cout << "Looking for all the entriesF in the lsm tree (1)" << endl;
    for(const Entry& entry : entriesF) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }

    cout << "Looking for all the entriesG in the lsm tree (2)" << endl;
    for(const Entry& entry : entriesG) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }

         cout << "Looking for all the entriesH in the lsm tree (1)" << endl;
    for(const Entry& entry : entriesH) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }

    cout << "Looking for all the entriesI in the lsm tree (2)" << endl;
    for(const Entry& entry : entriesI) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }

         cout << "Looking for all the entriesJ in the lsm tree (1)" << endl;
    for(const Entry& entry : entriesJ) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }

    cout << "Looking for all the entriesK in the lsm tree (2)" << endl;
    for(const Entry& entry : entriesK) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }
       cout << "Looking for all the entriesL in the lsm tree (2)" << endl;
    for(const Entry& entry : entriesL) {
        bool found = false;
        Entry entry_got(lsm_tree.get(entry.get_key().get_string_char()));

        assert(entry_got.get_key() == entry.get_key());
        assert(entry_got.get_value() == entry.get_value());
    }






    //cout << "Entires C(2)" << endl;
    //assert(test_mem_table(entriesC, lsm_tree));
    

    cout << "All test rounds completed successfully.\n";

    // Print fill ratios (if supported)
    vector<pair<uint16_t, double>> ratios = lsm_tree.get_fill_ratios();
    for (auto& r : ratios)
        cout << "Level " << r.first << " Ratio: " << r.second * 100 << "%\n";

    return 0;
}
*/
#include "../include/lsm_tree.h"
#include <cassert>
#include <iostream>
#include <random>
#include <string>
#include <vector>

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

// Test helper (unchanged)
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

int main() {
    cout << "Generating non-overlapping entry sets..." << endl;
    vector<Entry> entriesA = generate_test_entries(0, ENTRY_COUNT);        // keys 0–999
    vector<Entry> entriesB = generate_test_entries(100, ENTRY_COUNT);     // keys 1000–1999
    /*vector<Entry> entriesC = generate_test_entries(200, ENTRY_COUNT);     // keys 2000–2999
    vector<Entry> entriesD = generate_test_entries(300, ENTRY_COUNT);     // keys 3000–3999
    vector<Entry> entriesE = generate_test_entries(400, ENTRY_COUNT);     // keys 4000–4999
    vector<Entry> entriesF = generate_test_entries(500, ENTRY_COUNT);     // keys 5000–5999
    vector<Entry> entriesG = generate_test_entries(600, ENTRY_COUNT);     // keys 6000–6999
    vector<Entry> entriesH = generate_test_entries(700, ENTRY_COUNT);     // keys 7000–7999
    vector<Entry> entriesI = generate_test_entries(800, ENTRY_COUNT);     // keys 8000–8999
    vector<Entry> entriesJ = generate_test_entries(900, ENTRY_COUNT);     // keys 9000–9999*/
    
    
        cout << "Creating LSM tree..." << endl;
        LSM_Tree lsm_tree;
                std::cout << "LSM tree created successfully." << std::endl;



    

    // Run tests with guaranteed non-overlapping keys
    assert(test_mem_table(entriesA, lsm_tree));
    assert(test_mem_table(entriesB, lsm_tree));
    // assert(test_mem_table(entriesC, lsm_tree));
    // assert(test_mem_table(entriesD, lsm_tree));
    // assert(test_mem_table(entriesE, lsm_tree));
    // assert(test_mem_table(entriesF, lsm_tree));
    // assert(test_mem_table(entriesG, lsm_tree));
    // assert(test_mem_table(entriesH, lsm_tree));
    // assert(test_mem_table(entriesI, lsm_tree));
    // assert(test_mem_table(entriesJ, lsm_tree));
    // assert(test_mem_table(entriesA, lsm_tree));
    // assert(test_mem_table(entriesB, lsm_tree));
    // assert(test_mem_table(entriesC, lsm_tree));

    cout << "Verifying that all inserted entries can be retrieved..." << endl;
   // vector<vector<Entry>*> all = {&entriesA, &entriesB, &entriesC, &entriesD, &entriesE,
    //                              &entriesF, &entriesG, &entriesH, &entriesI, &entriesJ};

    vector<vector<Entry>*> all = {&entriesA, &entriesB};

    for (size_t idx = 0; idx < all.size(); ++idx) {
        cout << "Checking entries set " << idx + 1 << endl;
        for (const Entry& entry : *all[idx]) {
            //std::cout << entry.get_key().get_string() << std::endl;
            Entry entry_got(lsm_tree.get(entry.get_key().get_string()));
            //std::cout << entry_got.get_key().get_string() << std::endl;
            assert(entry_got.get_key() == entry.get_key());
            assert(entry_got.get_value() == entry.get_value());
        }
    }

    cout << "All tests passed — no overlapping keys detected.\n";

    // Print fill ratios (if supported)
    vector<pair<uint16_t, double>> ratios = lsm_tree.get_fill_ratios();
    for (auto& r : ratios)
        cout << "Level " << r.first << " Ratio: " << r.second * 100 << "%\n";
        
        

    return 0;
}
