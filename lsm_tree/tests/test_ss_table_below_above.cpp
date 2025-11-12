#include "../include/ss_table.h"
#include <iostream>
#include <vector>
#include <algorithm>
#include <random>
#include <iomanip>
#include <filesystem>

// ANSI color codes for terminal output
#define COLOR_GREEN "\033[32m"
#define COLOR_RED "\033[31m"
#define COLOR_RESET "\033[0m"

// ========== CONFIGURABLE TEST PARAMETERS ==========
constexpr size_t NUM_ENTRIES = 50;     // number of total entries
constexpr size_t RANDOM_STR_LEN = 10;  // value length
constexpr size_t TEST_COUNT = 5;       // number of entries to read per partial test
constexpr size_t KEY_TEST_POS_SMALLER = 15;
constexpr size_t KEY_TEST_POS_LARGER = 5;
constexpr size_t KEY_TEST_BATCH = 10;
// =================================================

// Generate random string
std::string generate_random_string(size_t length) {
    static const char chars[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static std::mt19937 gen(std::random_device{}());
    static std::uniform_int_distribution<> dis(0, sizeof(chars) - 2);
    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i)
        result += chars[dis(gen)];
    return result;
}

void print_test_result(const std::string& test_name, bool passed) {
    std::cout << "[" << (passed ? COLOR_GREEN "PASS" COLOR_RESET : COLOR_RED "FAIL" COLOR_RESET)
              << "] " << test_name << std::endl;
}

void print_entries(const std::vector<Entry>& entries, const std::string& label) {
    std::cout << label << " (" << entries.size() << "): ";
    for (size_t i = 0; i < std::min<size_t>(entries.size(), 5); ++i) {
        std::cout << "{" << entries[i].get_key().get_string()
                  << ":" << entries[i].get_value().get_string() << "} ";
    }
    if (entries.size() > 5) std::cout << "...";
    std::cout << std::endl;
}

int main() {
    std::cout << "=== SS_Table Comprehensive Test ===" << std::endl << std::endl;

    try {
        // Create temp file paths
        std::filesystem::path data_file = "test_data.bin";
        std::filesystem::path index_file = "test_index.bin";
        std::filesystem::path index_offset_file = "test_index_offset.bin";

        std::filesystem::remove(data_file);
        std::filesystem::remove(index_file);
        std::filesystem::remove(index_offset_file);

        // STEP 1: Generate and sort entries
        std::cout << "Generating and sorting " << NUM_ENTRIES << " entries..." << std::endl;
        std::vector<Entry> entries;
        entries.reserve(NUM_ENTRIES);
        for (size_t i = 0; i < NUM_ENTRIES; ++i) {
            Bits key("key_" + std::to_string(i));
            Bits value("value_" + generate_random_string(RANDOM_STR_LEN));
            entries.emplace_back(key, value);
        }
        std::sort(entries.begin(), entries.end());
        std::cout << "  Done.\n\n";

        // STEP 2: Fill SS_Table
        SS_Table ss_table(data_file, index_file, index_offset_file);
        uint64_t bytes_written = ss_table.fill_ss_table(entries);
        std::cout << "Wrote " << bytes_written << " bytes.\n\n";

        // Store keys
        std::vector<Bits> all_keys;
        for (const auto& e : entries) all_keys.push_back(e.get_key());

        // === TEST 1: get_n_entries() ===
        {
            std::cout << "TEST 1: get_n_entries()" << std::endl;
            auto [retrieved, next_key] = ss_table.get_n_entries(NUM_ENTRIES);

            bool match = retrieved.size() == entries.size();
            for (size_t i = 0; match && i < entries.size(); ++i) {
                if (retrieved[i].get_key() != entries[i].get_key() ||
                    retrieved[i].get_value() != entries[i].get_value()) {
                    match = false;
                    break;
                }
            }

            bool next_key_ok = (next_key.get_string() == ENTRY_PLACEHOLDER_KEY);
            print_test_result("All entries match expected", match);
            print_test_result("next_key = placeholder (end of table)", next_key_ok);
            std::cout << std::endl;
        }

        // === TEST 2: get_entries_key_larger_or_equal() ===
        {
            std::cout << "TEST 2: get_entries_key_larger_or_equal()" << std::endl;
            Bits test_key = all_keys[KEY_TEST_POS_LARGER];
            std::cout << "  Test key: " << test_key.get_string() << std::endl;

            auto [got, next_key] = ss_table.get_entries_key_larger_or_equal(test_key, TEST_COUNT);

            std::vector<Entry> expected;
            for (size_t i = KEY_TEST_POS_LARGER;
                 i < std::min(KEY_TEST_POS_LARGER + TEST_COUNT, NUM_ENTRIES); ++i)
                expected.push_back(entries[i]);

            bool ok = (got.size() == expected.size());
            for (size_t i = 0; ok && i < expected.size(); ++i)
                if (got[i].get_key() != expected[i].get_key()) ok = false;

            bool next_key_ok = true;
            size_t expected_next_index = KEY_TEST_POS_LARGER + TEST_COUNT;
            if (expected_next_index < NUM_ENTRIES)
                next_key_ok = (next_key.get_string() == all_keys[expected_next_index].get_string());
            else
                next_key_ok = (next_key.get_string() == ENTRY_PLACEHOLDER_KEY);

            print_entries(got, "  Result");
            print_test_result("Returned entries match expected", ok);
            print_test_result("next_key correct", next_key_ok);
            std::cout << std::endl;
        }

        // === TEST 3: get_entries_key_smaller_or_equal() ===
        {
            std::cout << "TEST 3: get_entries_key_smaller_or_equal()" << std::endl;
            Bits test_key = all_keys[KEY_TEST_POS_SMALLER];
            std::cout << "  Test key: " << test_key.get_string() << std::endl;

            auto [got, next_key] = ss_table.get_entries_key_smaller_or_equal(test_key, TEST_COUNT);

            // Expect entries in descending order
// Expect entries in ascending order (same as SS_Table output)
            size_t start_idx = (KEY_TEST_POS_SMALLER >= TEST_COUNT - 1)
                   ? KEY_TEST_POS_SMALLER - (TEST_COUNT - 1)
                   : 0;
            size_t end_idx = KEY_TEST_POS_SMALLER + 1;

            std::vector<Entry> expected(entries.begin() + start_idx, entries.begin() + end_idx);

            bool ok = (got.size() == expected.size());
            for (size_t i = 0; ok && i < expected.size(); ++i)
                if (got[i].get_key() != expected[i].get_key()) ok = false;

            bool next_key_ok;
            if (start_idx > 0)
                next_key_ok = (next_key.get_string() == entries[start_idx - 1].get_key().get_string());
            else
                next_key_ok = (next_key.get_string() == ENTRY_PLACEHOLDER_KEY);
            
            //    std::cout << "NEXT KEY - " << next_key.get_string() << std::endl;
            print_entries(got, "  Result");
            print_test_result("Returned ascending entries match expected", ok);
            print_test_result("next_key correct", next_key_ok);
            std::cout << std::endl;
        }

        // === TEST 4: key before all keys ===
        {
            std::cout << "TEST 4: Key before all keys" << std::endl;
            Bits before_key("key_");

            auto [larger, next_key_large] =
                ss_table.get_entries_key_larger_or_equal(before_key, KEY_TEST_BATCH);
            auto [smaller, next_key_small] =
                ss_table.get_entries_key_smaller_or_equal(before_key, KEY_TEST_BATCH);

                std::cout << smaller.size() << std::endl;

            bool larger_ok = (larger.size() == std::min(KEY_TEST_BATCH, NUM_ENTRIES));
            bool smaller_ok = smaller.empty();
            bool next_key_ok = (next_key_small.get_string() == ENTRY_PLACEHOLDER_KEY);

            print_test_result("Larger returns first N entries", larger_ok);
            print_test_result("Smaller returns none", smaller_ok);
            print_test_result("next_key (smaller) placeholder", next_key_ok);
            std::cout << std::endl;
        }

        // === TEST 5: key after all keys ===
        {
            std::cout << "TEST 5: Key after all keys" << std::endl;
            Bits after_key("key_999999");

            auto [larger, next_key_large] =
                ss_table.get_entries_key_larger_or_equal(after_key, KEY_TEST_BATCH);
            auto [smaller, next_key_small] =
                ss_table.get_entries_key_smaller_or_equal(after_key, KEY_TEST_BATCH);

            bool larger_ok = larger.empty();
            bool smaller_ok = (smaller.size() == std::min(KEY_TEST_BATCH, NUM_ENTRIES));
            bool next_key_ok = (next_key_large.get_string() == ENTRY_PLACEHOLDER_KEY);

            print_test_result("Larger returns none", larger_ok);
            print_test_result("Smaller returns last N entries", smaller_ok);
            print_test_result("next_key (larger) placeholder", next_key_ok);
            std::cout << std::endl;
        }

        // === TEST 6: Exact first key ===
        {
            std::cout << "TEST 6: Exact first key" << std::endl;
            Bits first_key = all_keys.front();
            auto [got, next_key] =
                ss_table.get_entries_key_smaller_or_equal(first_key, KEY_TEST_BATCH);
            bool ok = (got.size() == 1 && got[0].get_key() == first_key);
            bool next_key_ok = (next_key.get_string() == ENTRY_PLACEHOLDER_KEY);
            print_test_result("Returns only first entry", ok);
            print_test_result("next_key placeholder", next_key_ok);
            std::cout << std::endl;
        }

        // === TEST 7: Exact last key ===
        {
            std::cout << "TEST 7: Exact last key" << std::endl;
            Bits last_key = all_keys.back();
            auto [got, next_key] =
                ss_table.get_entries_key_larger_or_equal(last_key, KEY_TEST_BATCH);
            bool ok = (got.size() == 1 && got[0].get_key() == last_key);
            bool next_key_ok = (next_key.get_string() == ENTRY_PLACEHOLDER_KEY);
            print_test_result("Returns only last entry", ok);
            print_test_result("next_key placeholder", next_key_ok);
            std::cout << std::endl;
        }

        std::cout << "=== All Tests Completed ===" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "[" << COLOR_RED << "ERROR" << COLOR_RESET << "] Exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
