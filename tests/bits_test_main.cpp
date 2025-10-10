#include <iostream>
#include <cassert>
#include <fstream>
#include "../include/bits.h"

// TODO 

#define RUN_TEST(test) \
    do { \
        std::cout << "Running " << #test << "... "; \
        test(); \
        std::cout << "OK\n"; \
    } while(0)

#ifdef BITS_TEST_MAIN


#define FILE_1 "obj1.txt"

void test_construct_from_string(){
    std::vector<std::string> test_cases = {
        "",
        " ",
        "normal", 
        "2554862", 
        "#$@%#^#$#@$% @#$%"
        "bezdzionskjdfaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    };

    // jei const tada error
    for(auto& s : test_cases){
        Bits b(s);

        assert(b.size() == s.size());
        assert(b.get_string_char() == s);

    }
    
}

void test_reading_from_file_char(){
    std::ifstream file(FILE_1, std::ios::binary);
    if(!file){
        std::cerr << "Could not open file!" << std:: endl;
        return;
    }

    std::string file_contents((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    file.close();

    Bits bits(file_contents);

    std::string data_back = bits.get_string_char();

    std::ofstream file_out("OBJ_1_copy_string.txt", std::ios::binary);
    if(!file_out){
        std::cerr << "Could not open file!" << std:: endl;
        return;
    }

    file_out.write(data_back.data(), data_back.size());
    file_out.close();
}

void test_reading_from_file_u_int8(){
    std::ifstream file(FILE_1, std::ios::binary);
    if(!file){
        std::cerr << "Could not open file!" << std:: endl;
        return;
    }

    std::vector<uint8_t> file_contents((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    file.close();

    Bits bits(file_contents);

    std::string data_back = bits.get_string_char();

    std::ofstream file_out("OBJ_1_copy_uint8.txt", std::ios::binary);
    if(!file_out){
        std::cerr << "Could not open file!" << std:: endl;
        return;
    }

    file_out.write(data_back.data(), data_back.size());
    file_out.close();
}

void test_reading_from_file_u_int32(){
    std::ifstream file(FILE_1, std::ios::binary);
    if(!file){
        std::cerr << "Could not open file!" << std:: endl;
        return;
    }

    std::vector<uint32_t> file_contents;
    uint32_t val;
    while(file.read(reinterpret_cast<char*>(&val), sizeof(uint32_t))) {
        file_contents.push_back(val);
    }
    
    file.close();

    Bits bits(file_contents);

    std::string data_back = bits.get_string_char();

    std::ofstream file_out("OBJ_1_copy_uint32.txt", std::ios::binary);
    if(!file_out){
        std::cerr << "Could not open file!" << std:: endl;
        return;
    }

    file_out.write(data_back.data(), data_back.size());
    file_out.close();
}

void test_compare(){
    std::ifstream file1("compare1.txt", std::ios::binary);
    if(!file1){
        std::cerr << "Could not open file!" << std:: endl;
        return;
    }

    std::string file_contents1((std::istreambuf_iterator<char>(file1)), std::istreambuf_iterator<char>());
    file1.close();

    Bits bits1(file_contents1);

    std::ifstream file2("compare2.txt", std::ios::binary);
    if(!file2){
        std::cerr << "Could not open file!" << std:: endl;
        return;
    }

    std::string file_contents2((std::istreambuf_iterator<char>(file2)), std::istreambuf_iterator<char>());
    file2.close();

    Bits bits2(file_contents2);

    std::ifstream file3("compare3.txt", std::ios::binary);
    if(!file3){
        std::cerr << "Could not open file!" << std:: endl;
        return;
    }

    std::string file_contents3((std::istreambuf_iterator<char>(file3)), std::istreambuf_iterator<char>());
    file3.close();

    Bits bits3(file_contents3);

    assert(bits1 == bits2);
    assert(!(bits1 == bits3));

}



int main(int argc, char* argv[]) {

    RUN_TEST(test_construct_from_string);
    test_reading_from_file_char();
    test_reading_from_file_u_int8();
    test_reading_from_file_u_int32();
    RUN_TEST(test_compare);


    return 0;

}



#endif
