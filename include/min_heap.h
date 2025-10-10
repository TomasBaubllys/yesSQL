#ifndef MIN_HEAP_H_INCLUDED
#define MIN_HEAP_H_INCLUDED

#include <algorithm>
#include <queue>
#include <vector>
#include "bits.h"


struct Heap_element{
        Bits key;
        uint16_t level;
        uint16_t file_index;

        Heap_element(Bits key, uint16_t level, uint16_t file_index);
};

struct Compare_heap_element{
    bool operator()(Heap_element& a, Heap_element& b);
};

class Min_heap{
    private:
      

        std::priority_queue<Heap_element, std::vector<Heap_element>, Compare_heap_element> min_heap;

    public:
        // push pop and other operations
        
        uint16_t size();
        bool empty();
        const Heap_element& top();
        void push(Bits key, uint16_t level, uint16_t file_index);
        void pop();
        // for when we need to pop all of the elements with the same key
        void remove_by_key(Bits& key);
};


#endif // MIN_HEAP_H_INCLUDED