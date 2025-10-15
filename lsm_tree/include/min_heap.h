#ifndef MIN_HEAP_H_INCLUDED
#define MIN_HEAP_H_INCLUDED

#include <algorithm>
#include <queue>
#include <vector>
#include "bits.h"
#include "ss_table.h"

class Min_Heap{
    public:

        struct Heap_Element{
            Bits key;
            uint16_t level;
            uint16_t file_index;
            SS_Table::Keynator* keynator;

            Heap_Element(Bits key, uint16_t level, uint16_t file_index, SS_Table::Keynator* keynator);
        };

    private:
        struct Compare_heap_element{
            bool operator()(Heap_Element& a, Heap_Element& b);
        };

        std::priority_queue<Heap_Element, std::vector<Heap_Element>, Compare_heap_element> min_heap;

    public:
        // push pop and other operations
        uint16_t size();

        bool empty();

        const Heap_Element& top();

        void push(Bits key, uint16_t level, uint16_t file_index, SS_Table::Keynator* keynator);

        void pop();

        // for when we need to pop all of the elements with the same key
        void remove_by_key(Bits& key);
};

#endif // MIN_HEAP_H_INCLUDED
