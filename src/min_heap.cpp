#include "../include/min_heap.h"
\
bool Min_heap::Compare_heap_element::operator()(Heap_element& a, Heap_element&b){
    if(a.key != b.key){
        return a.key > b.key;
    }
    else if(a.level != b.level){
        return a.level > b.level;   
    } else{
        return a.file_index < b.file_index;
    }

}


Min_heap::Heap_element::Heap_element(Bits k, uint16_t l, uint16_t f_i)
        : key(k), level(l), file_index(f_i){}

uint16_t Min_heap:: size(){
    return this -> min_heap.size();
}

bool Min_heap::empty(){
    return this -> min_heap.empty();
}

const Min_heap::Heap_element& Min_heap:: top(){
    return this -> min_heap.top();
}


void Min_heap::push(Bits key, uint16_t level, uint16_t file_index){

    this -> min_heap.emplace(key, level, file_index);
    return;
}

void Min_heap::pop(){
    if(!(this -> min_heap.empty())){
        min_heap.pop();
    }

    return;
}
// for when we need to pop all of the elements with the same key
void Min_heap::remove_by_key(Bits& key){
    if(this -> min_heap.empty()){
        return;
    }
    while (true)
    {
        Heap_element element = this -> min_heap.top();
        if(element.key == key){
            this -> min_heap.pop();
        }
        else{
            break;
        }
         
    }

    return;
    
}
