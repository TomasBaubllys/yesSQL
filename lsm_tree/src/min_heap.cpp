#include "../include/min_heap.h"

// true if the first argument has lower priority
bool Min_Heap::Compare_heap_element::operator()(Heap_Element& a, Heap_Element&b){
    if(a.key != b.key){
        return a.key > b.key;
    }
    else if(a.level != b.level){
        return a.level > b.level;   
    } else{
        return a.file_index < b.file_index;
    }
}

// make const later
Min_Heap::Heap_Element::Heap_Element(Bits k, level_index_type l, table_index_type f_i, SS_Table::Keynator* kntr)
        : key(k), level(l), file_index(f_i), keynator(kntr){}


uint16_t Min_Heap:: size(){
    return this -> min_heap.size();
}

bool Min_Heap::empty(){
    return this -> min_heap.empty();
}

const Min_Heap::Heap_Element& Min_Heap:: top(){
    return this -> min_heap.top();
}


void Min_Heap::push(Bits key, level_index_type level, table_index_type file_index, SS_Table::Keynator* keynator){
    this -> min_heap.emplace(key, level, file_index, keynator);
    return;
}

void Min_Heap::pop(){
    if(!(this -> min_heap.empty())){
        min_heap.pop();
    }

    return;
}
// for when we need to pop all of the elements with the same key
void Min_Heap::remove_by_key(Bits& key){
    if(this -> min_heap.empty()){
        return;
    }
    while (true) {
        if(this -> min_heap.empty()) {
            break;
        } 

        Heap_Element element = this -> min_heap.top();
        if(element.key == key){
            this -> min_heap.pop();
            Bits next_key = element.keynator->get_next_key();
            if(next_key != Bits(ENTRY_PLACEHOLDER_KEY)){
                this -> push(next_key, element.level, element.file_index, element.keynator);
            }
            
        }
        else{
            break;
        }    
    }

    return;
    
}
