#!/bin/bash

g++ -o test_lsm -g src/lsm_tree.cpp src/crc32.cpp src/entry.cpp src/bits.cpp src/avl_tree.cpp src/mem_table.cpp src/min_heap.cpp tests/test_lsm.cpp src/ss_table.cpp src/ss_table_controller.cpp src/wal.cpp 

./test_lsm
