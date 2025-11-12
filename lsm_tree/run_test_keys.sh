#!/bin/bash

g++ -o test_sskeys -g src/file_exception.cpp src/crc32.cpp src/entry.cpp src/bits.cpp src/avl_tree.cpp src/min_heap.cpp tests/test_ss_table_below_above.cpp src/ss_table.cpp src/ss_table_controller.cpp src/wal.cpp

rm -r ./data/val*

./test_sskeys
