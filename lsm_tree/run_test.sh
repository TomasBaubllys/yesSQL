#!/bin/bash

g++ -o test_avl -Wall src/avl_tree.cpp src/entry.cpp src/bits.cpp src/crc32.cpp tests/test_avl.cpp
