#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

if make clean; then
	echo -e "${GREEN} make clean successful ${NC}"
else
	echo -e "${RED} make clean failed ${NC}"
	exit 1
fi

if make bits; then
	echo -e "${GREEN} make successful ${NC}"
else
	echo -e "${RED} make failed ${NC}"
	exit 1
fi

clear

./bin/test_bits
make clean
