BITS_FILE := -DBITS_TEST_MAIN
CXX := g++
CXXFLAGS := -W

BITS_SRC := ../src/bits.cpp
BITS_TEST := bits_test_main.cpp
BITS_OBJS := objs/bits.o objs/bits_test_main.o
BITS_TARGET := bin/test_bits

bits: $(BITS_TARGET)

$(BITS_TARGET): $(BITS_OBJS) | bin
	$(CXX) $(CXXFLAGS) $(BITS_FILE) -o $@ $(BITS_OBJS)

objs/%.o: %.cpp | objs
	$(CXX) $(CXXFLAGS) $(BITS_FILE) -c $< -o $@

objs/%.o: ../src/%.cpp | objs
	$(CXX) $(CXXFLAGS) $(BITS_FILE) -c $< -o $@

bin:
	mkdir -p bin

objs:
	mkdir -p objs

clean:
	rm -rf bin objs

