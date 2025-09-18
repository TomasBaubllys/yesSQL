TEST_TO_RUN := BITS_TEST_MAIN
CXX := g++
CXXFLAGS := -W -D$(TEST_TO_RUN)

SRC := ../src/bits.cpp
TEST := bits_test_main.cpp
OBJS := objs/bits.o objs/bits_test_main.o
TARGET := bin/test_bits

bits: $(TARGET)

$(TARGET): $(OBJS) | bin
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS)

objs/%.o: %.cpp | objs
	$(CXX) $(CXXFLAGS) -c $< -o $@

objs/%.o: ../src/%.cpp | objs
	$(CXX) $(CXXFLAGS) -c $< -o $@

bin:
	mkdir -p bin

objs:
	mkdir -p objs

clean:
	rm -rf bin objs

