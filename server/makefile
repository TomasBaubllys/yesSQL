CXX = g++
CFLAGS = -Wall -O2
LDFLAGS = 

SRC_DIR = src
OBJS_DIR = objs
BIN_DIR = bin

LSM_LIB = ../lsm_tree/lib/lsm_tree.a

TARGET = $(BIN_DIR)/server

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(OBJS_DIR)/%.o, $(SRCS))

all: $(TARGET)

$(TARGET): $(OBJS) $(LSM_LIB)
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CFLAGS) $(OBJS) $(LSM_LIB) -o $(TARGET) $(LDFLAGS)

$(OBJS_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(OBJS_DIR)
	$(CXX) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(OBJS_DIR) $(BIN_DIR)

.PHONY: all clean
