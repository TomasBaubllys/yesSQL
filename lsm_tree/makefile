CXX = g++
CFLAGS = -Wall -O2 -std=c++17
AR = ar
ARFLAGS = rcs

SRC_DIR = src
OBJS_DIR = objs
LIB_DIR = lib

TARGET = $(LIB_DIR)/lsm_tree.a

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(OBJS_DIR)/%.o, $(SRCS))

all: $(TARGET)

$(TARGET): $(OBJS)
	@mkdir -p $(LIB_DIR)
	$(AR) $(ARFLAGS) $(TARGET) $(OBJS)

$(OBJS_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(OBJS_DIR)
	$(CXX) $(CFLAGS) -c $< -o $@
	@echo "Compiled: $<"

clean:
	rm -rf $(OBJS_DIR) $(LIB_DIR)