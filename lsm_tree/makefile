CXX = g++
CFLAGS = -Wall -O2
LDFLAGS = -g

SRC_DIR = src
OBJS_DIR = objs
BIN_DIR = bin

TARGET = $(BIN_DIR)/main

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(OBJS_DIR)/%.o, $(SRCS))

all: $(TARGET)

$(TARGET): $(OBJS)
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CFLAGS) $(OBJS) -o $(TARGET) $(LDFLAGS)

$(OBJS_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(OBJS_DIR)	
	$(CXX) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(OBJS_DIR) $(BIN_DIR)

<<<<<<< HEAD

=======
>>>>>>> merge_new
