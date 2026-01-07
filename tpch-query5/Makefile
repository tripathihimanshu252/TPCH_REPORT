# Compiler settings
CXX = g++
CXXFLAGS = -O3 -pthread -std=c++17 -Iinclude

# Folder structure
SRC_DIR = src
TARGET = solution.exe

# Source files
SRCS = $(SRC_DIR)/main.cpp $(SRC_DIR)/query5.cpp

# Build rule
all:
	$(CXX) $(CXXFLAGS) $(SRCS) -o $(TARGET)

# Cleanup rule
clean:
	rm -f $(TARGET)