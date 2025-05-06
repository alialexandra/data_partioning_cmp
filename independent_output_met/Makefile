# Compiler
CXX = g++
CXXFLAGS = -std=c++17 -O2 -pthread -Wall -Wextra -Wpedantic -g -pg -fno-omit-frame-pointer

# Output binary name
TARGET = independent_output

# Source files
SRCS = independent_output.cpp

# Default target
all: $(TARGET)

# Compile the program
$(TARGET): $(SRCS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRCS)

# Clean up
clean:
	rm -f $(TARGET) $(OBJS) gmon.out

# Clean and rebuild
rebuild: clean all

# Run the script
run: $(TARGET)
	./run_and_plot.sh
