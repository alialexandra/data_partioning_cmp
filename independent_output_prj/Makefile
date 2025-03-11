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

# Compile object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Run experiments
run: $(TARGET)
	./$(TARGET) 1
	./$(TARGET) 2
	./$(TARGET) 4
	./$(TARGET) 8
	./$(TARGET) 16
	./$(TARGET) 32

# Profiling with perf
perf: $(TARGET)
	sudo perf stat -e cache-misses,cache-references,cycles,instructions,branches,branch-misses ./$(TARGET)

# Debugging with gdb
debug: $(TARGET)
	gdb ./$(TARGET)

# Clean up
clean:
	rm -f $(TARGET) $(OBJS) gmon.out perf.data

# Clean and rebuild
rebuild: clean all
