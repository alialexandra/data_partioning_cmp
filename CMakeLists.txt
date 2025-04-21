cmake_minimum_required(VERSION 3.20)
project(data_partitioning_cmp)

add_executable(independent_output independent_output.cpp)
add_executable(concurrent_output concurrent_output.cpp)
add_executable(concurrent_output_affinity concurrent_output_affinity.cpp)


# Modern way to set C++ version
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)