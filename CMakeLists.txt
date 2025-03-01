cmake_minimum_required(VERSION 3.0 FATAL_ERROR)
project (project1)

SET (CMAKE_CXX_FLAGS "-std=c++20")
add_executable(independent_output independent_output.cpp)
add_executable(concurrent_output concurrent_output.cpp)