cmake_minimum_required(VERSION 3.10)
project(in_memory_mapreduce)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined")

add_executable(mapreduce main.cpp)

find_package(Boost REQUIRED COMPONENTS system)
target_link_libraries(mapreduce Boost::system)
