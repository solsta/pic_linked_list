cmake_minimum_required(VERSION 3.21)
project(pic_linked_list C)

set(CMAKE_C_STANDARD 17)
link_libraries(pmem pmemobj)

add_executable(pic_linked_list main.c)
