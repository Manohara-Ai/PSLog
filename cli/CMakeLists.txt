cmake_minimum_required(VERSION 3.14)
project(pslog_cli VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

add_executable(pslog
    src/main.cpp
)

find_package(Threads REQUIRED)
target_link_libraries(pslog PRIVATE Threads::Threads)