cmake_minimum_required(VERSION 3.20)
project(quorum LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

# Given: warnings must fail the build, but only for our own code.
# Approach: an interface target that every quorum target links privately.
add_library(quorum_warnings INTERFACE)
target_compile_options(quorum_warnings INTERFACE -Wall -Wextra -Werror)

enable_testing()
add_subdirectory(tests)
