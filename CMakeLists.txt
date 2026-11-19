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

add_library(quorum_sim
  src/sim/network.cpp
  src/sim/rng.cpp
)
target_include_directories(quorum_sim PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(quorum_sim PRIVATE quorum_warnings)

add_library(quorum_raft
  src/raft/messages.cpp
  src/raft/cluster.cpp
  src/raft/invariants.cpp
  src/raft/node.cpp
)
target_include_directories(quorum_raft PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(quorum_raft PUBLIC quorum_sim PRIVATE quorum_warnings)

add_library(quorum_kv
  src/kv/command.cpp
  src/kv/client.cpp
  src/kv/cluster.cpp
  src/kv/linearizability.cpp
  src/kv/service.cpp
  src/kv/state_machine.cpp
)
target_include_directories(quorum_kv PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(quorum_kv PUBLIC quorum_raft PRIVATE quorum_warnings)

enable_testing()
add_subdirectory(tests)
