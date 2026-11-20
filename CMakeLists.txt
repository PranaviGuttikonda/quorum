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

# Given: the engine uses exceptions (DiskCrash, input validation), and
#        Emscripten disables catching them by default.
# Approach: native WebAssembly exceptions for every target in the web build.
if(EMSCRIPTEN)
  add_compile_options(-fwasm-exceptions)
  add_link_options(-fwasm-exceptions)
endif()

# Given: the file layer sits below Raft, and SimDisk only needs its header.
# Approach: quorum_storage has no quorum dependencies.
add_library(quorum_storage
  src/storage/crc32.cpp
  src/storage/posix_fs.cpp
)
target_include_directories(quorum_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(quorum_storage PRIVATE quorum_warnings)

add_library(quorum_sim
  src/sim/disk.cpp
  src/sim/network.cpp
  src/sim/rng.cpp
)
target_include_directories(quorum_sim PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(quorum_sim PRIVATE quorum_warnings)

add_library(quorum_raft
  src/raft/messages.cpp
  src/raft/cluster.cpp
  src/raft/file_storage.cpp
  src/raft/invariants.cpp
  src/raft/node.cpp
)
target_include_directories(quorum_raft PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(quorum_raft PUBLIC quorum_sim quorum_storage PRIVATE quorum_warnings)

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

# Given: the browser visualizer hosts the engine, and its host must be
#        testable natively as well as compiled to WebAssembly.
# Approach: quorum_websim is plain C++. The embind bindings and the page are
#           only built under Emscripten (web/CMakeLists.txt).
add_library(quorum_websim src/web/simulation.cpp)
target_include_directories(quorum_websim PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(quorum_websim PUBLIC quorum_kv PRIVATE quorum_warnings)

add_executable(quorum_scenario tools/scenario.cpp)
target_link_libraries(quorum_scenario PRIVATE quorum_websim quorum_warnings)

if(EMSCRIPTEN)
  add_subdirectory(web)
else()
  enable_testing()
  add_subdirectory(tests)
endif()
