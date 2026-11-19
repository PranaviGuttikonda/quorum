add_executable(quorum_tests
  main.cpp
  harness_test.cpp
  sim/rng_test.cpp
  sim/clock_test.cpp
  sim/network_test.cpp
  raft/messages_test.cpp
  raft/node_test.cpp
  raft/replication_node_test.cpp
  raft/log_invariants_test.cpp
  raft/service_test.cpp
  raft/replication_test.cpp
  raft/replication_fuzz_test.cpp
  raft/election_test.cpp
  raft/election_fuzz_test.cpp
  kv/state_machine_test.cpp
  kv/kv_test.cpp
  kv/kv_fuzz_test.cpp
)
target_include_directories(quorum_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(quorum_tests PRIVATE quorum_kv quorum_warnings)

add_test(NAME quorum_tests COMMAND quorum_tests)
