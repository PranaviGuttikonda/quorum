// Given: every RPC crosses the network as a string.
// Approach: round-trip each message type, including extreme values, and check
//           that malformed payloads are rejected rather than half-parsed.

#include "quorum/raft/messages.h"

#include <cstdint>
#include <limits>
#include <string>

#include "harness.h"
#include "quorum/raft/storage.h"

using namespace quorum::raft;

namespace {

bool round_trips(const RaftMessage& m) {
    const auto d = decode(encode(m));
    return d.has_value() && *d == m;
}

}  // namespace

TEST(raft_messages_round_trip) {
    const std::uint64_t big = std::numeric_limits<std::uint64_t>::max();
    const NodeId big_node = std::numeric_limits<NodeId>::max();
    CHECK(round_trips(RequestVote{3, 1, 0, 0}));
    CHECK(round_trips(RequestVote{big, big_node, big, big}));
    CHECK(round_trips(RequestVoteReply{7, true}));
    CHECK(round_trips(RequestVoteReply{0, false}));
    CHECK(round_trips(AppendEntries{4, 2, 0, 0, 0}));
    CHECK(round_trips(AppendEntries{big, big_node, big, big, big}));
    CHECK(round_trips(AppendEntriesReply{9, true}));
    CHECK(round_trips(AppendEntriesReply{9, false}));
}

TEST(raft_messages_encoding_is_readable) {
    CHECK_EQ(encode(RequestVote{3, 1, 0, 0}), std::string("RV 3 1 0 0"));
    CHECK_EQ(encode(RequestVoteReply{3, true}), std::string("RVR 3 1"));
    CHECK_EQ(encode(AppendEntries{4, 2, 0, 0, 0}), std::string("AE 4 2 0 0 0"));
    CHECK_EQ(encode(AppendEntriesReply{4, false}), std::string("AER 4 0"));
}

TEST(raft_messages_reject_malformed) {
    for (const char* bad : {
             "", " ", "RV", "RV 1 2 3", "RV 1 2 3 4 5", "RV 1 2 3 x", "RV  1 2 3 4",
             " RV 1 2 3 4", "RV 1 2 3 4 ", "RV -1 2 3 4", "RV +1 2 3 4", "RV 0x1 2 3 4",
             "RV 1 4294967296 3 4",           // node id overflows NodeId
             "RV 18446744073709551616 1 0 0",  // term overflows uint64
             "RVR 1 2", "RVR 1", "AER 1 yes", "AE 1 2 3 4", "XX 1 2", "rv 1 2 3 4",
         }) {
        if (decode(bad).has_value())
            quorum::test::fail(__FILE__, __LINE__, std::string("accepted: \"") + bad + "\"");
    }
}

TEST(raft_term_of_reads_every_variant) {
    CHECK_EQ(term_of(RequestVote{5, 1, 0, 0}), Term{5});
    CHECK_EQ(term_of(RequestVoteReply{6, false}), Term{6});
    CHECK_EQ(term_of(AppendEntries{7, 1, 0, 0, 0}), Term{7});
    CHECK_EQ(term_of(AppendEntriesReply{8, true}), Term{8});
}

TEST(raft_memory_storage_round_trips) {
    MemoryStorage s;
    CHECK(s.load() == PersistentState{});
    s.save(PersistentState{4, NodeId{2}});
    CHECK(s.load() == (PersistentState{4, NodeId{2}}));
    CHECK_EQ(s.saves(), std::uint64_t{1});
}
