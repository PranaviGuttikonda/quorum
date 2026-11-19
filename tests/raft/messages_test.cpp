// Given: every RPC crosses the network as a string, and commands are arbitrary
//        bytes.
// Approach: round-trip every message type, including extreme values and
//           commands containing separators and binary bytes. Check that
//           malformed or truncated payloads are rejected rather than
//           half-parsed.

#include "quorum/raft/messages.h"

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "harness.h"
#include "quorum/raft/storage.h"

using namespace quorum::raft;

namespace {

bool round_trips(const RaftMessage& m) {
    const auto d = decode(encode(m));
    return d.has_value() && *d == m;
}

void check_rejected(const std::string& bad, int line) {
    if (decode(bad).has_value()) quorum::test::fail(__FILE__, line, "accepted: \"" + bad + "\"");
}

}  // namespace

TEST(raft_messages_round_trip) {
    const std::uint64_t big = std::numeric_limits<std::uint64_t>::max();
    const NodeId big_node = std::numeric_limits<NodeId>::max();
    CHECK(round_trips(RequestVote{3, 1, 0, 0}));
    CHECK(round_trips(RequestVote{big, big_node, big, big}));
    CHECK(round_trips(RequestVoteReply{7, true}));
    CHECK(round_trips(RequestVoteReply{0, false}));
    CHECK(round_trips(AppendEntries{4, 2, 0, 0, 0, {}}));
    CHECK(round_trips(AppendEntries{big, big_node, big, big, big, {}}));
    CHECK(round_trips(AppendEntriesReply{9, true, 5, 7}));
    CHECK(round_trips(AppendEntriesReply{big, false, big, big}));
}

TEST(raft_messages_entries_round_trip_any_bytes) {
    const std::string binary("a\0b\nc\x01\xff", 7);
    const AppendEntries ae{5,
                           2,
                           10,
                           4,
                           9,
                           {
                               {4, "set x 1"},
                               {5, ""},  // empty command
                               LogEntry::noop(5),
                               {5, " leading and trailing "},
                               {5, "  "},
                               {5, binary},
                               {5, "123 4 5"},  // looks like more fields
                               {std::numeric_limits<Term>::max(), std::string(1000, 'z')},
                           }};
    CHECK(round_trips(ae));

    AppendEntries many{1, 1, 0, 0, 0, {}};
    for (int i = 0; i < 500; ++i) many.entries.push_back({1, "c" + std::to_string(i)});
    CHECK(round_trips(many));
}

TEST(raft_messages_encoding_is_readable) {
    CHECK_EQ(encode(RequestVote{3, 1, 0, 0}), std::string("RV 3 1 0 0"));
    CHECK_EQ(encode(RequestVoteReply{3, true}), std::string("RVR 3 1"));
    CHECK_EQ(encode(AppendEntries{4, 2, 0, 0, 0, {}}), std::string("AE 4 2 0 0 0 0"));
    CHECK_EQ(encode(AppendEntries{4, 2, 1, 3, 1, {{4, "x=1"}, {4, "a b"}}}),
             std::string("AE 4 2 1 3 1 2 4 0 3 x=1 4 0 3 a b"));
    CHECK_EQ(encode(AppendEntries{4, 2, 0, 0, 0, {LogEntry::noop(4)}}),
             std::string("AE 4 2 0 0 0 1 4 1 0 "));
    CHECK_EQ(encode(AppendEntriesReply{4, false, 0, 6}), std::string("AER 4 0 0 6"));
}

TEST(raft_messages_reject_malformed) {
    for (const char* bad : {
             "", " ", "RV", "RV 1 2 3", "RV 1 2 3 4 5", "RV 1 2 3 x", "RV  1 2 3 4",
             " RV 1 2 3 4", "RV 1 2 3 4 ", "RV -1 2 3 4", "RV +1 2 3 4", "RV 0x1 2 3 4",
             "RV 1 4294967296 3 4",           // node id overflows NodeId
             "RV 18446744073709551616 1 0 0",  // term overflows uint64
             "RVR 1 2", "RVR 1", "RVR 1 1 ", "XX 1 2", "rv 1 2 3 4",
             "AER 1 1 0", "AER 1 yes 0 0", "AER 1 1 0 0 0",
             "AE 1 2 3 4", "AE 1 2 3 4 5",      // missing entry count
             "AE 1 2 0 0 0 1",                    // count 1, no entry
             "AE 1 2 0 0 0 1 1",                  // entry missing kind
             "AE 1 2 0 0 0 1 1 0",                // entry missing length
             "AE 1 2 0 0 0 1 1 0 3",              // missing bytes
             "AE 1 2 0 0 0 1 1 0 3 ab",           // too few bytes
             "AE 1 2 0 0 0 1 1 0 3 abcd",         // trailing byte
             "AE 1 2 0 0 0 1 1 0 3 abc ",         // trailing space
             "AE 1 2 0 0 0 1 1 2 3 abc",          // unknown entry kind
             "AE 1 2 0 0 0 2 1 0 1 a",            // count larger than entries
             "AE 1 2 0 0 0 0 1 0 1 a",            // entries beyond count
             "AE 1 2 0 0 0 1 1 0 01a",            // length token not separated
             "AE 1 2 0 0 0 99999999999 1 0 1 a",  // absurd count
         })
        check_rejected(bad, __LINE__);
}

TEST(raft_messages_every_truncation_is_rejected) {
    const std::string full =
        encode(AppendEntries{4, 2, 1, 3, 1, {{4, "x = 1"}, LogEntry::noop(4), {4, "tail"}}});
    CHECK(decode(full).has_value());
    for (std::size_t len = 0; len < full.size(); ++len) check_rejected(full.substr(0, len), __LINE__);
}

TEST(raft_term_of_reads_every_variant) {
    CHECK_EQ(term_of(RequestVote{5, 1, 0, 0}), Term{5});
    CHECK_EQ(term_of(RequestVoteReply{6, false}), Term{6});
    CHECK_EQ(term_of(AppendEntries{7, 1, 0, 0, 0, {}}), Term{7});
    CHECK_EQ(term_of(AppendEntriesReply{8, true, 0, 0}), Term{8});
}

TEST(raft_memory_storage_round_trips) {
    MemoryStorage s;
    CHECK(s.load() == PersistentState{});
    s.save_term_and_vote(4, NodeId{2});
    const std::vector<LogEntry> a{{1, "a"}, {1, "b"}, {2, "c"}};
    s.write_log(0, a);
    CHECK(s.load() == (PersistentState{4, NodeId{2}, a}));
    CHECK_EQ(s.hard_state_writes(), std::uint64_t{1});
    CHECK_EQ(s.log_writes(), std::uint64_t{1});
}

TEST(raft_memory_storage_truncates_and_appends) {
    MemoryStorage s;
    const std::vector<LogEntry> a{{1, "a"}, {1, "b"}, {2, "c"}};
    s.write_log(0, a);
    const std::vector<LogEntry> b{{3, "x"}};
    s.write_log(1, b);  // keep "a", replace "b" and "c"
    CHECK((s.load().log == std::vector<LogEntry>{{1, "a"}, {3, "x"}}));
    CHECK_EQ(s.truncated_entries(), std::uint64_t{2});
    s.write_log(2, {});  // keep everything: a no-op write
    CHECK_EQ(s.load().log.size(), std::size_t{2});
    CHECK_THROWS(s.write_log(3, b));  // can't keep more than exists
}
