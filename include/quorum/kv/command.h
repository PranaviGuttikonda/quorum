#pragma once

// Given: KV operations travel twice: as Raft log entries, and between clients
//        and servers.
// Approach: plain structs plus strict codecs (quorum/codec/text.h). Keys and
//           values are arbitrary bytes.
//
//   command: PUT <client> <seq> <key> <value>
//            GET <client> <seq> <key>
//            DEL <client> <seq> <key>
//            CAS <client> <seq> <key> <expected?> <new>
//   request: KREQ <command>
//   reply:   KRESP <client> <seq> OK <ok> <value?>
//            KRESP <client> <seq> NL <hint?>
// (<x?> is 0, or 1 followed by x.)

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "quorum/raft/types.h"

namespace quorum::kv {

using raft::NodeId;
using ClientId = std::uint64_t;
using Seq = std::uint64_t;  // starts at 1; 0 is invalid

enum class Op : std::uint8_t { Put, Get, Delete, Cas };

const char* to_string(Op op);

struct Command {
    ClientId client = 0;
    Seq seq = 0;
    Op op = Op::Get;
    std::string key;
    std::string value;                    // Put: the value. Cas: the new value.
    std::optional<std::string> expected;  // Cas only. nullopt: the key must be absent.
    bool operator==(const Command&) const = default;

    static Command put(ClientId c, Seq s, std::string k, std::string v);
    static Command get(ClientId c, Seq s, std::string k);
    static Command del(ClientId c, Seq s, std::string k);
    static Command cas(ClientId c, Seq s, std::string k, std::optional<std::string> expected,
                       std::string v);
    bool is_write() const { return op != Op::Get; }
};

// Put: {true, nullopt}. Get: {true, value or nullopt}. Delete: {existed,
// nullopt}. Cas: {swapped, the value before the operation}.
struct Result {
    bool ok = false;
    std::optional<std::string> value;
    bool operator==(const Result&) const = default;
};

struct Redirect {
    std::optional<NodeId> leader_hint;
    bool operator==(const Redirect&) const = default;
};

struct Reply {
    ClientId client = 0;
    Seq seq = 0;
    std::variant<Result, Redirect> body;
    bool operator==(const Reply&) const = default;
};

std::string encode(const Command& c);
std::optional<Command> decode_command(std::string_view s);

std::string encode_request(const Command& c);
std::optional<Command> decode_request(std::string_view s);

std::string encode(const Reply& r);
std::optional<Reply> decode_reply(std::string_view s);

}  // namespace quorum::kv
