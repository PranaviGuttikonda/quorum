#include "quorum/raft/file_storage.h"

#include "quorum/codec/text.h"
#include "quorum/storage/crc32.h"

namespace quorum::raft {

namespace {

constexpr std::size_t kHeader = 12;

void put_u32(std::string& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out += static_cast<char>((v >> (8 * i)) & 0xFF);
}

std::uint32_t get_u32(std::string_view s, std::size_t at) {
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i)
        v |= static_cast<std::uint32_t>(static_cast<unsigned char>(s[at + i])) << (8 * i);
    return v;
}

std::string encode_entry(const LogEntry& e) {
    return codec::Writer("E").u64(e.term).u64(static_cast<std::uint64_t>(e.kind)).bytes(e.command).str();
}

std::optional<LogEntry> decode_entry(std::string_view s) {
    codec::Reader r(s);
    const auto tag = r.token();
    const auto term = r.u64();
    const auto kind = r.u64();
    const auto cmd = r.bytes();
    if (!tag || *tag != "E" || !term || !kind || *kind > 1 || !cmd || !r.done()) return std::nullopt;
    return LogEntry(*term, std::string(*cmd), static_cast<EntryKind>(*kind));
}

std::string encode_meta(Term term, std::optional<NodeId> vote) {
    return codec::Writer("M").u64(term).flag(vote.has_value()).u64(vote.value_or(0)).str();
}

struct Parsed {
    std::vector<std::string_view> payloads;
    std::vector<std::uint64_t> offsets;
    std::uint64_t good_end = 0;  // end of the last good record
};

// Given: a file of records whose last write may be torn.
// Approach: walk records from the start. Stop at the first bad one, and
//           decide whether it can be the torn tail (see file_storage.h).
Parsed parse_records(std::string_view data, const char* file) {
    Parsed p;
    std::size_t pos = 0;
    auto corrupt = [&](const std::string& what) {
        throw StorageCorruption(std::string(file) + ": " + what + " in the record at byte " +
                                std::to_string(pos) + " of " + std::to_string(data.size()));
    };
    while (pos < data.size()) {
        const std::size_t rest = data.size() - pos;
        if (rest < kHeader) break;  // torn header
        const std::uint32_t len = get_u32(data, pos);
        if (storage::crc32(data.substr(pos, 4)) != get_u32(data, pos + 4)) corrupt("bad length checksum");
        if (len > rest - kHeader) break;  // torn payload
        const std::string_view payload = data.substr(pos + kHeader, len);
        if (storage::crc32(payload) != get_u32(data, pos + 8)) {
            if (pos + kHeader + len == data.size()) break;  // the final record, torn in place
            corrupt("bad payload checksum");
        }
        p.offsets.push_back(pos);
        p.payloads.push_back(payload);
        pos += kHeader + len;
    }
    p.good_end = pos;
    return p;
}

}  // namespace

std::string encode_record(std::string_view payload) {
    std::string out;
    out.reserve(kHeader + payload.size());
    std::string len;
    put_u32(len, static_cast<std::uint32_t>(payload.size()));
    out += len;
    put_u32(out, storage::crc32(len));
    put_u32(out, storage::crc32(payload));
    out += payload;
    return out;
}

// Given: files left by any earlier incarnation, possibly cut off by a crash.
// Approach: drop a stale temp file, read meta strictly (it was synced before
//           it got its name, so it can't be torn), then recover the log and
//           make its truncated tail durable before the node can act on it.
FileStorage::FileStorage(storage::FileSystem& fs) : fs_(fs) {
    fs_.remove(kMetaTmp);

    if (const auto meta = fs_.read(kMeta)) {
        const Parsed p = parse_records(*meta, kMeta);
        if (p.payloads.size() != 1 || p.good_end != meta->size())
            throw StorageCorruption(std::string(kMeta) + ": expected exactly one intact record");
        codec::Reader r(p.payloads[0]);
        const auto tag = r.token();
        const auto term = r.u64();
        const auto has_vote = r.flag();
        const auto vote = r.u64();
        if (!tag || *tag != "M" || !term || !has_vote || !vote || !r.done())
            throw StorageCorruption(std::string(kMeta) + ": undecodable record");
        state_.current_term = *term;
        if (*has_vote) state_.voted_for = *vote;
    }

    const auto log = fs_.read(kLog);
    if (!log) {
        if (fs_.read(kMeta))
            throw StorageCorruption("meta exists but the log is missing");
        fs_.create(kLog);
        fs_.fsync_dir();
        recovery_.created = true;
        return;
    }
    const Parsed p = parse_records(*log, kLog);
    for (std::size_t i = 0; i < p.payloads.size(); ++i) {
        auto e = decode_entry(p.payloads[i]);
        if (!e)
            throw StorageCorruption(std::string(kLog) + ": undecodable entry " + std::to_string(i + 1));
        state_.log.push_back(std::move(*e));
    }
    offsets_ = p.offsets;
    end_ = p.good_end;
    recovery_.records = p.payloads.size();
    recovery_.torn_bytes = log->size() - p.good_end;
    if (recovery_.torn_bytes > 0) {
        fs_.truncate(kLog, end_);
        fs_.fsync(kLog);
    }
}

void FileStorage::save_term_and_vote(Term term, std::optional<NodeId> voted_for) {
    fs_.create(kMetaTmp);
    fs_.append(kMetaTmp, encode_record(encode_meta(term, voted_for)));
    fs_.fsync(kMetaTmp);
    fs_.rename(kMetaTmp, kMeta);
    fs_.fsync_dir();
    state_.current_term = term;
    state_.voted_for = voted_for;
}

void FileStorage::write_log(LogIndex keep, std::span<const LogEntry> entries) {
    if (keep > state_.log.size()) throw std::out_of_range("FileStorage::write_log: keep > size");
    if (keep < state_.log.size()) {
        end_ = offsets_[keep];
        fs_.truncate(kLog, end_);
        offsets_.resize(keep);
        state_.log.resize(keep);
        dirty_ = true;
    }
    if (entries.empty()) return;
    std::string buf;
    for (const LogEntry& e : entries) {
        offsets_.push_back(end_ + buf.size());
        buf += encode_record(encode_entry(e));
    }
    fs_.append(kLog, buf);
    end_ += buf.size();
    state_.log.insert(state_.log.end(), entries.begin(), entries.end());
    dirty_ = true;
}

void FileStorage::sync() {
    if (!dirty_) return;
    fs_.fsync(kLog);
    dirty_ = false;
}

}  // namespace quorum::raft
