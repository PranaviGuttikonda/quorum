// Given: FileStorage must recover exactly the state Raft made durable, cut
//        off a torn tail, and refuse to run on corrupted synced data.
// Approach: drive it on SimDisk, where power loss is precise and seeded, and
//           check every recovery against what was synced and what was only
//           issued. One test runs on real files in a temp directory.

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "harness.h"
#include "quorum/raft/file_storage.h"
#include "quorum/sim/disk.h"
#include "quorum/storage/posix_fs.h"

using namespace quorum::raft;
using quorum::sim::DiskCrash;
using quorum::sim::SimDisk;

namespace {

std::vector<LogEntry> entries(Term term, int n, const std::string& tag) {
    std::vector<LogEntry> out;
    for (int i = 0; i < n; ++i) out.emplace_back(term, tag + std::to_string(i));
    return out;
}

void append(Storage& s, const std::vector<LogEntry>& es) {
    s.write_log(s.load().log.size(), es);
}

// Power loss, then a fresh recovery, as a reboot does.
PersistentState reboot(SimDisk& d) {
    d.power_loss();
    d.power_on();
    return FileStorage(d).load();
}

bool is_prefix(const std::vector<LogEntry>& p, const std::vector<LogEntry>& of) {
    return p.size() <= of.size() && std::equal(p.begin(), p.end(), of.begin());
}

std::uint64_t log_size(SimDisk& d) {
    return d.read(FileStorage::kLog)->size();
}

}  // namespace

TEST(file_storage_round_trips_through_a_reboot) {
    SimDisk d(1);
    {
        FileStorage s(d);
        CHECK(s.recovery().created);
        s.save_term_and_vote(3, 2);
        append(s, entries(1, 3, "a"));
        s.write_log(3, entries(3, 2, "b"));
        s.write_log(1, std::vector<LogEntry>{LogEntry::noop(3)});  // conflict truncation
        append(s, entries(3, 1, "c"));
        s.sync();
        CHECK(!s.has_unsynced_writes());
    }
    const PersistentState got = reboot(d);
    PersistentState want;
    want.current_term = 3;
    want.voted_for = 2;
    want.log = {LogEntry(1, "a0"), LogEntry::noop(3), LogEntry(3, "c0")};
    CHECK(got == want);
    FileStorage again(d);
    CHECK(!again.recovery().created);
    CHECK_EQ(again.recovery().records, std::uint64_t{3});
    CHECK_EQ(again.recovery().torn_bytes, std::uint64_t{0});
}

TEST(file_storage_sync_is_free_when_clean) {
    SimDisk d(2);
    FileStorage s(d);
    const auto before = d.stats().fsyncs;
    s.sync();
    s.sync();
    CHECK_EQ(d.stats().fsyncs, before);
    append(s, entries(1, 2, "x"));
    CHECK(s.has_unsynced_writes());
    s.sync();
    s.sync();
    CHECK_EQ(d.stats().fsyncs, before + 1);  // one fsync for the whole batch
}

TEST(file_storage_truncates_a_torn_final_record_at_every_cut) {
    // The bytes of a 4-entry log, and where the 4th record starts.
    std::string full;
    std::uint64_t three = 0;
    {
        SimDisk d(3);
        FileStorage s(d);
        append(s, entries(1, 3, "entry-"));
        s.sync();
        three = log_size(d);
        append(s, entries(2, 1, "the-last-entry-"));
        s.sync();
        full = *d.read(FileStorage::kLog);
    }
    CHECK(full.size() > three + 12);
    for (std::uint64_t cut = three + 1; cut < full.size(); ++cut) {
        SimDisk d(cut);
        d.create(FileStorage::kLog);
        d.append(FileStorage::kLog, full.substr(0, cut));
        d.fsync(FileStorage::kLog);
        d.fsync_dir();
        {
            FileStorage s(d);
            CHECK(s.load().log == entries(1, 3, "entry-"));
            CHECK_EQ(s.recovery().torn_bytes, cut - three);
            CHECK_EQ(log_size(d), three);  // truncated...
            CHECK_EQ(d.unsynced_ops(FileStorage::kLog), std::size_t{0});  // ...and synced
            // New entries land right after the last good record.
            append(s, entries(5, 1, "after"));
            s.sync();
        }
        std::vector<LogEntry> want = entries(1, 3, "entry-");
        want.emplace_back(5, "after0");
        CHECK(reboot(d).log == want);
    }
}

TEST(file_storage_reports_a_bit_flip_in_the_middle_as_corruption) {
    std::string full;
    std::vector<std::uint64_t> starts;  // record start offsets
    {
        SimDisk d(4);
        FileStorage s(d);
        for (int i = 0; i < 5; ++i) {
            starts.push_back(log_size(d));
            append(s, entries(1, 1, "record-" + std::to_string(i) + "-"));
            s.sync();
        }
        full = *d.read(FileStorage::kLog);
    }
    auto recover_with_flip = [&](std::uint64_t byte) {
        SimDisk d(5);
        d.create(FileStorage::kLog);
        d.append(FileStorage::kLog, full);
        d.fsync(FileStorage::kLog);
        d.fsync_dir();
        d.flip_bit(FileStorage::kLog, byte, 3);
        return FileStorage(d).load();
    };
    // Every byte of records 1..4 (all but the last): length, its checksum,
    // the payload checksum and the payload.
    for (std::uint64_t byte = 0; byte < starts[4]; ++byte) {
        bool threw = false;
        try {
            (void)recover_with_flip(byte);
        } catch (const StorageCorruption&) {
            threw = true;
        }
        if (!threw) quorum::test::fail(__FILE__, __LINE__, "flip at byte " + std::to_string(byte) + " not reported");
    }
    // The final record's length fields are protected too.
    for (std::uint64_t byte = starts[4]; byte < starts[4] + 8; ++byte)
        CHECK_THROWS(recover_with_flip(byte));
    // A bad payload in the final record can't be told apart from a torn
    // write of it, so it is dropped as a torn tail (file_storage.h).
    const PersistentState last = recover_with_flip(starts[4] + 12);
    CHECK_EQ(last.log.size(), std::size_t{4});
}

TEST(file_storage_reports_damaged_meta_and_a_missing_log) {
    {
        SimDisk d(6);
        FileStorage(d).save_term_and_vote(7, 1);
        d.flip_bit(FileStorage::kMeta, 14, 0);
        CHECK_THROWS(FileStorage{d});
    }
    {
        SimDisk d(7);
        FileStorage(d).save_term_and_vote(7, 1);
        d.remove(FileStorage::kLog);
        d.fsync_dir();
        CHECK_THROWS(FileStorage{d});
    }
}

TEST(file_storage_crash_between_write_and_fsync_keeps_a_prefix) {
    const auto synced = entries(1, 4, "synced-");
    const auto unsynced = entries(2, 6, "unsynced-");
    std::vector<LogEntry> all = synced;
    all.insert(all.end(), unsynced.begin(), unsynced.end());
    bool lost_all = false, kept_some = false, torn = false;
    for (std::uint64_t seed = 1; seed <= 200; ++seed) {
        SimDisk d(seed);
        {
            FileStorage s(d);
            append(s, synced);
            s.sync();
            // Three separate appends, none synced.
            s.write_log(4, std::vector<LogEntry>(unsynced.begin(), unsynced.begin() + 2));
            s.write_log(6, std::vector<LogEntry>(unsynced.begin() + 2, unsynced.begin() + 4));
            s.write_log(8, std::vector<LogEntry>(unsynced.begin() + 4, unsynced.end()));
        }
        d.power_loss();
        d.power_on();
        FileStorage s(d);
        const auto& log = s.load().log;
        CHECK(is_prefix(synced, log));  // everything synced survives
        CHECK(is_prefix(log, all));     // nothing else appears
        lost_all |= log.size() == synced.size();
        kept_some |= log.size() > synced.size();
        torn |= s.recovery().torn_bytes > 0;
    }
    CHECK(lost_all && kept_some && torn);
}

TEST(file_storage_unsynced_conflict_truncation_may_be_undone) {
    // A follower truncates a conflicting suffix and appends, then loses power
    // before syncing. Each outcome is a state the follower could have had
    // before or during the step; none mixes in anything else.
    const auto old_log = entries(1, 5, "old-");
    const auto fresh = entries(2, 2, "new-");
    std::vector<LogEntry> after(old_log.begin(), old_log.begin() + 2);
    after.insert(after.end(), fresh.begin(), fresh.end());
    std::set<std::size_t> sizes;
    for (std::uint64_t seed = 1; seed <= 200; ++seed) {
        SimDisk d(seed);
        {
            FileStorage s(d);
            append(s, old_log);
            s.sync();
            s.write_log(2, fresh);
        }
        const auto log = reboot(d).log;
        CHECK(log == old_log || is_prefix(log, after));
        CHECK(log.size() >= 2);
        sizes.insert(log.size());
    }
    CHECK(sizes.count(5) && sizes.count(2) && sizes.count(4));
}

TEST(file_storage_crash_during_the_term_vote_update_keeps_old_or_new) {
    // save_term_and_vote makes five calls: create, append, fsync, rename,
    // fsync_dir. A power loss partway through any of them must leave the old
    // value, because the new one isn't durable until the last completes.
    for (std::uint64_t call = 0; call < 5; ++call) {
        for (std::uint64_t seed = 1; seed <= 40; ++seed) {
            SimDisk d(seed * 10 + call);
            {
                FileStorage s(d);
                s.save_term_and_vote(4, 1);
                d.crash_after(call);
                CHECK_THROWS(s.save_term_and_vote(5, 2));
            }
            d.power_on();
            const PersistentState got = FileStorage(d).load();
            CHECK_EQ(got.current_term, Term{4});
            CHECK(got.voted_for == std::optional<NodeId>{1});
        }
    }
    // Once it returns, the new value survives any power loss.
    for (std::uint64_t seed = 1; seed <= 40; ++seed) {
        SimDisk d(seed);
        {
            FileStorage s(d);
            s.save_term_and_vote(4, 1);
            s.save_term_and_vote(5, std::nullopt);
        }
        const PersistentState got = reboot(d);
        CHECK_EQ(got.current_term, Term{5});
        CHECK(!got.voted_for.has_value());
        CHECK(!d.read(FileStorage::kMetaTmp).has_value());
    }
}

TEST(file_storage_recovering_twice_in_a_row_is_stable) {
    for (std::uint64_t seed = 1; seed <= 100; ++seed) {
        SimDisk d(seed);
        {
            FileStorage s(d);
            s.save_term_and_vote(2, 3);
            append(s, entries(2, 3, "kept-"));
            s.sync();
            append(s, entries(2, 3, "maybe-"));
        }
        d.power_loss();
        d.power_on();
        const PersistentState first = FileStorage(d).load();
        // Again, with nothing written in between, and again after a crash in
        // the middle of that recovery's own truncate and fsync.
        CHECK(reboot(d) == first);
        d.crash_after(seed % 3);
        try {
            FileStorage again(d);
        } catch (const DiskCrash&) {
        }
        CHECK(reboot(d) == first);
        FileStorage last(d);
        CHECK_EQ(last.recovery().torn_bytes, std::uint64_t{0});
        CHECK(last.load() == first);
        CHECK(is_prefix(entries(2, 3, "kept-"), first.log));
    }
}

TEST(file_storage_rejects_keep_beyond_the_log) {
    SimDisk d(8);
    FileStorage s(d);
    append(s, entries(1, 2, "x"));
    CHECK_THROWS(s.write_log(3, entries(1, 1, "y")));
}

TEST(file_storage_on_real_files) {
    namespace fs = std::filesystem;
    std::random_device rd;
    const fs::path dir = fs::temp_directory_path() / ("quorum-file-storage-" + std::to_string(rd()));
    fs::create_directories(dir);
    struct Cleanup {
        fs::path p;
        ~Cleanup() {
            std::error_code ec;
            fs::remove_all(p, ec);
        }
    } cleanup{dir};

    quorum::storage::PosixFileSystem pfs(dir.string());
    {
        FileStorage s(pfs);
        CHECK(s.recovery().created);
        s.save_term_and_vote(9, 4);
        append(s, entries(9, 3, "real-"));
        s.write_log(2, entries(9, 1, "replaced-"));
        s.sync();
    }
    std::vector<LogEntry> want = entries(9, 2, "real-");
    want.emplace_back(9, "replaced-0");
    {
        FileStorage s(pfs);
        CHECK_EQ(s.load().current_term, Term{9});
        CHECK(s.load().voted_for == std::optional<NodeId>{4});
        CHECK(s.load().log == want);
        CHECK(!fs::exists(dir / FileStorage::kMetaTmp));
    }

    // A torn tail: half of a record appended by hand.
    const std::string record = encode_record("E 9 0 4 torn");
    pfs.append(FileStorage::kLog, record.substr(0, record.size() / 2));
    const auto good_size = fs::file_size(dir / FileStorage::kLog) - record.size() / 2;
    {
        FileStorage s(pfs);
        CHECK(s.load().log == want);
        CHECK_EQ(s.recovery().torn_bytes, std::uint64_t{record.size() / 2});
        CHECK_EQ(fs::file_size(dir / FileStorage::kLog), good_size);
    }

    // A flipped bit in the first record's payload, with records after it.
    {
        std::fstream f(dir / FileStorage::kLog, std::ios::in | std::ios::out | std::ios::binary);
        f.seekg(12);
        char c = 0;
        f.read(&c, 1);
        c = static_cast<char>(c ^ 0x10);
        f.seekp(12);
        f.write(&c, 1);
    }
    CHECK_THROWS(FileStorage{pfs});
}
