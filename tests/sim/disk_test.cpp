// Given: SimDisk must lose exactly what a real disk may lose on power loss:
//        unsynced contents (except a seed-chosen in-order prefix, whose last
//        write may be torn) and names not made durable by fsync_dir.
// Approach: check the guarantees directly, then sweep seeds to show every
//           outcome (all lost, some kept, torn) occurs and is reproducible.

#include <set>
#include <string>

#include "harness.h"
#include "quorum/sim/disk.h"

using quorum::sim::DiskCrash;
using quorum::sim::SimDisk;

namespace {

// A disk holding `name` with synced contents `data`, its name durable.
void put_durable(SimDisk& d, const std::string& name, const std::string& data) {
    d.create(name);
    d.append(name, data);
    d.fsync(name);
    d.fsync_dir();
}

}  // namespace

TEST(simdisk_synced_data_survives_power_loss) {
    SimDisk d(1);
    put_durable(d, "f", "hello");
    d.power_loss();
    CHECK(!d.powered());
    CHECK_THROWS(d.read("f"));
    d.power_on();
    CHECK_EQ(*d.read("f"), std::string("hello"));
}

TEST(simdisk_unsynced_appends_survive_only_as_an_in_order_prefix) {
    std::set<std::string> outcomes;
    for (std::uint64_t seed = 1; seed <= 300; ++seed) {
        SimDisk d(seed);
        put_durable(d, "f", "base|");
        d.append("f", "one|");
        d.append("f", "two|");
        CHECK_EQ(d.unsynced_ops("f"), std::size_t{2});
        d.power_loss();
        d.power_on();
        const std::string got = *d.read("f");
        // Always the durable contents plus a prefix of what was issued.
        CHECK(std::string("base|one|two|").rfind(got, 0) == 0);
        CHECK(got.size() >= 5);
        outcomes.insert(got);
    }
    CHECK(outcomes.count("base|"));          // everything lost
    CHECK(outcomes.count("base|one|"));      // one write kept
    CHECK(outcomes.count("base|one|two|"));  // both kept
    // Torn writes: anything strictly inside a write.
    bool torn = false;
    for (const auto& o : outcomes) torn |= o != "base|" && o != "base|one|" && o != "base|one|two|";
    CHECK(torn);
}

TEST(simdisk_names_need_a_directory_fsync) {
    SimDisk d(2);
    put_durable(d, "old", "x");
    d.create("new");
    d.append("new", "y");
    d.fsync("new");  // contents durable, name not
    d.power_loss();
    d.power_on();
    CHECK(!d.read("new").has_value());
    CHECK(d.read("old").has_value());
}

TEST(simdisk_rename_is_undone_without_a_directory_fsync) {
    SimDisk d(3);
    put_durable(d, "meta", "v1");
    d.create("tmp");
    d.append("tmp", "v2");
    d.fsync("tmp");
    d.rename("tmp", "meta");
    CHECK_EQ(*d.read("meta"), std::string("v2"));
    d.power_loss();
    d.power_on();
    CHECK_EQ(*d.read("meta"), std::string("v1"));
    CHECK(!d.read("tmp").has_value());

    d.create("tmp");
    d.append("tmp", "v2");
    d.fsync("tmp");
    d.rename("tmp", "meta");
    d.fsync_dir();
    d.power_loss();
    d.power_on();
    CHECK_EQ(*d.read("meta"), std::string("v2"));
}

TEST(simdisk_crash_after_fires_on_the_right_call) {
    SimDisk d(4);
    put_durable(d, "f", "a");
    d.crash_after(2);
    d.append("f", "b");
    d.append("f", "c");
    CHECK(d.powered());
    CHECK_THROWS(d.fsync("f"));  // the third call loses power before completing
    CHECK(!d.powered());
    CHECK(!d.armed());
    CHECK_EQ(d.stats().mid_call_crashes, std::uint64_t{1});
    CHECK_THROWS(d.append("f", "d"));  // still off
    d.power_on();
    const std::string got = *d.read("f");
    CHECK(std::string("abc").rfind(got, 0) == 0 && !got.empty());
}

TEST(simdisk_mid_call_crash_may_tear_the_write_in_flight) {
    bool torn = false, whole = false, lost = false;
    for (std::uint64_t seed = 1; seed <= 200; ++seed) {
        SimDisk d(seed);
        put_durable(d, "f", "a");
        d.crash_after(0);
        bool threw = false;
        try {
            d.append("f", "0123456789");
        } catch (const DiskCrash&) {
            threw = true;
        }
        CHECK(threw);
        d.power_on();
        const std::string got = *d.read("f");
        CHECK(std::string("a0123456789").rfind(got, 0) == 0);
        lost |= got == "a";
        whole |= got == "a0123456789";
        torn |= got.size() > 1 && got.size() < 11;
    }
    CHECK(lost && whole && torn);
}

TEST(simdisk_power_loss_is_reproducible) {
    for (std::uint64_t seed : {11ULL, 12ULL, 13ULL}) {
        std::string a, b;
        for (std::string* out : {&a, &b}) {
            SimDisk d(seed);
            put_durable(d, "f", "x");
            for (int i = 0; i < 5; ++i) d.append("f", "chunk" + std::to_string(i));
            d.power_loss();
            d.power_on();
            *out = *d.read("f");
        }
        CHECK_EQ(a, b);
    }
}

TEST(simdisk_flip_bit_corrupts_durable_contents) {
    SimDisk d(5);
    put_durable(d, "f", "A");
    d.flip_bit("f", 0, 0);
    d.power_loss();
    d.power_on();
    CHECK_EQ(*d.read("f"), std::string("@"));
    CHECK_THROWS(d.flip_bit("f", 1, 0));
}

TEST(simdisk_rejects_bad_calls) {
    SimDisk d(6);
    CHECK_THROWS(d.append("missing", "x"));
    CHECK_THROWS(d.fsync("missing"));
    CHECK_THROWS(d.rename("missing", "x"));
    put_durable(d, "f", "abc");
    CHECK_THROWS(d.truncate("f", 4));
    d.truncate("f", 1);
    CHECK_EQ(*d.read("f"), std::string("a"));
    d.remove("f");
    CHECK(!d.read("f").has_value());
    d.remove("f");  // removing a missing file is fine
}
