// Given: every other test relies on the harness reporting failures.
// Approach: run the CHECK macros on passing and failing input and confirm that
//           a failure throws quorum::test::Failure.

#include <stdexcept>
#include <string>

#include "harness.h"

namespace {

bool check_fails(void (*body)()) {
    try {
        body();
    } catch (const quorum::test::Failure&) {
        return true;
    }
    return false;
}

}  // namespace

TEST(harness_passing_checks_do_not_throw) {
    CHECK(1 + 1 == 2);
    CHECK_EQ(std::string("a"), std::string("a"));
    CHECK_THROWS(throw std::runtime_error("x"));
}

TEST(harness_failing_checks_throw_failure) {
    CHECK(check_fails([] { CHECK(false); }));
    CHECK(check_fails([] { CHECK_EQ(1, 2); }));
    CHECK(check_fails([] { CHECK_THROWS(0); }));
}

TEST(harness_failure_message_shows_values) {
    try {
        CHECK_EQ(3, 4);
    } catch (const quorum::test::Failure& f) {
        CHECK(f.message.find("3 != 4") != std::string::npos);
        return;
    }
    CHECK(false);
}
