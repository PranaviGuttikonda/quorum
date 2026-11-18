#pragma once

// Given: the project needs unit tests without an external dependency.
// Approach: a self-registering TEST() macro plus CHECK macros that throw on
//           failure; run_all() executes every registered case in name order.

#include <ostream>
#include <sstream>
#include <string>
#include <vector>

namespace quorum::test {

struct Case {
    const char* name;
    void (*fn)();
};

// Given: TEST() objects in different translation units register during static
//        initialization, in an unspecified order.
// Approach: a function-local static avoids the init-order problem; run_all()
//           sorts by name so the run order is stable.
inline std::vector<Case>& registry() {
    static std::vector<Case> cases;
    return cases;
}

struct Registrar {
    Registrar(const char* name, void (*fn)()) { registry().push_back({name, fn}); }
};

struct Failure {
    std::string message;
};

[[noreturn]] inline void fail(const char* file, int line, const std::string& what) {
    std::ostringstream os;
    os << file << ":" << line << ": " << what;
    throw Failure{os.str()};
}

template <class T>
std::string show(const T& value) {
    if constexpr (requires(std::ostream& os, const T& v) { os << v; }) {
        std::ostringstream os;
        os << value;
        return os.str();
    } else {
        return "<unprintable>";
    }
}

// Runs every case whose name contains `filter` (all cases if null).
// Returns the process exit code: 0 only if at least one case ran and none failed.
int run_all(const char* filter);

}  // namespace quorum::test

#define QT_CAT2(a, b) a##b
#define QT_CAT(a, b) QT_CAT2(a, b)

#define TEST(name)                                                               \
    static void QT_CAT(qt_test_, name)();                                        \
    static const ::quorum::test::Registrar QT_CAT(qt_reg_, name){#name,          \
                                                                 &QT_CAT(qt_test_, name)}; \
    static void QT_CAT(qt_test_, name)()

#define CHECK(expr)                                                              \
    do {                                                                         \
        if (!(expr)) ::quorum::test::fail(__FILE__, __LINE__, "CHECK(" #expr ")"); \
    } while (0)

#define CHECK_EQ(a, b)                                                           \
    do {                                                                         \
        const auto& qt_a = (a);                                                  \
        const auto& qt_b = (b);                                                  \
        if (!(qt_a == qt_b))                                                     \
            ::quorum::test::fail(__FILE__, __LINE__,                             \
                                 "CHECK_EQ(" #a ", " #b "): " +                  \
                                     ::quorum::test::show(qt_a) + " != " +       \
                                     ::quorum::test::show(qt_b));                \
    } while (0)

#define CHECK_THROWS(expr)                                                       \
    do {                                                                         \
        bool qt_threw = false;                                                   \
        try {                                                                    \
            (void)(expr);                                                        \
        } catch (const ::quorum::test::Failure&) {                               \
            throw;                                                               \
        } catch (...) {                                                          \
            qt_threw = true;                                                     \
        }                                                                        \
        if (!qt_threw)                                                           \
            ::quorum::test::fail(__FILE__, __LINE__, "CHECK_THROWS(" #expr ")"); \
    } while (0)
