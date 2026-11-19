#include <algorithm>
#include <cstdio>
#include <cstring>
#include <exception>

#include "fuzz_seeds.h"
#include "harness.h"

namespace quorum::test {

int run_all(const char* filter) {
    std::vector<Case> cases = registry();
    std::sort(cases.begin(), cases.end(),
              [](const Case& a, const Case& b) { return std::strcmp(a.name, b.name) < 0; });

    int ran = 0;
    int failed = 0;
    for (const Case& c : cases) {
        if (filter != nullptr && std::strstr(c.name, filter) == nullptr) continue;
        ++ran;
        try {
            c.fn();
            std::printf("[ OK   ] %s\n", c.name);
        } catch (const Failure& f) {
            ++failed;
            std::printf("[ FAIL ] %s\n         %s\n", c.name, f.message.c_str());
        } catch (const std::exception& e) {
            ++failed;
            std::printf("[ FAIL ] %s\n         unexpected exception: %s\n", c.name, e.what());
        } catch (...) {
            ++failed;
            std::printf("[ FAIL ] %s\n         unexpected non-std exception\n", c.name);
        }
    }

    std::printf("\n%d run, %d passed, %d failed\n", ran, ran - failed, failed);
    if (ran == 0) {
        std::printf("no tests matched\n");
        return 1;
    }
    return failed == 0 ? 0 : 1;
}

}  // namespace quorum::test

int main(int argc, char** argv) {
    try {
        std::printf("fuzz seeds: %llu (QUORUM_FUZZ_SEEDS)\n\n",
                    static_cast<unsigned long long>(quorum::testing::fuzz_seeds()));
    } catch (const std::exception& e) {
        std::printf("%s\n", e.what());
        return 2;
    }
    return quorum::test::run_all(argc > 1 ? argv[1] : nullptr);
}
