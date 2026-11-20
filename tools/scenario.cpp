// Given: the WASM build must match native for the same seed.
// Approach: print web::scenario_trace for each seed on the command line
//           (default 1 2 3). scripts/wasm_determinism.mjs prints the same from
//           the WASM module, and CI diffs the two.

#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "quorum/web/simulation.h"

int main(int argc, char** argv) {
    std::vector<std::uint64_t> seeds;
    for (int i = 1; i < argc; ++i) seeds.push_back(std::strtoull(argv[i], nullptr, 10));
    if (seeds.empty()) seeds = {1, 2, 3};
    for (std::uint64_t seed : seeds) {
        std::printf("== seed %llu\n%s", static_cast<unsigned long long>(seed),
                    quorum::web::scenario_trace(seed).c_str());
    }
}
