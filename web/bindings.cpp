// Given: the page drives web::Simulation from JavaScript.
// Approach: embind bindings for the visualizer API. JavaScript numbers are
//           doubles, so 64-bit values cross as doubles (seeds and ticks stay
//           far below 2^53).

#include <emscripten/bind.h>

#include "quorum/web/simulation.h"

using quorum::web::Simulation;

namespace {

Simulation* create(double seed, unsigned servers) {
    return new Simulation(static_cast<std::uint64_t>(seed), servers);
}

}  // namespace

EMSCRIPTEN_BINDINGS(quorum) {
    using namespace emscripten;
    class_<Simulation>("Simulation")
        .function("tick", &Simulation::tick)
        .function("crash", &Simulation::crash)
        .function("restart", &Simulation::restart)
        .function("partition", &Simulation::partition)
        .function("heal", &Simulation::heal)
        .function("submit", optional_override([](Simulation& s, unsigned client, const std::string& op,
                                                 const std::string& key, const std::string& value) {
                      return static_cast<double>(s.submit(client, op, key, value));
                  }))
        .function("snapshot", &Simulation::snapshot)
        .function("now", optional_override([](const Simulation& s) { return static_cast<double>(s.now()); }))
        .function("seed", optional_override([](const Simulation& s) { return static_cast<double>(s.seed()); }));
    function("create", &create, allow_raw_pointers());
    function("scenarioTrace", optional_override([](double seed) {
                 return quorum::web::scenario_trace(static_cast<std::uint64_t>(seed));
             }));
}
