// Given: RaftCluster hosts one Service per live node and routes messages by
//        sender.
// Approach: a recording echo service, checked for routing, its
//           crash/restart lifecycle, and committed entries being replayed to
//           a fresh incarnation.

#include <memory>
#include <string>
#include <vector>

#include "harness.h"
#include "quorum/raft/cluster.h"

using namespace quorum::raft;

namespace {

struct Echo final : Service {
    NodeId self;
    RaftNode& node;
    ServiceSend send;
    std::vector<std::string> from_clients;
    std::vector<LogIndex> applied;

    Echo(NodeId s, RaftNode& n, ServiceSend snd) : self(s), node(n), send(std::move(snd)) {}
    void on_apply(LogIndex index, const LogEntry&) override { applied.push_back(index); }
    void on_client_message(NodeId from, std::string_view payload) override {
        from_clients.emplace_back(payload);
        send(from, "echo:" + std::string(payload));
        if (payload == "propose" && node.role() == Role::Leader) node.propose("from-client");
    }
};

ServiceFactory echo_factory() {
    return [](NodeId self, RaftNode& node, ServiceSend send) -> std::unique_ptr<Service> {
        return std::make_unique<Echo>(self, node, std::move(send));
    };
}

const Echo* echo(const RaftCluster& c, NodeId id) {
    return dynamic_cast<const Echo*>(c.service(id));
}

}  // namespace

TEST(service_routes_client_messages_and_replies) {
    RaftCluster c(1, 3, {}, {}, echo_factory());
    std::vector<std::string> got;
    c.add_client(100, [&](const quorum::sim::Message& m) { got.push_back(m.payload); });
    c.client_send(100, 2, "hello");
    c.run_for(30);
    CHECK((echo(c, 2)->from_clients == std::vector<std::string>{"hello"}));
    CHECK(echo(c, 1)->from_clients.empty());
    CHECK((got == std::vector<std::string>{"echo:hello"}));
    // Raft traffic between servers never reaches the service.
    c.run_for(1000);
    for (NodeId id : c.ids()) CHECK(echo(c, id)->from_clients.size() <= 1);
}

TEST(service_lifecycle_follows_crash_and_restart) {
    RaftCluster c(2, 3, {}, {}, echo_factory());
    c.add_client(100, [](const quorum::sim::Message&) {});
    CHECK(c.run_until([](const RaftCluster& x) { return x.stable_leader().has_value(); }, 2000));
    const NodeId leader = *c.stable_leader();
    c.client_send(100, leader, "propose");
    CHECK(c.run_until([&](const RaftCluster& x) { return echo(x, 1)->applied.size() >= 2; }, 2000));

    const NodeId victim = leader == 1 ? 2 : 1;
    c.crash(victim);
    CHECK(c.service(victim) == nullptr);
    c.restart(victim);
    const Echo* fresh = echo(c, victim);
    CHECK(fresh != nullptr);
    CHECK(fresh->applied.empty());  // volatile: a new incarnation starts empty
    // The log is replayed from index 1 once the node learns the commit index.
    CHECK(c.run_until([&](const RaftCluster& x) { return echo(x, victim)->applied.size() >= 2; }, 2000));
    CHECK_EQ(echo(c, victim)->applied.front(), LogIndex{1});
}

TEST(service_clients_are_outside_partitions) {
    RaftCluster c(3, 3, {}, {}, echo_factory());
    std::vector<std::string> got;
    c.add_client(100, [&](const quorum::sim::Message& m) { got.push_back(m.payload); });
    c.partition({{1}, {2, 3}});
    c.client_send(100, 1, "a");
    c.client_send(100, 3, "b");
    c.run_for(30);
    CHECK_EQ(got.size(), std::size_t{2});

    c.set_global(100, false);  // now unlisted, so cut off from both groups
    c.client_send(100, 1, "c");
    c.run_for(30);
    CHECK_EQ(got.size(), std::size_t{2});
}

TEST(service_client_registration_is_validated) {
    RaftCluster c(4, 3, {}, {}, echo_factory());
    CHECK_THROWS(c.add_client(2, [](const quorum::sim::Message&) {}));
    CHECK_THROWS(c.client_send(1, 2, "not a client"));
    CHECK(c.is_server(3) && !c.is_server(4) && !c.is_server(0));
}
