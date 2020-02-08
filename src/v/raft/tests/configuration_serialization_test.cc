#include "raft/consensus_utils.h"
#include "random/generators.h"
#include "test_utils/randoms.h"

#include <seastar/testing/thread_test_case.hh>

std::vector<model::broker> random_brokers() {
    std::vector<model::broker> ret;
    for (auto i = 0; i < random_generators::get_int(5, 10); ++i) {
        ret.push_back(tests::random_broker(i, i));
    }
    return ret;
}

SEASTAR_THREAD_TEST_CASE(roundtrip_raft_configuration_entry) {
    auto voters = random_brokers();
    auto learners = random_brokers();
    auto leader = model::node_id(random_generators::get_int(1, 10));
    raft::group_configuration cfg = {.nodes = voters, .learners = learners};

    // serialize to entry
    auto entry = raft::details::serialize_configuration(std::move(cfg));
    // extract from entry
    auto new_cfg
      = raft::details::extract_configuration(std::move(entry)).get0();

    BOOST_REQUIRE_EQUAL(voters, new_cfg->nodes);
    BOOST_REQUIRE_EQUAL(learners, new_cfg->learners);
}