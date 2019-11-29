#define BOOST_TEST_MODULE raft
#include "raft/types.h"

#include <boost/test/unit_test.hpp>

model::broker create_broker(int32_t id) {
    return model::broker(model::node_id{id}, "localhost", 9002, std::nullopt);
}

BOOST_AUTO_TEST_CASE(should_return_true_as_it_contains_learner) {
    raft::group_configuration test_grp = {.leader_id = model::node_id(10),
                                          .nodes = {},
                                          .learners = {create_broker(1)}};

    auto contains = test_grp.contains_broker(model::node_id(1));
    BOOST_REQUIRE_EQUAL(contains, true);
}

BOOST_AUTO_TEST_CASE(should_return_true_as_it_contains_voter) {
    raft::group_configuration test_grp = {.leader_id = model::node_id(1),
                                          .nodes = {create_broker(1)},
                                          .learners = {}};

    auto contains = test_grp.contains_broker(model::node_id(1));
    BOOST_REQUIRE_EQUAL(contains, true);
}

BOOST_AUTO_TEST_CASE(should_return_false_as_it_does_not_contain_machine) {
    raft::group_configuration test_grp = {.leader_id = model::node_id(1),
                                          .nodes = {create_broker(3)},
                                          .learners = {create_broker(4)}};

    auto contains = test_grp.contains_broker(model::node_id(1));
    BOOST_REQUIRE_EQUAL(contains, false);
}