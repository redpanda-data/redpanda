#define BOOST_TEST_MODULE raft
#include "raft/consensus_utils.h"

#include <boost/test/unit_test.hpp>

model::broker test_broker(int32_t id) {
    return model::broker(model::node_id{id}, "localhost", 9092, std::nullopt);
}
std::vector<model::broker> test_brokers() {
    return {test_broker(1), test_broker(2), test_broker(3)};
}

BOOST_AUTO_TEST_CASE(test_lookup_existing) {
    auto broker = raft::details::find_machine(
      test_brokers(), model::node_id(2));

    BOOST_CHECK(broker);
    BOOST_CHECK(broker->id() == 2);
}

BOOST_AUTO_TEST_CASE(test_lookup_non_existing) {
    auto broker = raft::details::find_machine(
      test_brokers(), model::node_id(4));

    BOOST_CHECK(!broker);
}