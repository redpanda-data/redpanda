#define BOOST_TEST_MODULE raft
#include "raft/consensus_utils.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/tests/utils/random_batch.h"

#include <seastar/core/circular_buffer.hh>

#include <boost/test/unit_test.hpp>

model::broker test_broker(int32_t id) {
    return model::broker(
      model::node_id{id},
      unresolved_address("127.0.0.1", 9092),
      unresolved_address("127.0.0.1", 1234),
      std::nullopt,
      model::broker_properties{});
}
std::vector<model::broker> test_brokers() {
    return {test_broker(1), test_broker(2), test_broker(3)};
}

BOOST_AUTO_TEST_CASE(test_lookup_existing) {
    auto brokers = test_brokers();
    auto it = raft::details::find_machine(
      std::begin(brokers), std::end(brokers), model::node_id(2));
    BOOST_CHECK(it != brokers.end());
    BOOST_CHECK(it->id() == 2);
}

BOOST_AUTO_TEST_CASE(test_lookup_non_existing) {
    auto brokers = test_brokers();
    auto it = raft::details::find_machine(
      std::begin(brokers), std::end(brokers), model::node_id(4));

    BOOST_CHECK(it == brokers.end());
}

BOOST_AUTO_TEST_CASE(test_filling_gaps) {
    auto batches = storage::test::make_random_batches(
      model::offset(20), 50, true);

    // cut some holes in log
    for (size_t i = 0; i < 10; ++i) {
        auto idx = random_generators::get_int(batches.size() - 1);
        auto it = std::next(batches.begin(), idx);
        batches.erase(it, std::next(it));
    }

    model::offset first_expected = model::offset(10);

    auto without_gaps = raft::details::make_ghost_batches_in_gaps(
      first_expected, std::move(batches));

    for (auto& b : without_gaps) {
        BOOST_REQUIRE_EQUAL(b.base_offset(), first_expected);
        first_expected = raft::details::next_offset(b.last_offset());
    }
}