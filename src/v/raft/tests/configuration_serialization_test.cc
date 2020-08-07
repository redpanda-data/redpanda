#include "raft/consensus_utils.h"
#include "random/generators.h"
#include "storage/tests/utils/random_batch.h"
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
    auto batches = raft::details::serialize_configuration_as_batches(
      std::move(cfg));
    // extract from entry
    auto new_cfg = reflection::adl<raft::group_configuration>{}.from(
      batches.begin()->begin()->release_value());

    BOOST_REQUIRE_EQUAL(voters, new_cfg.nodes);
    BOOST_REQUIRE_EQUAL(learners, new_cfg.learners);
}

struct test_consumer {
    explicit test_consumer(model::offset base_offset)
      : _next_offset(base_offset + model::offset(1)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch& b) {
        if (
          !b.compressed()
          && b.header().type == raft::configuration_batch_type) {
            _config_offsets.push_back(_next_offset);
        }
        _next_offset += model::offset(b.header().last_offset_delta)
                        + model::offset(1);
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    std::vector<model::offset> end_of_stream() { return _config_offsets; }

    model::offset _next_offset;
    std::vector<model::offset> _config_offsets;
};

SEASTAR_THREAD_TEST_CASE(test_config_extracting_reader) {
    raft::group_configuration cfg_1 = {
      .nodes = random_brokers(), .learners = random_brokers()};
    raft::group_configuration cfg_2 = {
      .nodes = random_brokers(), .learners = random_brokers()};
    using batches_t = ss::circular_buffer<model::record_batch>;
    ss::circular_buffer<model::record_batch> all_batches;

    // serialize to batches
    auto cfg_batch_1 = raft::details::serialize_configuration_as_batches(cfg_1);
    auto cfg_batch_2 = raft::details::serialize_configuration_as_batches(cfg_2);
    auto batches = storage::test::make_random_batches(
      model::offset(0), 10, true);

    std::vector<batches_t> ranges;
    ranges.reserve(4);
    // interleave config batches with data batches
    ranges.push_back(std::move(cfg_batch_1));
    ranges.push_back(
      storage::test::make_random_batches(model::offset(0), 10, true));
    ranges.push_back(std::move(cfg_batch_2));
    ranges.push_back(
      storage::test::make_random_batches(model::offset(0), 10, true));

    for (auto& r : ranges) {
        std::move(r.begin(), r.end(), std::back_inserter(all_batches));
    }

    raft::details::for_each_ref_extract_configuration(
      model::offset(100),
      model::make_memory_record_batch_reader(std::move(all_batches)),
      test_consumer(model::offset(100)),
      model::no_timeout)
      .then([&cfg_1, &cfg_2](auto res) {
          auto& [offsets, configurations] = res;
          BOOST_REQUIRE_EQUAL(offsets[0], model::offset(101));
          BOOST_REQUIRE_EQUAL(configurations[0].offset, model::offset(101));
          BOOST_REQUIRE_EQUAL(configurations[0].cfg.nodes, cfg_1.nodes);
          BOOST_REQUIRE_EQUAL(configurations[0].cfg.learners, cfg_1.learners);
          BOOST_REQUIRE_EQUAL(configurations[1].offset, offsets[1]);
          BOOST_REQUIRE_EQUAL(configurations[1].cfg.nodes, cfg_2.nodes);
          BOOST_REQUIRE_EQUAL(configurations[1].cfg.learners, cfg_2.learners);
      })
      .get0();
}