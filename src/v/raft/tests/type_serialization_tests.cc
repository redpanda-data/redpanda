#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/random_batch.h"
#include "test_utils/rpc.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <vector>

struct checking_consumer {
    using batches_t = ss::circular_buffer<model::record_batch>;

    checking_consumer(ss::circular_buffer<model::record_batch> exp)
      : expected(std::move(exp)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        auto current_batch = std::move(expected.front());
        expected.pop_front();
        BOOST_REQUIRE_EQUAL(current_batch.base_offset(), batch.base_offset());
        BOOST_REQUIRE_EQUAL(current_batch.last_offset(), batch.last_offset());
        BOOST_REQUIRE_EQUAL(current_batch.crc(), batch.crc());
        BOOST_REQUIRE_EQUAL(current_batch.compressed(), batch.compressed());
        BOOST_REQUIRE_EQUAL(current_batch.type(), batch.type());
        BOOST_REQUIRE_EQUAL(current_batch.size_bytes(), batch.size_bytes());
        BOOST_REQUIRE_EQUAL(current_batch.size(), batch.size());
        BOOST_REQUIRE_EQUAL(current_batch.term(), batch.term());
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    void end_of_stream() { BOOST_REQUIRE(expected.empty()); }

    batches_t expected;
};

SEASTAR_THREAD_TEST_CASE(append_entries_requests) {
    auto batches = storage::test::make_random_batches(
      model::offset(1), 3, false);

    for (auto& b : batches) {
        b.set_term(model::term_id(123));
    }

    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    auto readers = raft::details::share_n(std::move(rdr), 2).get0();
    auto meta = raft::protocol_metadata{
      .group = 1,
      .commit_index = 100,
      .term = 10,
      .prev_log_index = 99,
      .prev_log_term = -1,
    };
    raft::append_entries_request req{.node_id = model::node_id(1),
                                     .meta = meta,
                                     .batches = std::move(readers.back())};

    readers.pop_back();
    auto d = async_serialize_roundtrip_rpc(std::move(req)).get0();

    BOOST_REQUIRE_EQUAL(d.node_id, model::node_id(1));
    BOOST_REQUIRE_EQUAL(d.meta.group, meta.group);
    BOOST_REQUIRE_EQUAL(d.meta.commit_index, meta.commit_index);
    BOOST_REQUIRE_EQUAL(d.meta.term, meta.term);
    BOOST_REQUIRE_EQUAL(d.meta.prev_log_index, meta.prev_log_index);
    BOOST_REQUIRE_EQUAL(d.meta.prev_log_term, meta.prev_log_term);

    auto batches_result = model::consume_reader_to_memory(
                            std::move(readers.back()), model::no_timeout)
                            .get0();
    d.batches
      .consume(checking_consumer(std::move(batches_result)), model::no_timeout)
      .get0();
}

model::broker create_test_broker() {
    return model::broker(
      model::node_id(random_generators::get_int(1000)), // id
      unresolved_address(
        "127.0.0.1",
        random_generators::get_int(10000, 20000)), // kafka api address
      unresolved_address(
        "127.0.0.1", random_generators::get_int(10000, 20000)), // rpc address
      "some_rack",
      model::broker_properties{
        .cores = 8 // cores
      });
}

SEASTAR_THREAD_TEST_CASE(group_configuration) {
    std::vector<model::broker> nodes;
    std::vector<model::broker> learners;
    for (int i = 0; i < 10; ++i) {
        nodes.push_back(create_test_broker());
        learners.push_back(create_test_broker());
    }

    raft::group_configuration cfg{.leader_id = model::node_id(10),
                                  .nodes = std::move(nodes),
                                  .learners = std::move(learners)};
    auto expected = cfg;

    auto deser = async_serialize_roundtrip_rpc(std::move(cfg)).get0();

    BOOST_REQUIRE_EQUAL(deser.leader_id, expected.leader_id);
    BOOST_REQUIRE_EQUAL(deser.nodes, expected.nodes);
    BOOST_REQUIRE_EQUAL(deser.learners, expected.learners);
}

SEASTAR_THREAD_TEST_CASE(serialize_configuration) {
    std::vector<model::broker> nodes;
    std::vector<model::broker> learners;
    for (int i = 0; i < 10; ++i) {
        nodes.push_back(create_test_broker());
        learners.push_back(create_test_broker());
    }

    raft::group_configuration cfg{.leader_id = model::node_id(10),
                                  .nodes = std::move(nodes),
                                  .learners = std::move(learners)};
    auto expected = cfg;

    auto batch_reader = raft::details::serialize_configuration(std::move(cfg));
    auto deser = raft::details::extract_configuration(std::move(batch_reader))
                   .get0()
                   .value();
    BOOST_REQUIRE_EQUAL(deser.leader_id, expected.leader_id);
    BOOST_REQUIRE_EQUAL(deser.nodes, expected.nodes);
    BOOST_REQUIRE_EQUAL(deser.learners, expected.learners);
}
