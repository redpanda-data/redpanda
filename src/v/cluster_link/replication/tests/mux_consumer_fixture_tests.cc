/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/replication/mux_remote_consumer.h"
#include "kafka/client/direct_consumer/tests/direct_consumer_fixture.h"

#include <seastar/core/sleep.hh>

ss::logger test_log("mux-consumer-fixture-test");

using namespace cluster_link::replication;
using BasicConsumerFixture = kafka::client::tests::basic_consumer_fixture;

static constexpr std::chrono::milliseconds fetch_max_wait{10};
static constexpr size_t partition_max_buffered{1024 * 1024}; // 1MB

class MuxConsumerFixture : public BasicConsumerFixture {
public:
    void SetUp() override {
        basic_consumer_fixture::SetUp();
        auto consumer = make_consumer();
        _raw_consumer = consumer.get();
        _mux_consumer = std::make_unique<mux_remote_consumer>(
          std::move(consumer), partition_max_buffered, fetch_max_wait);
        _mux_consumer->start().get();
    }
    void TearDown() override {
        if (_mux_consumer) {
            _mux_consumer->stop().get();
        }
        basic_consumer_fixture::TearDown();
    }

    bool is_queue_full(const model::topic_partition& tp) {
        const auto& queues = _mux_consumer->_partitions;
        auto it = queues.find(tp);
        return it != queues.end() && it->second->full();
    }

    bool is_assigned(const model::topic_partition& tp) {
        const auto& subs = _raw_consumer->subscriptions();
        auto it = subs.find(tp.topic);
        return it != subs.end() && it->second.contains(tp.partition);
    }

    // Do nothing, handled by mux consumer
    void StartConsumer() override {}
    void StopConsumer() override {}

protected:
    std::unique_ptr<mux_remote_consumer> _mux_consumer;
    const kafka::client::direct_consumer* _raw_consumer;
};

TEST_P(MuxConsumerFixture, TestProduceConsume) {
    // Produces randomly to all partitions and ensures they are consumed
    // register partitions and set to start from 0
    for (int i = 0; i < 3; i++) {
        auto tp = model::topic_partition{topic, model::partition_id{i}};
        auto added = _mux_consumer->add(tp, kafka::offset{0});
        ASSERT_TRUE(added.has_value()) << added.error();
        auto reset = _mux_consumer->reset(tp, kafka::offset{0});
        ASSERT_TRUE(reset.has_value()) << reset.error();
    }
    absl::flat_hash_map<int32_t, size_t> produced;
    auto deadline = ss::lowres_clock::now() + 5s;
    auto producer = ss::async([this, &produced, &deadline]() {
        // produce random amounts of data to random partitions
        while (ss::lowres_clock::now() < deadline) {
            int partition = random_generators::get_int(0, 2);
            int count = random_generators::get_int(1, 50);
            produce_to_partition(topic, partition, count).get();
            produced[partition] += count;
            ss::sleep(
              std::chrono::milliseconds(random_generators::get_int(1, 10)))
              .get();
        }
    });
    chunked_vector<ss::future<>> fetchers;
    absl::flat_hash_map<int32_t, size_t> consumed;
    ss::abort_source as;
    for (int i = 0; i < 3; i++) {
        fetchers.push_back(
          ss::async([this, i, &consumed, &as]() {
              auto tp = model::topic_partition{topic, model::partition_id{i}};
              while (!as.abort_requested()) {
                  auto result = _mux_consumer->fetch(tp, as).get();
                  ASSERT_TRUE(result.has_value()) << result.error();
                  auto data = std::move(result.value());
                  auto num_records = std::accumulate(
                    data.batches.begin(),
                    data.batches.end(),
                    0,
                    [](size_t sum, const auto& batch) {
                        return sum + batch.header().record_count;
                    });
                  consumed[i] += num_records;
              }
          }).handle_exception_type([](const ss::abort_requested_exception&) {
          }));
    }
    {
        auto abort = ss::defer([&as]() { as.request_abort(); });
        // wait for all producers to produce and then wait for fetchers to
        // finish
        producer.get();
        RPTEST_REQUIRE_EVENTUALLY(5s, [&]() {
            return std::ranges::all_of(produced, [&consumed](const auto& p) {
                return p.second == consumed[p.first];
            });
        });
    }
    ss::when_all(fetchers.begin(), fetchers.end()).get();
}

TEST_P(MuxConsumerFixture, TestUnassignmentOnFullMemory) {
    auto tp = model::topic_partition{topic, model::partition_id{0}};
    auto added = _mux_consumer->add(tp, kafka::offset{0});
    ASSERT_TRUE(added.has_value()) << added.error();
    auto reset = _mux_consumer->reset(tp, kafka::offset{0});
    ASSERT_TRUE(reset.has_value()) << reset.error();

    RPTEST_REQUIRE_EVENTUALLY(2s, [this, &tp]() { return is_assigned(tp); });

    size_t produced = 0;
    while (!is_queue_full(tp)) {
        produce_to_partition(topic, 0, 10000).get();
        produced += 10000;
    }
    // If the queue is full, the partition should be unassigned.
    RPTEST_REQUIRE_EVENTUALLY(2s, [this, &tp]() { return !is_assigned(tp); });
    // drain some data, should reassign the partition.
    // data contains units that need destroying, hence in a nested scope.
    ss::abort_source as;
    size_t consumed = 0;
    auto fetcher
      = ss::async([this, &tp, &as, &consumed]() {
            while (!as.abort_requested()) {
                auto result = _mux_consumer->fetch(tp, as).get();
                ASSERT_TRUE(result.has_value()) << result.error();
                auto data = std::move(result.value());
                consumed += std::accumulate(
                  data.batches.begin(),
                  data.batches.end(),
                  0,
                  [](size_t sum, const auto& batch) {
                      return sum + batch.header().record_count;
                  });
            }
        }).handle_exception_type([](const ss::abort_requested_exception&) {});

    RPTEST_REQUIRE_EVENTUALLY(2s, [&]() { return consumed == produced; });
    as.request_abort();
    RPTEST_REQUIRE_EVENTUALLY(2s, [this, &tp]() { return is_assigned(tp); });
    RPTEST_REQUIRE_EVENTUALLY(2s, [this, &tp]() { return !is_queue_full(tp); });
    fetcher.get();
}

TEST_P(MuxConsumerFixture, TestResetPartition) {
    auto tp = model::topic_partition{topic, model::partition_id{0}};
    auto added = _mux_consumer->add(tp, kafka::offset{0});
    ASSERT_TRUE(added.has_value()) << added.error();
    auto reset = _mux_consumer->reset(tp, kafka::offset{0});
    ASSERT_TRUE(reset.has_value()) << reset.error();

    RPTEST_REQUIRE_EVENTUALLY(2s, [this, &tp]() { return is_assigned(tp); });

    ss::abort_source as;
    // no data in the partition right now.
    auto fetch = _mux_consumer->fetch(tp, as);
    // reset the consumer to offset 50, this should reset any inflight fetches.

    // publish some data into the partition in two batches.
    produce_to_partition(topic, 0, 100).get();
    produce_to_partition(topic, 0, 100).get();
    reset = _mux_consumer->reset(tp, kafka::offset{101});
    ASSERT_TRUE(reset.has_value()) << reset.error();
    // ignore first fetch result
    auto first_fetch_res = fetch.get();
    // note: consumer always fetches on batch boundaries.
    // fetch should fetch from offset [100 - 199) = 100 offsets
    auto fetch_res = _mux_consumer->fetch(tp, as).get();
    ASSERT_TRUE(fetch_res.has_value()) << fetch_res.error();
    auto batches = std::move(fetch_res.value().batches);
    EXPECT_EQ(batches.begin()->base_offset(), model::offset(100));
    auto records = std::accumulate(
      batches.begin(), batches.end(), 0, [](size_t sum, const auto& batch) {
          return sum + batch.header().record_count;
      });
    EXPECT_EQ(records, 100);
}

using session_config = kafka::client::tests::session_config;
INSTANTIATE_TEST_SUITE_P(
  MuxConsumerFixtureAndSessions,
  MuxConsumerFixture,
  testing::Values(
    session_config::with_sessions,
    session_config::without_sessions,
    session_config::toggle_sessions));
