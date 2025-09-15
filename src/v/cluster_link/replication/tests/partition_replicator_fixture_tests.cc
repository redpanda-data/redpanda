/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/deps_impl.h"
#include "cluster_link/replication/mux_remote_consumer.h"
#include "cluster_link/replication/partition_replicator.h"
#include "kafka/client/direct_consumer/tests/direct_consumer_fixture.h"

namespace {
ss::logger logger{"replicator-fixture-test"};
}
using namespace cluster_link::replication;
using BasicConsumerFixture = kafka::client::tests::basic_consumer_fixture;

static constexpr std::chrono::milliseconds fetch_max_wait{10};
static constexpr size_t partition_max_buffered{1024 * 1024}; // 1MB

class ReplicatorFixture : public BasicConsumerFixture {
public:
    void SetUp() override {
        basic_consumer_fixture::SetUp();
        auto consumer = make_consumer();
        _mux_consumer = std::make_unique<mux_remote_consumer>(
          make_consumer(), partition_max_buffered, fetch_max_wait);
        _mux_consumer->start().get();
        // create source and target topics;
        create_topic(model::topic_namespace_view{_source}).get();
        create_topic(model::topic_namespace_view{_target}).get();
        // setup the replicator
        auto [_, partition] = get_leader(_target);
        vassert(partition, "no partition for {}", _target);

        auto source = std::make_unique<remote_partition_source>(
          _source.tp, *_mux_consumer);
        auto sink = std::make_unique<local_partition_sink>(partition);
        _replicator = std::make_unique<partition_replicator>(
          _source, model::term_id{0}, std::move(source), std::move(sink));
        _replicator->start().get();
    }

    model::record_batch generate_random_batch(int64_t start_offset) {
        auto num_records = random_generators::get_int(25, 50);
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset{0});

        for (int i = 0; i < num_records; ++i) {
            builder.add_raw_kv(
              serde::to_iobuf(kafka::offset{start_offset++}), std::nullopt);
        }
        return std::move(builder).build();
    }

    void TearDown() override {
        if (_replicator) {
            _replicator->stop().get();
        }
        if (_mux_consumer) {
            _mux_consumer->stop().get();
        }
        basic_consumer_fixture::TearDown();
    }

    // Do nothing, handled by mux consumer
    void StartConsumer() override {}
    void StopConsumer() override {}

protected:
    std::unique_ptr<mux_remote_consumer> _mux_consumer;
    std::unique_ptr<partition_replicator> _replicator;
    model::ntp _source{model::kafka_namespace, "source", 0};
    model::ntp _target{model::kafka_namespace, "target", 0};
};

TEST_P(ReplicatorFixture, TestProduceConsume) {
    auto deadline = ss::lowres_clock::now() + 10s;
    int64_t next_offset = 0;
    while (ss::lowres_clock::now() < deadline) {
        auto batch = generate_random_batch(next_offset);
        auto res
          = produce_to_partition(_source.tp.topic, 0, std::move(batch)).get();
        next_offset = res() + 1;
        auto sleep_for = random_generators::get_int(1, 5);
        ss::sleep(std::chrono::milliseconds(sleep_for)).get();
    }
    auto produced = next_offset;
    // wait until the target is caught up.
    RPTEST_REQUIRE_EVENTUALLY(
      10s, [this, offset = kafka::offset{produced - 1}] {
          auto [_, partition] = get_leader(_target);
          vassert(partition, "No leader found for {}", _target);
          auto raft = partition->raft();
          auto max_kafka_offset = model::offset_cast(
            raft->log()->from_log_offset(raft->committed_offset()));
          vlog(
            logger.debug,
            "Expected offset: {}, current: {}",
            offset,
            max_kafka_offset);
          return max_kafka_offset == offset;
      });

    // validate offsets from the target topic
    auto max_offset = kafka::offset(next_offset - 1);
    kafka::offset next_to_consume{0};
    while (next_to_consume <= max_offset) {
        auto records = consume_from_partition(
                         _target.tp.topic,
                         _target.tp.partition,
                         next_to_consume)
                         .get();
        ASSERT_FALSE(records.empty())
          << "Expected records to be non-empty, last_consumed: "
          << kafka::prev_offset(next_to_consume)
          << ", max_offset: " << max_offset;
        for (auto& record : records) {
            auto offset = serde::from_iobuf<kafka::offset>(
              record.release_key());
            EXPECT_EQ(next_to_consume, offset);
            next_to_consume = kafka::next_offset(next_to_consume);
        }
    }
}

using session_config = kafka::client::tests::session_config;
INSTANTIATE_TEST_SUITE_P(
  ReplicatorFixtureAndSessions,
  ReplicatorFixture,
  testing::Values(
    session_config::with_sessions,
    session_config::without_sessions,
    session_config::toggle_sessions));
