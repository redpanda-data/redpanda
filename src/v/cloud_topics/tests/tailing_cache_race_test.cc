/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vlog.h"
#include "cloud_topics/tests/cluster_fixture.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "ssx/sformat.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"
#include "utils/offset_monitor.h"

#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>

#include <gtest/gtest.h>

using tests::kv_t;

namespace {

ss::logger test_log("tailing_cache_race_test");
const model::topic topic_name{"tapioca"};
const model::ntp ntp{model::kafka_namespace, topic_name, 0};

} // namespace

class tailing_cache_race_fixture
  : public cloud_topics::cluster_fixture
  , public ::testing::Test {
public:
    void SetUp() override {
        cfg.get("enable_leader_balancer").set_value(false);
        cfg.get("cloud_topics_disable_reconciliation_loop").set_value(true);
        cfg.get("raft_heartbeat_interval_ms").set_value(50ms);
        cfg.get("raft_heartbeat_timeout_ms").set_value(500ms);
        for (int i = 0; i < 3; ++i) {
            add_node();
        }
        wait_for_all_members(5s).get();

        cluster::topic_properties props;
        props.storage_mode = model::redpanda_storage_mode::cloud;
        props.shadow_indexing = model::shadow_indexing_mode::disabled;

        create_topic({model::kafka_namespace, topic_name}, 1, 3, props).get();
    }

    ss::future<ss::lw_shared_ptr<cluster::partition>> wait_for_leadership() {
        ss::lw_shared_ptr<cluster::partition> p;
        co_await tests::cooperative_spin_wait_with_timeout(10s, [&] {
            auto [leader_fx, leader_p] = get_leader(ntp);
            if (!leader_fx) {
                return false;
            }
            p = leader_p;
            return true;
        });
        co_return p;
    }

    /// Produce initial records, step down leader, wait for new leader.
    /// Returns the new leader partition and populates the out-params.
    ss::future<ss::lw_shared_ptr<cluster::partition>> setup_offset_delta(
      model::offset& raft_committed_out,
      model::offset& kafka_committed_out,
      kafka::offset& last_initial_offset_out) {
        // Phase 1: produce initial data.
        auto initial_leader = co_await wait_for_leadership();
        auto initial_leader_id = initial_leader->raft()->self().id();
        auto* initial_producer = co_await make_producer(initial_leader_id);

        constexpr size_t initial_records = 10;
        kafka::offset last_initial_offset{};
        for (size_t i = 0; i < initial_records; ++i) {
            std::vector<kv_t> batch = {
              {ssx::sformat("init_key{}", i), ssx::sformat("init_val{}", i)}};
            auto offset = co_await initial_producer->produce_to_partition(
              topic_name, model::partition_id(0), batch);
            last_initial_offset = kafka::offset{offset()};
        }
        vlog(
          test_log.info,
          "Produced {} initial records, last offset: {}",
          initial_records,
          last_initial_offset);

        // Phase 2: step down to create a raft configuration batch.
        auto leader_raft = initial_leader->raft();
        co_await leader_raft->step_down("create-config-batch");
        vlog(test_log.info, "Leader stepped down to create config batch");

        // Phase 3: wait for a new leader to stabilize.
        auto new_leader = co_await wait_for_leadership();
        auto new_leader_id = new_leader->raft()->self().id();
        vlog(
          test_log.info,
          "New leader is node {}, term {}",
          new_leader_id,
          new_leader->raft()->term());

        // Verify offset delta is non-zero.
        auto raft_committed = new_leader->raft()->committed_offset();
        auto kafka_committed = new_leader->from_log_offset(raft_committed);
        vlog(
          test_log.info,
          "Raft committed: {}, kafka committed: {}, delta: {}",
          raft_committed,
          kafka_committed,
          raft_committed() - kafka_committed());

        raft_committed_out = raft_committed;
        kafka_committed_out = kafka_committed;
        last_initial_offset_out = last_initial_offset;
        co_return new_leader;
    }

protected:
    scoped_config cfg;
};

// Verify that a non-zero offset delta (D > 0) after a leader election
// causes the batch_cache offset_monitor to be bypassed when seeded with
// a raft-space committed_offset.
//
// The level_zero_reader seeds the monitor with:
//   prev_offset(committed_offset())   -- a raft-space value
// but the monitor tracks kafka-space offsets. With D > 0:
//   raft_committed - 1 >= kafka_committed >= next_kafka_offset
// so the wait resolves immediately, defeating the purpose of cache_wait.
//
// This test creates the conditions (D > 0 via step_down) and directly
// verifies the arithmetic: the raft-space seed would bypass the monitor,
// while a kafka-space seed would correctly block.
TEST_F(tailing_cache_race_fixture, test_monitor_seed_offset_space) {
    model::offset raft_committed;
    model::offset kafka_committed;
    kafka::offset last_initial_offset;
    auto new_leader = setup_offset_delta(
                        raft_committed, kafka_committed, last_initial_offset)
                        .get();

    ASSERT_GT(raft_committed(), kafka_committed())
      << "Expected non-zero offset delta after step_down, but raft_committed="
      << raft_committed << " kafka_committed=" << kafka_committed;

    // The next kafka offset to be produced/consumed:
    auto next_kafka = model::offset{kafka_committed()};

    // The buggy seed (what level_zero_reader.cc currently uses):
    //   prev_offset(committed_offset()) where committed_offset() is raft-space
    auto buggy_seed = model::prev_offset(raft_committed);

    // The correct seed (what the fix uses):
    //   prev_offset(from_log_offset(committed_offset()))
    auto correct_seed = model::prev_offset(kafka_committed);

    vlog(
      test_log.info,
      "next_kafka: {}, buggy_seed: {}, correct_seed: {}",
      next_kafka,
      buggy_seed,
      correct_seed);

    // Verify the raft-space seed bypasses the monitor.
    {
        offset_monitor<model::offset> mon;
        mon.notify(buggy_seed);
        auto f = mon.wait(next_kafka, model::no_timeout, std::nullopt);
        ASSERT_TRUE(f.available())
          << "Expected raft-space seed (" << buggy_seed
          << ") to bypass monitor for kafka target (" << next_kafka
          << "), D=" << (raft_committed() - kafka_committed());
        f.get();
    }

    // Verify the kafka-space seed correctly blocks.
    {
        offset_monitor<model::offset> mon;
        mon.notify(correct_seed);
        auto f = mon.wait(
          next_kafka, model::timeout_clock::now(), std::nullopt);
        ASSERT_FALSE(f.available())
          << "Expected kafka-space seed (" << correct_seed
          << ") to block for kafka target (" << next_kafka
          << "), D=" << (raft_committed() - kafka_committed());
        // Clean up: stop the monitor to cancel the pending waiter.
        mon.stop();
        ASSERT_THROW(f.get(), ss::abort_requested_exception);
    }
}

// End-to-end test: tailing produce + consume after a leader election.
// After step_down creates D > 0, run a tailing consumer concurrently with
// a producer. Verify no S3 GetObject requests occur during tailing — any
// such request indicates the consumer bypassed cache_wait and hit a cache
// miss.
TEST_F(tailing_cache_race_fixture, test_tailing_after_leader_election) {
    model::offset raft_committed;
    model::offset kafka_committed;
    kafka::offset last_initial_offset;
    auto new_leader = setup_offset_delta(
                        raft_committed, kafka_committed, last_initial_offset)
                        .get();

    ASSERT_GT(raft_committed(), kafka_committed())
      << "Expected non-zero offset delta after step_down, but raft_committed="
      << raft_committed << " kafka_committed=" << kafka_committed;

    auto new_leader_id = new_leader->raft()->self().id();

    // Record current S3 GET count before the tailing phase.
    auto gets_before
      = get_requests([](const http_test_utils::request_info& req) {
            return req.method == "GET" && req.q_list_type.empty();
        }).size();

    auto* producer = make_producer(new_leader_id).get();
    auto* consumer = make_consumer(new_leader_id).get();

    const size_t num_tailing_batches = 50;
    model::offset consumer_start{last_initial_offset() + 1};
    vlog(test_log.info, "Consumer starting at offset {}", consumer_start);

    size_t total_consumed = 0;
    auto produce_fut = ss::async([&] {
        for (size_t i = 0; i < num_tailing_batches; i++) {
            std::vector<kv_t> batch = {
              {ssx::sformat("key{}", i), ssx::sformat("val{}", i)}};
            producer
              ->produce_to_partition(topic_name, model::partition_id(0), batch)
              .get();
        }
    });

    auto consume_fut = ss::async([&] {
        model::offset next_offset = consumer_start;
        while (total_consumed < num_tailing_batches) {
            try {
                auto records = consumer
                                 ->consume_from_partition(
                                   topic_name,
                                   model::partition_id(0),
                                   next_offset)
                                 .get();
                total_consumed += records.size();
                if (!records.empty()) {
                    next_offset = model::offset(
                      next_offset() + static_cast<int64_t>(records.size()));
                }
            } catch (const std::runtime_error&) {
                // consume_from_partition throws on empty fetch; retry.
            }
        }
    });

    auto results
      = ss::when_all(std::move(produce_fut), std::move(consume_fut)).get();

    auto& produce_result = std::get<0>(results);
    auto& consume_result = std::get<1>(results);
    if (produce_result.failed()) {
        std::rethrow_exception(produce_result.get_exception());
    }
    if (consume_result.failed()) {
        std::rethrow_exception(consume_result.get_exception());
    }

    ASSERT_EQ(total_consumed, num_tailing_batches);

    // Verify no S3 GetObject requests during the tailing phase.
    auto gets_after = get_requests([](
                                     const http_test_utils::request_info& req) {
                          return req.method == "GET" && req.q_list_type.empty();
                      }).size();
    auto tailing_gets = gets_after - gets_before;
    ASSERT_EQ(tailing_gets, 0)
      << "Detected " << tailing_gets
      << " S3 GetObject request(s) during tailing consume after leader "
         "election. This indicates the cache_wait monitor was seeded with "
         "a raft-space offset instead of a kafka-space offset, causing the "
         "wait to resolve immediately when offset_delta > 0.";
}
