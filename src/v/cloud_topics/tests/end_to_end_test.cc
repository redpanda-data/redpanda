/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "features/feature_table.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/server/tests/delete_records_utils.h"
#include "kafka/server/tests/list_offsets_utils.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/batch_builder.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "redpanda/tests/fixture.h"
#include "ssx/sformat.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>

#include <gtest/gtest.h>

using tests::kafka_consume_transport;
using tests::kafka_produce_transport;
using tests::kv_t;

static ss::logger e2e_test_log("e2e_test");

class e2e_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public ::testing::Test {
public:
    e2e_fixture()
      : redpanda_thread_fixture(init_cloud_topics_tag{}, httpd_port_number()) {
        // No expectations: tests will PUT and GET organically.
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
    }

    void SetUp() override {
        cluster::topic_properties props;
        props.storage_mode = model::redpanda_storage_mode::cloud;
        props.shadow_indexing = model::shadow_indexing_mode::disabled;
        add_topic({model::kafka_namespace, topic_name}, 1, props).get();
        wait_for_leader(ntp).get();
    }

    void TearDown() override {
        for (auto& fn : std::views::reverse(cleanup)) {
            fn();
        }
    }

    kafka_produce_transport* make_producer() {
        auto producer = std::make_unique<kafka_produce_transport>(
          make_kafka_client().get());
        producer->start().get();
        auto* p = producer.get();
        cleanup.emplace_back([p = std::move(producer)] { p->stop().get(); });
        return p;
    }

    kafka_consume_transport* make_consumer() {
        auto consumer = std::make_unique<kafka_consume_transport>(
          make_kafka_client().get());
        consumer->start().get();
        auto* c = consumer.get();
        cleanup.emplace_back([c = std::move(consumer)] { c->stop().get(); });
        return c;
    }

    tests::kafka_list_offsets_transport* make_list_offsets_client() {
        auto transport = std::make_unique<tests::kafka_list_offsets_transport>(
          make_kafka_client().get());
        transport->start().get();
        auto* t = transport.get();
        cleanup.emplace_back([t = std::move(transport)] { t->stop().get(); });
        return t;
    }

    tests::kafka_delete_records_transport* make_delete_records_client() {
        auto transport
          = std::make_unique<tests::kafka_delete_records_transport>(
            make_kafka_client().get());
        transport->start().get();
        auto* t = transport.get();
        cleanup.emplace_back([t = std::move(transport)] { t->stop().get(); });
        return t;
    }

    std::vector<ss::noncopyable_function<void()>> cleanup;
    scoped_config test_local_cfg;
    const model::topic topic_name{"tapioca"};
    model::ntp ntp{model::kafka_namespace, topic_name, 0};
};

TEST_F(e2e_fixture, test_create_cloud_topic) {
    auto partition = app.partition_manager.local().get(ntp);
    ASSERT_TRUE(
      partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>()
      != nullptr);
}

// A non-idempotent client (no producer id) with max.in.flight > 1 may have
// two produce requests for the same partition on the wire at once: the kafka
// connection only waits for a request's `dispatched` stage before reading the
// next request. Kafka still requires that the records of that one producer be
// appended in send order.
//
// Two produce requests are pipelined on one connection here (the test client
// serializes only the wire write, not the response), each carrying one
// identifiable record.
TEST_F(e2e_fixture, pipelined_non_idempotent_produce_preserves_order) {
    test_local_cfg.get("cloud_topics_disable_reconciliation_loop")
      .set_value(true);

    auto* producer = make_producer();

    std::vector<kv_t> first{{"k0", "val-sent-first"}};
    std::vector<kv_t> second{{"k1", "val-sent-second"}};

    auto fut1 = producer->produce_to_partition(
      topic_name, model::partition_id(0), first);
    auto fut2 = producer->produce_to_partition(
      topic_name, model::partition_id(0), second);
    auto offsets = ss::when_all_succeed(std::move(fut1), std::move(fut2)).get();
    auto off_first = std::get<0>(offsets);
    auto off_second = std::get<1>(offsets);

    EXPECT_LT(off_first, off_second)
      << "the record sent first must get the lower offset, got first="
      << off_first << " second=" << off_second;

    auto* consumer = make_consumer();
    auto consumed = consumer
                      ->consume_from_partition(
                        topic_name, model::partition_id(0), model::offset(0))
                      .get();
    ASSERT_EQ(consumed.size(), 2u);
    EXPECT_EQ(consumed[0].val, "val-sent-first")
      << "consumer saw '" << consumed[0].val.value_or("<none>")
      << "' at offset 0";
    EXPECT_EQ(consumed[1].val, "val-sent-second");
}

// Control for the test above on a plain (storage.mode=local) topic: the same
// client, the same pipelining, no cloud topics write path.
TEST_F(e2e_fixture, pipelined_non_idempotent_produce_preserves_order_local) {
    const model::topic local_topic{"local_control"};
    cluster::topic_properties props;
    props.storage_mode = model::redpanda_storage_mode::local;
    add_topic({model::kafka_namespace, local_topic}, 1, props).get();
    wait_for_leader(model::ntp{model::kafka_namespace, local_topic, 0}).get();

    auto* producer = make_producer();

    std::vector<kv_t> first{{"k0", "val-sent-first"}};
    std::vector<kv_t> second{{"k1", "val-sent-second"}};

    auto fut1 = producer->produce_to_partition(
      local_topic, model::partition_id(0), first);
    auto fut2 = producer->produce_to_partition(
      local_topic, model::partition_id(0), second);
    auto offsets = ss::when_all_succeed(std::move(fut1), std::move(fut2)).get();
    auto off_first = std::get<0>(offsets);
    auto off_second = std::get<1>(offsets);

    EXPECT_LT(off_first, off_second)
      << "the record sent first must get the lower offset, got first="
      << off_first << " second=" << off_second;

    auto* consumer = make_consumer();
    auto consumed = consumer
                      ->consume_from_partition(
                        local_topic, model::partition_id(0), model::offset(0))
                      .get();
    ASSERT_EQ(consumed.size(), 2u);
    EXPECT_EQ(consumed[0].val, "val-sent-first");
    EXPECT_EQ(consumed[1].val, "val-sent-second");
}

TEST_F(e2e_fixture, test_l0_path) {
    // Disable reconciliation to ensure we test the L0 path exclusively.
    test_local_cfg.get("cloud_topics_disable_reconciliation_loop")
      .set_value(true);

    auto* producer = make_producer();
    size_t total_records = 100;
    size_t records_per_batch = 5;
    std::vector<kv_t> records;
    for (size_t i = 0; i < total_records; i += records_per_batch) {
        std::vector<kv_t> batch;
        for (size_t j = 0; j < records_per_batch; j++) {
            records.emplace_back(
              ssx::sformat("key{}", i + j), ssx::sformat("val{}", i + j));
            batch.push_back(records.back());
        }
        producer
          ->produce_to_partition(topic_name, model::partition_id(0), batch)
          .get();
    }

    auto consumer = make_consumer();
    for (auto [seek_offset, start_offset] :
         std::map<int, int>{{0, 0}, {1, 0}, {5, 5}, {6, 5}, {99, 95}}) {
        auto consumed_records = consumer
                                  ->consume_from_partition(
                                    topic_name,
                                    model::partition_id(0),
                                    model::offset(seek_offset))
                                  .get();
        ASSERT_EQ(consumed_records.size(), records.size() - start_offset);
        for (const auto& [expected_offset, consumed] : std::views::zip(
               std::views::iota(start_offset), consumed_records)) {
            ASSERT_EQ(records[expected_offset].key, consumed.key);
            ASSERT_EQ(records[expected_offset].val, consumed.val);
        }
    }
}

TEST_F(e2e_fixture, timequery) {
    lconf().log_message_timestamp_after_max_ms.set_value(
      serde::max_serializable_ms);
    auto unset_cluster_config = ss::defer([&] {
        lconf().log_message_timestamp_after_max_ms.set_value(
          lconf().log_message_timestamp_after_max_ms.default_value());
    });

    // Units we use in this test for relative timestamps is *hours* as to make
    // sure the slow uploading into cloud topics doesn't effect the results.
    constexpr int64_t hour_in_milli = 60L * 60 * 1000;
    struct timequery_batch_spec {
        std::vector<int64_t> relative_timestamps;
        bool broker_time = false; // append time or create time?
    };

    std::vector<timequery_batch_spec> data_spec{
      {.relative_timestamps = {-60, -50, -40, -30}},
      {.relative_timestamps = {-25, -24, -25, -25}},
      {.relative_timestamps = {-29, -28, -27}},
      {.relative_timestamps = {-19, -20, -19, -20, -19}},
      // For broker time, we should be ignoring the timestamp deltas
      {.relative_timestamps = {0, 1, 2, 3, 5}, .broker_time = true},
      {.relative_timestamps = {16, 17, 17, 18, 16, 20}},
      {.relative_timestamps = {22, 23, 24}},
      {.relative_timestamps = {25, 26, 27}},
    };
    model::timestamp now = model::timestamp::now();
    auto* producer = make_producer();
    for (const auto& spec : data_spec) {
        model::batch_builder builder;
        for (auto rel_ts : spec.relative_timestamps) {
            builder.add_record(
              model::record(/*attributes=*/{},
                            /*timestamp_delta=*/rel_ts * hour_in_milli,
                            /*offset_delta=*/builder.num_records(),
                            /*key=*/std::nullopt,
                            /*value=*/std::nullopt,
                            /*hdrs=*/{}));
        }
        auto timestamp_type = spec.broker_time
                                ? model::timestamp_type::append_time
                                : model::timestamp_type::create_time;
        builder.set_batch_timestamp(timestamp_type, now);
        // Set the timestamp type for the topic (well cluster) so that we can
        // toggle between the different types of append vs create time.
        lconf().log_message_timestamp_type.set_value(timestamp_type);
        auto unset_cluster_config = ss::defer([&] {
            lconf().log_message_timestamp_type.set_value(
              lconf().log_message_timestamp_type.default_value());
        });
        producer->produce_to_partition(ntp, builder.build_sync()).get();
    }
    struct timequery_test_spec {
        int64_t relative_timestamp;
        int64_t expected_offset;
    };
    std::vector<timequery_test_spec> timequery_tests{
      {.relative_timestamp = -70, .expected_offset = 0},
      {.relative_timestamp = -60, .expected_offset = 0},
      {.relative_timestamp = -55, .expected_offset = 1},
      {.relative_timestamp = -50, .expected_offset = 1},
      {.relative_timestamp = -25, .expected_offset = 4},
      {.relative_timestamp = -29, .expected_offset = 4},
      {.relative_timestamp = 0, .expected_offset = 16},
      {.relative_timestamp = 1, .expected_offset = 21},
      {.relative_timestamp = 16, .expected_offset = 21},
      {.relative_timestamp = 17, .expected_offset = 22},
      {.relative_timestamp = 18, .expected_offset = 24},
      {.relative_timestamp = 19, .expected_offset = 26},
      {.relative_timestamp = 20, .expected_offset = 26},
      {.relative_timestamp = 21, .expected_offset = 27},
      {.relative_timestamp = 22, .expected_offset = 27},
      {.relative_timestamp = 23, .expected_offset = 28},
      {.relative_timestamp = 28, .expected_offset = -1},
    };
    auto* client = make_list_offsets_client();
    for (const auto& testcase : timequery_tests) {
        model::timestamp ts{
          now() + (testcase.relative_timestamp * hour_in_milli)};
        auto offset = client->timequery(ntp.tp, ts).get();
        EXPECT_EQ(offset, testcase.expected_offset)
          << "for timequery at relative timestamp: "
          << testcase.relative_timestamp;
    }
    auto partition = app.partition_manager.local().get(ntp);
    auto state = partition->get_cloud_topics_state();
    ASSERT_NE(state, nullptr);
    auto topic_id = partition->get_topic_config()->get().tp_id;
    ASSERT_NE(topic_id, std::nullopt);
    RPTEST_REQUIRE_EVENTUALLY(30s, [this, state, topic_id]() {
        // Expect eventually we don't get a missing ntp error.
        return state->local()
          .get_l1_metastore()
          ->get_offsets({*topic_id, ntp.tp.partition})
          .then([](auto result) { return result.has_value(); });
    });
    // Retry now that the data is in L1
    for (const auto& testcase : timequery_tests) {
        auto offset = client
                        ->timequery(
                          ntp.tp,
                          model::timestamp{
                            now()
                            + (testcase.relative_timestamp * hour_in_milli)})
                        .get();
        EXPECT_EQ(offset, testcase.expected_offset)
          << "for L1 timequery at relative timestamp: "
          << testcase.relative_timestamp;
    }
}

// Regression test for a race condition in the cloud topics write path.
//
// In the write path, data is first uploaded to S3, then the placeholder
// batch is replicated via raft (advancing the HWM), and only after that
// the materialized batch is inserted into the record batch cache. A tailing
// consumer whose fetch is waiting at the partition tip can observe the new
// HWM, read the placeholder, find the batch cache empty (cache_put hasn't
// run yet), and fall through to an expensive S3 download.
//
// This test runs a producer and a tailing consumer concurrently. S3
// GetObject requests are configured to fail so that any cache miss during
// consumption is observable. After the test we assert that no GetObject
// requests were attempted.
TEST_F(e2e_fixture, test_tailing_consumer_no_l0_downloads) {
    // Disable reconciliation to ensure we only exercise the L0 path.
    test_local_cfg.get("cloud_topics_disable_reconciliation_loop")
      .set_value(true);

    auto* producer = make_producer();
    auto* consumer = make_consumer();

    const size_t num_batches = 100;
    const size_t total_records = num_batches;

    // Producer: produce single-record batches one at a time so that the
    // consumer has many opportunities to observe the cache-miss window.
    auto produce_fut = ss::async([&] {
        for (size_t i = 0; i < num_batches; i++) {
            std::vector<kv_t> batch = {
              {ssx::sformat("key{}", i), ssx::sformat("val{}", i)}};
            producer
              ->produce_to_partition(topic_name, model::partition_id(0), batch)
              .get();
        }
    });

    // Consumer: tail the partition — always fetch from the latest consumed
    // offset. The fetch uses max_wait_ms=1000ms internally, so it blocks
    // until new data arrives or the timeout expires. This creates the
    // tailing workload where the consumer is waiting right at the tip
    // when the producer advances the HWM.
    size_t total_consumed = 0;
    auto consume_fut = ss::async([&] {
        model::offset next_offset{0};
        while (total_consumed < total_records) {
            auto records = consumer
                             ->consume_from_partition(
                               topic_name, model::partition_id(0), next_offset)
                             .get();
            total_consumed += records.size();
            if (!records.empty()) {
                next_offset = model::offset(
                  next_offset() + static_cast<int64_t>(records.size()));
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

    ASSERT_EQ(total_consumed, total_records);

    // Verify no S3 GetObject requests were made. Any such request means
    // the consumer hit a cache miss and attempted an S3 download, which
    // indicates the race between replicate() and cache_put().
    auto s3_get_requests = get_requests(
      [](const http_test_utils::request_info& req) {
          return req.method == "GET" && req.q_list_type.empty();
      });
    ASSERT_EQ(s3_get_requests.size(), 0)
      << "Detected " << s3_get_requests.size()
      << " unexpected S3 GetObject request(s) during tailing consume. "
         "This indicates a race between replicate() and cache_put() "
         "in the cloud topics write path.";
}

// L1RT-2: a ListOffsets-by-timestamp answer must never be below the
// partition's Kafka start offset. Kafka guarantees the offset returned by
// offsetsForTimes is fetchable; if it is below the start offset the client's
// own fetch is rejected with OFFSET_OUT_OF_RANGE and auto.offset.reset fires.
//
// Setup: 20 single-record batches reconciled into a single L1 extent
// [0, 20), then DeleteRecords(10). The surviving log is [10, 20). A query for
// a timestamp older than every record must answer 10, not 0.
TEST_F(e2e_fixture, timequery_after_delete_records_respects_start_offset_l1) {
    // Hold reconciliation off while producing so that all of the records land
    // in a single L1 extent -- DeleteRecords must fall strictly inside an
    // extent, otherwise the metastore prunes the whole extent and the
    // straddling case is never exercised.
    test_local_cfg.get("cloud_topics_disable_reconciliation_loop")
      .set_value(true);

    constexpr int num_records = 20;
    constexpr int delete_up_to = 10;

    auto now = model::timestamp::now();
    // Every record is in the past, oldest first: record i has
    // ts = now - (num_records - i) seconds.
    auto ts_for = [now](int i) {
        return model::timestamp{now() - (num_records - i) * 1000};
    };

    auto* producer = make_producer();
    for (int i = 0; i < num_records; ++i) {
        producer
          ->produce_to_partition(
            topic_name,
            model::partition_id(0),
            std::vector<kv_t>{{ssx::sformat("k{}", i), ssx::sformat("v{}", i)}},
            ts_for(i))
          .get();
    }

    // Let a single reconciliation round move everything into L1.
    test_local_cfg.get("cloud_topics_disable_reconciliation_loop")
      .set_value(false);

    auto partition = app.partition_manager.local().get(ntp);
    ASSERT_NE(partition, nullptr);
    auto state = partition->get_cloud_topics_state();
    ASSERT_NE(state, nullptr);
    auto topic_id = partition->get_topic_config()->get().tp_id;
    ASSERT_NE(topic_id, std::nullopt);
    model::topic_id_partition tidp{*topic_id, ntp.tp.partition};

    auto* metastore = state->local().get_l1_metastore();
    RPTEST_REQUIRE_EVENTUALLY(30s, [metastore, tidp]() {
        return metastore->get_offsets(tidp).then([](auto res) {
            return res.has_value()
                   && res.value().next_offset == kafka::offset(num_records);
        });
    });

    auto* deleter = make_delete_records_client();
    auto lwm = deleter
                 ->delete_records_from_partition(
                   topic_name,
                   model::partition_id(0),
                   model::offset(delete_up_to),
                   std::chrono::seconds(10))
                 .get();
    ASSERT_EQ(lwm, model::offset(delete_up_to));

    auto* client = make_list_offsets_client();
    auto start_offset
      = client->start_offset_for_partition(topic_name, model::partition_id(0))
          .get();
    ASSERT_EQ(start_offset, model::offset(delete_up_to));

    // Precondition for the finding: the coarse L1 candidate the frontend uses
    // still starts below the Kafka start offset (the extent straddles it).
    auto coarse = metastore
                    ->get_first_ge(
                      tidp,
                      kafka::offset(delete_up_to),
                      model::timestamp{now() - (num_records + 10) * 1000})
                    .get();
    ASSERT_TRUE(coarse.has_value());
    e2e_test_log.info(
      "coarse L1 candidate for timequery: [{}, {}], kafka start offset {}",
      coarse.value().first_offset,
      coarse.value().last_offset,
      start_offset);

    // A timestamp older than every record. The oldest *surviving* record is at
    // offset 10, so that is the only Kafka-correct answer.
    auto query_ts = model::timestamp{now() - (num_records + 10) * 1000};
    auto answer = client->timequery(ntp.tp, query_ts).get();
    e2e_test_log.info(
      "timequery({}) answered {} (kafka start offset {})",
      query_ts,
      answer,
      start_offset);
    EXPECT_GE(answer, kafka::offset(delete_up_to))
      << "timequery answered " << answer << " which is below the Kafka start "
      << "offset " << start_offset << "; a client seeking there gets "
      << "OFFSET_OUT_OF_RANGE";
    EXPECT_EQ(answer, kafka::offset(delete_up_to));

    // Kafka's contract for offsetsForTimes: the returned offset is fetchable.
    auto proxy = kafka::make_partition_proxy(partition);
    auto fetch_ec = proxy
                      .validate_fetch_offset(
                        model::offset(answer()),
                        false,
                        model::timeout_clock::now() + 30s)
                      .get();
    EXPECT_EQ(fetch_ec, kafka::error_code::none)
      << "a fetch at the offset timequery answered (" << answer
      << ") was rejected with " << fetch_ec;
}

// L1RT-2, L0 branch: same invariant, but the coarse candidate comes from the
// local log. DeleteRecords lands in the middle of a placeholder batch, so the
// local reader hands refine_timequery_result a batch whose base offset is
// below the Kafka start offset.
TEST_F(e2e_fixture, timequery_after_delete_records_respects_start_offset_l0) {
    // Keep everything in L0 so l1_timequery finds nothing and the L0 candidate
    // is the one that gets refined.
    test_local_cfg.get("cloud_topics_disable_reconciliation_loop")
      .set_value(true);

    constexpr int records_per_batch = 5;
    constexpr int num_batches = 4;
    constexpr int num_records = records_per_batch * num_batches;
    // Strictly inside the first batch [0, 4].
    constexpr int delete_up_to = 3;

    auto now = model::timestamp::now();
    auto* producer = make_producer();
    for (int b = 0; b < num_batches; ++b) {
        std::vector<kv_t> records;
        for (int i = 0; i < records_per_batch; ++i) {
            int n = b * records_per_batch + i;
            records.emplace_back(
              ssx::sformat("k{}", n), ssx::sformat("v{}", n));
        }
        producer
          ->produce_to_partition(
            topic_name,
            model::partition_id(0),
            std::move(records),
            model::timestamp{now() - (num_batches - b) * 1000})
          .get();
    }

    auto* deleter = make_delete_records_client();
    auto lwm = deleter
                 ->delete_records_from_partition(
                   topic_name,
                   model::partition_id(0),
                   model::offset(delete_up_to),
                   std::chrono::seconds(10))
                 .get();
    ASSERT_EQ(lwm, model::offset(delete_up_to));

    auto* client = make_list_offsets_client();
    auto start_offset
      = client->start_offset_for_partition(topic_name, model::partition_id(0))
          .get();
    ASSERT_EQ(start_offset, model::offset(delete_up_to));
    auto hwm
      = client->high_watermark_for_partition(topic_name, model::partition_id(0))
          .get();
    ASSERT_EQ(hwm, model::offset(num_records));

    auto query_ts = model::timestamp{now() - (num_batches + 10) * 1000};
    auto answer = client->timequery(ntp.tp, query_ts).get();
    e2e_test_log.info(
      "L0 timequery({}) answered {} (kafka start offset {})",
      query_ts,
      answer,
      start_offset);
    EXPECT_GE(answer, kafka::offset(delete_up_to))
      << "timequery answered " << answer << " which is below the Kafka start "
      << "offset " << start_offset;
    EXPECT_EQ(answer, kafka::offset(delete_up_to));
}

// Validation test for audit finding guard-asymmetry:GA-1.
//
// The postcondition the reconciler's commit path states for itself
// (reconciliation_source.cc:101-127) is that after a successful round the
// local-read floor covers the range just reconciled:
//   min_allowed_local_threshold == min(next(LRO), next(placeholder_hwm))
// Every offset in this topic is placeholder-backed (storage.mode=cloud), so
// next(placeholder_hwm) == next(LRO) and the floor must equal next(LRO).
//
// If the floor lags the LRO, offsets in the gap are still routed to the local
// log after a flip to storage.mode=tiered_cloud, where their placeholders
// point at L0 objects that L0 GC is free to delete.
TEST_F(e2e_fixture, reconciliation_advances_local_read_floor) {
    ASSERT_TRUE(app.controller->get_feature_table().local().is_active(
      features::feature::tiered_cloud_topics))
      << "tiered_cloud_topics must be active for the reconciler to report a "
         "placeholder observation at all";

    auto* producer = make_producer();
    const size_t num_batches = 20;
    const size_t records_per_batch = 5;
    for (size_t i = 0; i < num_batches; ++i) {
        std::vector<kv_t> batch;
        for (size_t j = 0; j < records_per_batch; ++j) {
            batch.emplace_back(
              ssx::sformat("key{}", i * records_per_batch + j),
              ssx::sformat("val{}", i * records_per_batch + j));
        }
        producer
          ->produce_to_partition(topic_name, model::partition_id(0), batch)
          .get();
    }
    const auto total_records = static_cast<int64_t>(
      num_batches * records_per_batch);

    auto partition = app.partition_manager.local().get(ntp);
    ASSERT_NE(partition, nullptr);
    auto stm = partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    ASSERT_TRUE(stm != nullptr);
    cloud_topics::ctp_stm_api api(stm);

    // Wait for the reconciler to commit an LRO covering everything produced.
    RPTEST_REQUIRE_EVENTUALLY(60s, [&] {
        auto lro = api.get_last_reconciled_offset();
        vlog(
          e2e_test_log.info,
          "LRO {}, floor {}",
          lro,
          api.get_min_allowed_local_threshold());
        return lro >= kafka::offset(total_records - 1);
    });

    auto lro = api.get_last_reconciled_offset();
    auto floor = api.get_min_allowed_local_threshold();
    vlog(
      e2e_test_log.info,
      "GA-1: after reconciliation LRO={} min_allowed_local_threshold={} "
      "(expected {})",
      lro,
      floor,
      kafka::next_offset(lro));
    EXPECT_EQ(floor, kafka::next_offset(lro)) << fmt::format(
      "local-read floor {} does not cover the reconciled range [0, {}]; "
      "offsets ({}, {}] would still be routed to local placeholders",
      floor,
      lro,
      floor,
      lro);
}

// Control for GA-1: with the batch cache disabled the reconciler is forced
// down the fetch_metadata path, which is the only path that records the
// placeholder observation. If this passes while the test above fails, the
// batch-cache fast path is the cause.
TEST_F(e2e_fixture, reconciliation_advances_local_read_floor_no_batch_cache) {
    test_local_cfg.get("disable_batch_cache").set_value(true);

    auto* producer = make_producer();
    const size_t num_batches = 20;
    const size_t records_per_batch = 5;
    for (size_t i = 0; i < num_batches; ++i) {
        std::vector<kv_t> batch;
        for (size_t j = 0; j < records_per_batch; ++j) {
            batch.emplace_back(
              ssx::sformat("key{}", i * records_per_batch + j),
              ssx::sformat("val{}", i * records_per_batch + j));
        }
        producer
          ->produce_to_partition(topic_name, model::partition_id(0), batch)
          .get();
    }
    const auto total_records = static_cast<int64_t>(
      num_batches * records_per_batch);

    auto partition = app.partition_manager.local().get(ntp);
    ASSERT_NE(partition, nullptr);
    auto stm = partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    ASSERT_TRUE(stm != nullptr);
    cloud_topics::ctp_stm_api api(stm);

    RPTEST_REQUIRE_EVENTUALLY(60s, [&] {
        return api.get_last_reconciled_offset()
               >= kafka::offset(total_records - 1);
    });

    auto lro = api.get_last_reconciled_offset();
    auto floor = api.get_min_allowed_local_threshold();
    vlog(
      e2e_test_log.info,
      "GA-1 control (no batch cache): LRO={} min_allowed_local_threshold={}",
      lro,
      floor);
    EXPECT_EQ(floor, kafka::next_offset(lro));
}

// Severity probe for GA-1: is the missed floor advance a permanently skipped
// range, or does it heal on the next round that misses the batch cache?
//
// The floor is a single monotone watermark, so a later log-served round could
// in principle jump it past the earlier gap. This test creates a gap with the
// cache on, then turns the cache off, produces more, and records where the
// floor ends up.
TEST_F(e2e_fixture, local_read_floor_gap_heals_on_cache_miss_round) {
    auto produce_batches = [&](size_t n, size_t base) {
        auto* producer = make_producer();
        for (size_t i = 0; i < n; ++i) {
            std::vector<kv_t> batch;
            for (size_t j = 0; j < 5; ++j) {
                batch.emplace_back(
                  ssx::sformat("key{}", base + i * 5 + j),
                  ssx::sformat("val{}", base + i * 5 + j));
            }
            producer
              ->produce_to_partition(topic_name, model::partition_id(0), batch)
              .get();
        }
    };

    auto partition = app.partition_manager.local().get(ntp);
    ASSERT_TRUE(partition != nullptr);
    auto stm = partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    ASSERT_TRUE(stm != nullptr);
    cloud_topics::ctp_stm_api api(stm);

    // Phase 1: batch cache on (default) -> reconciler takes the fast path.
    produce_batches(20, 0);
    RPTEST_REQUIRE_EVENTUALLY(60s, [&] {
        return api.get_last_reconciled_offset() >= kafka::offset(99);
    });
    auto lro1 = api.get_last_reconciled_offset();
    auto floor1 = api.get_min_allowed_local_threshold();
    vlog(
      e2e_test_log.info,
      "GA-1 phase 1 (cache on): LRO={} floor={}",
      lro1,
      floor1);

    // Phase 2: batch cache off -> reconciler must go through fetch_metadata.
    test_local_cfg.get("disable_batch_cache").set_value(true);
    produce_batches(20, 100);
    RPTEST_REQUIRE_EVENTUALLY(60s, [&] {
        return api.get_last_reconciled_offset() >= kafka::offset(199);
    });
    auto lro2 = api.get_last_reconciled_offset();
    auto floor2 = api.get_min_allowed_local_threshold();
    vlog(
      e2e_test_log.info,
      "GA-1 phase 2 (cache off): LRO={} floor={}",
      lro2,
      floor2);

    // Whichever way this goes it is informative: if floor2 == next(lro2) the
    // gap healed and GA-1 is a transient lag; if it only covers the second
    // range the gap is permanent.
    EXPECT_EQ(floor2, kafka::next_offset(lro2)) << fmt::format(
      "phase-1 floor {} (LRO {}), phase-2 floor {} (LRO {})",
      floor1,
      lro1,
      floor2,
      lro2);
}
