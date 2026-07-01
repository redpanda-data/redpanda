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
#include "cloud_topics/app.h"
#include "cloud_topics/batch_cache/batch_cache.h"
#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/level_one/prefetch/l1_fetch_service.h"
#include "cloud_topics/level_one/prefetch/prefetch_probe.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/state_accessors.h"
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

namespace cloud_topics {
// Friend accessor for batch_cache — allows test code to force-evict cache
// entries for a given tidp, making L1 reads deterministically hit the cloud
// instead of being served from the warm produce-path cache.
struct batch_cache_accessor {
    static void
    reclaim_all(batch_cache& c, const model::topic_id_partition& tidp) {
        auto it = c._entries.find(tidp);
        if (it != c._entries.end()) {
            it->second.index->testing_reclaim_from_cache(
              std::numeric_limits<size_t>::max());
        }
    }
};
} // namespace cloud_topics

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

// End-to-end proof that the LIVE L1 read path routes a consumer fetch through
// the l1_fetch_service (Task 9 wiring): produce -> reconcile into L1 -> consume
// from offset 0. The consume traverses frontend::make_reader's L1 branch, which
// now calls l1_fetch_service::get_reader via the state_accessors. We assert:
//   1. The scan returns every produced record in order with correct payloads.
//   2. The prefetch service was actually exercised (its probe shows a download
//      driven by this consume) — i.e. the read went through the new service,
//      not the removed l1_reader_cache.
TEST_F(e2e_fixture, test_l1_read_path_through_prefetch_service) {
    // Run with the batch cache ENABLED — the supported configuration. The L1
    // read path serves data exclusively through the shared batch cache, so a
    // disabled cache disables L1 reads by design (see
    // l1_fetch_service::get_reader). The produce path may pre-warm the cache,
    // but the prefetch service's producer still drives a real cloud download to
    // fill its prefetch window when get_reader creates a stream (it does not
    // short-circuit on already-cached offsets), so the downloads-counter
    // assertion below remains deterministic.
    auto* producer = make_producer();
    const size_t total_records = 100;
    const size_t records_per_batch = 5;
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

    auto partition = app.partition_manager.local().get(ntp);
    auto state = partition->get_cloud_topics_state();
    ASSERT_NE(state, nullptr);
    auto topic_id = partition->get_topic_config()->get().tp_id;
    ASSERT_NE(topic_id, std::nullopt);

    // Wait until reconciliation has pushed the produced data into L1.
    RPTEST_REQUIRE_EVENTUALLY(30s, [state, topic_id, this]() {
        return state->local()
          .get_l1_metastore()
          ->get_offsets({*topic_id, ntp.tp.partition})
          .then([](auto result) {
              return result.has_value()
                     && result.value().next_offset > kafka::offset{99};
          });
    });

    // Force-evict the batch cache for this partition so the L1 read cannot be
    // served from the produce-path warm entries. Without this, if the
    // reconciled offsets are still resident in the shared batch cache the
    // prefetch service never issues a cloud download, making the
    // downloads-counter assertion below flaky (0 vs 0 when the cache is warm).
    {
        model::topic_id_partition tidp{*topic_id, ntp.tp.partition};
        auto* bc = app.cloud_topics_app->get_state()
                     ->local()
                     .get_data_plane()
                     ->get_batch_cache();
        ASSERT_NE(bc, nullptr);
        cloud_topics::batch_cache_accessor::reclaim_all(*bc, tidp);
    }

    // Snapshot the prefetch service probe before the L1 read so the deltas
    // attribute unambiguously to this consume.
    auto* svc
      = app.cloud_topics_app->get_state()->local().get_l1_fetch_service();
    ASSERT_NE(svc, nullptr);
    const auto downloads_before = svc->probe().downloads();

    // Consume the whole partition from offset 0. This drives the L1 read path:
    // frontend::make_reader -> L1 branch -> l1_fetch_service::get_reader.
    auto consumer = make_consumer();
    auto consumed = consumer
                      ->consume_from_partition(
                        topic_name, model::partition_id(0), model::offset(0))
                      .get();

    ASSERT_EQ(consumed.size(), records.size());
    for (const auto& [expected, got] : std::views::zip(records, consumed)) {
        ASSERT_EQ(expected.key, got.key);
        ASSERT_EQ(expected.val, got.val);
    }

    // The L1 read must have gone through the prefetch service: with the
    // produce-path cache disabled, serving the consume required at least one
    // cloud download driven by l1_fetch_service. This is the live-path proof
    // that frontend::make_reader now routes L1 through the prefetch service.
    ASSERT_GT(svc->probe().downloads(), downloads_before)
      << "L1 consume did not drive any download through l1_fetch_service; the "
         "read path is not routed through the prefetch service";

    // The warm consecutive-fetch / no-metastore-requery behaviour is asserted
    // deterministically at the service level in l1_fetch_service_test's
    // warm_get_reader_reuses_stream (a single fetch RPC here may not split into
    // multiple get_reader calls, so cache-hit counting is not deterministic at
    // the e2e layer).
}
