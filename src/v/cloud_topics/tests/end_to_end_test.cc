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
#include "cloud_topics/level_zero/stm/ctp_stm.h"
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
        props.cloud_topic_enabled = true;
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
