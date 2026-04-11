/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "kafka/server/tests/delete_records_utils.h"
#include "kafka/server/tests/list_offsets_utils.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <seastar/util/log.hh>

#include <gtest/gtest.h>

using tests::kafka_consume_transport;
using tests::kafka_delete_records_transport;
using tests::kafka_list_offsets_transport;
using tests::kafka_produce_transport;
using tests::kv_t;

static ss::logger rr_test_log("read_replica_test");

class read_replica_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public ::testing::Test {
public:
    read_replica_fixture()
      : redpanda_thread_fixture(
          init_cloud_topics_tag{},
          httpd_port_number(),
          default_url_style,
          model::node_id(1),
          cloud_topics::test_fixture_cfg{
            .use_lsm_metastore = true, .skip_flush_loop = true}) {}

    void SetUp() override {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
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

    kafka_delete_records_transport* make_delete_records_client() {
        auto transport = std::make_unique<kafka_delete_records_transport>(
          make_kafka_client().get());
        transport->start().get();
        auto* t = transport.get();
        cleanup.emplace_back([t = std::move(transport)] { t->stop().get(); });
        return t;
    }

    kafka_consume_transport*
    make_consumer_for_rp(redpanda_thread_fixture* replica) {
        auto consumer = std::make_unique<kafka_consume_transport>(
          replica->make_kafka_client().get());
        consumer->start().get();
        auto* c = consumer.get();
        cleanup.emplace_back([c = std::move(consumer)] { c->stop().get(); });
        return c;
    }

    kafka_list_offsets_transport*
    make_list_offsets_for_rp(redpanda_thread_fixture* replica) {
        auto transport = std::make_unique<kafka_list_offsets_transport>(
          replica->make_kafka_client().get());
        transport->start().get();
        auto* t = transport.get();
        cleanup.emplace_back([t = std::move(transport)] { t->stop().get(); });
        return t;
    }

    std::unique_ptr<redpanda_thread_fixture> start_read_replica() {
        cloud_topics::test_fixture_cfg ct_cfg{
          .use_lsm_metastore = true, .skip_flush_loop = true};
        auto replica = std::make_unique<redpanda_thread_fixture>(
          model::node_id(2),
          9092 + 10,  // kafka_port
          33145 + 10, // rpc_port
          std::nullopt,
          std::nullopt,
          std::vector<config::seed_server>{},
          test_directory(),
          false,
          redpanda_thread_fixture::get_s3_config(httpd_port_number()),
          redpanda_thread_fixture::get_archival_config(),
          redpanda_thread_fixture::get_cloud_config(httpd_port_number()),
          configure_node_id::yes,
          empty_seed_starts_cluster::yes,
          false, // enable_data_transforms
          true,  // enable_legacy_upload_mode
          false, // iceberg_enabled
          true,  // enable_cloud_topics
          false, // development_cluster_linking_enabled
          ct_cfg);
        replica->wait_for_controller_leadership().get();
        return replica;
    }

    model::ntp make_ntp(int partition_id) {
        return model::ntp{
          model::kafka_namespace,
          topic_name,
          model::partition_id{partition_id}};
    }

    model::topic_id create_cloud_topic() {
        cluster::topic_properties props;
        props.storage_mode = model::redpanda_storage_mode::cloud;
        add_topic({model::kafka_namespace, topic_name}, num_partitions, props)
          .get();
        for (int i = 0; i < num_partitions; ++i) {
            wait_for_leader(make_ntp(i)).get();
        }
        auto topic_metadata = app.metadata_cache.local().get_topic_metadata(
          {model::kafka_namespace, topic_name});
        return topic_metadata->get_configuration().tp_id.value();
    }

    void produce(
      int num_partitions, int batches_per_partition, int records_per_batch) {
        auto* producer = make_producer();
        for (int p = 0; p < num_partitions; ++p) {
            int n = 0;
            for (int i = 0; i < batches_per_partition; ++i) {
                std::vector<kv_t> records;
                for (int j = 0; j < records_per_batch; ++j) {
                    records.emplace_back(
                      ssx::sformat("{}_key{}", p, n),
                      ssx::sformat("{}_value{}", p, n));
                    ++n;
                }
                producer
                  ->produce_to_partition(
                    topic_name, model::partition_id(p), std::move(records))
                  .get();
            }
        }
    }

    // Waits for L1 on the source cluster to have the given start and next
    // offset for all partitions of the given topic.
    void wait_for_l1(
      const model::topic_id& topic_id,
      int num_partitions,
      kafka::offset start_offset,
      kafka::offset next_offset) {
        auto* ct_app = app.cloud_topics_app.get();
        auto* replicated_metastore = ct_app->get_sharded_replicated_metastore();
        for (int p = 0; p < num_partitions; ++p) {
            auto tidp = model::topic_id_partition{
              topic_id, model::partition_id(p)};
            RPTEST_REQUIRE_EVENTUALLY(30s, [&](this auto) -> ss::future<bool> {
                auto offsets
                  = co_await replicated_metastore->local().get_offsets(tidp);

                if (!offsets) {
                    vlog(
                      rr_test_log.info,
                      "Partition {} no metastore offsets",
                      tidp);
                    co_return false;
                }
                if (
                  offsets->start_offset != start_offset
                  || offsets->next_offset != next_offset) {
                    vlog(
                      rr_test_log.info,
                      "Partition {} metastore offsets: [{}, {}), targets [{}, "
                      "{})",
                      tidp,
                      offsets->start_offset,
                      offsets->next_offset,
                      start_offset,
                      next_offset);
                    co_return false;
                }
                co_return true;
            });
        }
    }

    // Waits for the Kafka API of the given cluster to have the given start and
    // next offset for all partitions of the test topic.
    void wait_for_offsets(
      redpanda_thread_fixture* rp,
      int num_partitions,
      kafka::offset start_offset,
      kafka::offset next_offset) {
        auto* lister = make_list_offsets_for_rp(rp);
        for (int p = 0; p < num_partitions; ++p) {
            RPTEST_REQUIRE_EVENTUALLY(30s, [&](this auto) -> ss::future<bool> {
                auto lwm = co_await lister->start_offset_for_partition(
                  topic_name, model::partition_id(p));
                auto hwm = co_await lister->high_watermark_for_partition(
                  topic_name, model::partition_id(p));

                bool offsets_match = lwm == kafka::offset_cast(start_offset)
                                     && hwm == kafka::offset_cast(next_offset);
                if (!offsets_match) {
                    vlog(
                      rr_test_log.info,
                      "Partition {} offsets: [{}, {}), targets [{}, {})",
                      p,
                      lwm,
                      hwm,
                      start_offset,
                      next_offset);
                    co_return false;
                }
                co_return true;
            });
        }
    }

    std::vector<ss::noncopyable_function<void()>> cleanup;
    scoped_config test_local_cfg;
    static constexpr int num_partitions = 3;
    const model::topic topic_name{"tapioca"};
};

TEST_F(read_replica_fixture, test_basic_end_to_end) {
    test_local_cfg.get("cloud_storage_readreplica_manifest_sync_timeout_ms")
      .set_value(std::chrono::milliseconds(5000));
    auto topic_id = create_cloud_topic();
    int records_per_partition = 100;
    produce(num_partitions, 1, records_per_partition);
    wait_for_l1(
      topic_id,
      num_partitions,
      kafka::offset(0),
      kafka::offset(records_per_partition));

    // Flush so the read replica can see data immediately.
    auto& meta
      = app.cloud_topics_app->get_sharded_replicated_metastore()->local();
    auto flush_result = meta.flush().get();
    ASSERT_TRUE(flush_result.has_value())
      << fmt::to_string(flush_result.error());

    // Start read replica and create topic.
    //
    // WARN: after creating a new redpanda_test_fixture, touching the original
    // cluster may have unexpected behavior caused by
    // config::node().data_directory being changed on starting up the new
    // cluster. The rest of this test only operates on the read replica.
    auto replica_rp = start_read_replica();
    cluster::topic_properties rr_props;
    rr_props.read_replica = true;
    rr_props.read_replica_bucket = "test-bucket";
    replica_rp
      ->add_topic(
        {model::kafka_namespace, topic_name}, num_partitions, rr_props)
      .get();
    for (int i = 0; i < num_partitions; ++i) {
        replica_rp->wait_for_leader(make_ntp(i)).get();
    }

    wait_for_offsets(
      replica_rp.get(),
      num_partitions,
      kafka::offset(0),
      kafka::offset(records_per_partition));
    auto* consumer = make_consumer_for_rp(replica_rp.get());
    for (int p = 0; p < num_partitions; ++p) {
        vlog(rr_test_log.info, "Consuming from read replica partition {}", p);
        std::vector<kv_t> records;
        try {
            records = consumer
                        ->consume_from_partition(
                          topic_name, model::partition_id(p), model::offset(0))
                        .get();
        } catch (...) {
            auto ex = std::current_exception();
            FAIL() << fmt::format("Error consuming: {}", ex);
        }
        EXPECT_EQ(records.size(), records_per_partition)
          << fmt::format("Partition {} wrong record count", p);
        EXPECT_EQ(records[0].key, fmt::format("{}_key0", p));
        EXPECT_EQ(
          records.back().key,
          fmt::format("{}_key{}", p, records_per_partition - 1));
    }
}

TEST_F(read_replica_fixture, test_timequery) {
    test_local_cfg.get("cloud_storage_readreplica_manifest_sync_timeout_ms")
      .set_value(std::chrono::milliseconds(5000));
    auto topic_id = create_cloud_topic();

    auto* producer = make_producer();
    auto pid = model::partition_id(0);

    // Batch 1: 5 records at ts=1000 (offsets 0-4)
    // Batch 2: 5 records at ts=2000 (offsets 5-9)
    for (int b = 0; b < 2; ++b) {
        std::vector<kv_t> records;
        for (int i = 0; i < 5; ++i) {
            int n = b * 5 + i;
            records.emplace_back(
              ssx::sformat("key{}", n), ssx::sformat("val{}", n));
        }
        model::timestamp ts{1000 + b * 1000};
        producer->produce_to_partition(topic_name, pid, std::move(records), ts)
          .get();
    }

    constexpr int total_records = 10;
    wait_for_l1(topic_id, 1, kafka::offset(0), kafka::offset(total_records));
    auto& meta
      = app.cloud_topics_app->get_sharded_replicated_metastore()->local();
    meta.flush().get();

    auto replica_rp = start_read_replica();
    cluster::topic_properties rr_props;
    rr_props.read_replica = true;
    rr_props.read_replica_bucket = "test-bucket";
    replica_rp->add_topic({model::kafka_namespace, topic_name}, 1, rr_props)
      .get();
    replica_rp->wait_for_leader(make_ntp(0)).get();
    wait_for_offsets(
      replica_rp.get(), 1, kafka::offset(0), kafka::offset(total_records));

    auto* lister = make_list_offsets_for_rp(replica_rp.get());

    // Timestamps [0, 1000] should all resolve to offset 0 (batch 1).
    for (int64_t ts = 0; ts <= 1000; ts += 100) {
        EXPECT_EQ(
          lister
            ->list_offset_for_partition(topic_name, pid, model::timestamp{ts})
            .get(),
          model::offset(0))
          << "ts=" << ts;
    }

    // Timestamps (1000, 2000] should resolve to offset 5 (batch 2).
    for (int64_t ts = 1001; ts <= 2000; ts += 100) {
        EXPECT_EQ(
          lister
            ->list_offset_for_partition(topic_name, pid, model::timestamp{ts})
            .get(),
          model::offset(5))
          << "ts=" << ts;
    }

    // Timestamps after all data should return -1 (no match).
    for (int64_t ts = 2001; ts <= 3000; ts += 100) {
        EXPECT_EQ(
          lister
            ->list_offset_for_partition(topic_name, pid, model::timestamp{ts})
            .get(),
          model::offset(-1))
          << "ts=" << ts;
    }
}

// Verifies that timequery respects the start offset after delete_records.
TEST_F(read_replica_fixture, test_timequery_after_delete_records) {
    test_local_cfg.get("cloud_storage_readreplica_manifest_sync_timeout_ms")
      .set_value(std::chrono::milliseconds(5000));
    test_local_cfg.get("cloud_storage_housekeeping_interval_ms")
      .set_value(std::chrono::milliseconds(1000));
    auto topic_id = create_cloud_topic();

    int total_records = 20;
    produce(1, total_records, 1);
    wait_for_l1(topic_id, 1, kafka::offset(0), kafka::offset(total_records));

    auto* deleter = make_delete_records_client();
    kafka::offset delete_up_to(10);
    deleter
      ->delete_records_from_partition(
        topic_name,
        model::partition_id(0),
        model::offset(delete_up_to()),
        std::chrono::seconds(5))
      .get();
    wait_for_l1(topic_id, 1, delete_up_to, kafka::offset(total_records));

    auto& meta
      = app.cloud_topics_app->get_sharded_replicated_metastore()->local();
    meta.flush().get();

    auto replica_rp = start_read_replica();
    cluster::topic_properties rr_props;
    rr_props.read_replica = true;
    rr_props.read_replica_bucket = "test-bucket";
    replica_rp->add_topic({model::kafka_namespace, topic_name}, 1, rr_props)
      .get();
    replica_rp->wait_for_leader(make_ntp(0)).get();
    wait_for_offsets(
      replica_rp.get(), 1, delete_up_to, kafka::offset(total_records));

    // Query timestamp 0 (before all records). Without the start offset
    // constraint, this would return offset 0. With delete_records having
    // advanced start_offset to 10, the result must be >= 10.
    auto* lister = make_list_offsets_for_rp(replica_rp.get());
    auto offset = lister
                    ->list_offset_for_partition(
                      topic_name, model::partition_id(0), model::timestamp{0})
                    .get();
    EXPECT_GE(offset, model::offset(delete_up_to()));
}

TEST_F(read_replica_fixture, test_delete_records_sync) {
    test_local_cfg.get("cloud_storage_readreplica_manifest_sync_timeout_ms")
      .set_value(std::chrono::milliseconds(5000));
    test_local_cfg.get("cloud_storage_housekeeping_interval_ms")
      .set_value(std::chrono::milliseconds(1000));

    auto topic_id = create_cloud_topic();
    int records_per_partition = 20;
    // NOTE: delete_records operates along batch boundaries on the broker side;
    // make the batch size 1 so reads can filter entire removed batches.
    produce(num_partitions, records_per_partition, 1);
    wait_for_l1(
      topic_id,
      num_partitions,
      kafka::offset(0),
      kafka::offset(records_per_partition));

    auto* deleter = make_delete_records_client();
    kafka::offset delete_up_to(10);
    for (int p = 0; p < num_partitions; ++p) {
        auto lwm = deleter
                     ->delete_records_from_partition(
                       topic_name,
                       model::partition_id(p),
                       model::offset(delete_up_to()),
                       std::chrono::seconds(5))
                     .get();

        ASSERT_EQ(lwm, model::offset(delete_up_to())) << fmt::format(
          "DeleteRecords failed for partition {}: expected {}, got {}",
          p,
          delete_up_to,
          lwm);
    }
    wait_for_l1(
      topic_id,
      num_partitions,
      delete_up_to,
      kafka::offset(records_per_partition));

    // Flush so the read replica can see the data and its removal immediately.
    auto& meta
      = app.cloud_topics_app->get_sharded_replicated_metastore()->local();
    auto flush_result = meta.flush().get();
    ASSERT_TRUE(flush_result.has_value())
      << fmt::to_string(flush_result.error());

    vlog(rr_test_log.info, "Metastore flushed");

    // Start read replica and create topic.
    //
    // WARN: after creating a new redpanda_test_fixture, touching the original
    // cluster may have unexpected behavior caused by
    // config::node().data_directory being changed on starting up the new
    // cluster. The rest of this test only operates on the read replica.
    vlog(rr_test_log.info, "Starting read replica");
    auto replica_rp = start_read_replica();
    cluster::topic_properties rr_props;
    rr_props.read_replica = true;
    rr_props.read_replica_bucket = "test-bucket";
    replica_rp
      ->add_topic(
        {model::kafka_namespace, topic_name}, num_partitions, rr_props)
      .get();

    for (int i = 0; i < num_partitions; ++i) {
        replica_rp->wait_for_leader(make_ntp(i)).get();
    }
    wait_for_offsets(
      replica_rp.get(),
      num_partitions,
      delete_up_to,
      kafka::offset(records_per_partition));

    auto* consumer = make_consumer_for_rp(replica_rp.get());
    for (int p = 0; p < num_partitions; ++p) {
        vlog(rr_test_log.info, "Consuming from read replica partition {}", p);
        std::vector<kv_t> records;
        try {
            records = consumer
                        ->consume_from_partition(
                          topic_name,
                          model::partition_id(p),
                          model::offset(delete_up_to()))
                        .get();
        } catch (...) {
            auto ex = std::current_exception();
            FAIL() << fmt::format("Error consuming: {}", ex);
        }
        ASSERT_FALSE(records.empty()) << fmt::format(
          "Expected records from partition {} at offset {}", p, delete_up_to);
        EXPECT_EQ(records[0].key, fmt::format("{}_key{}", p, delete_up_to()))
          << fmt::format("Partition {} first record key mismatch", p);
    }
}
