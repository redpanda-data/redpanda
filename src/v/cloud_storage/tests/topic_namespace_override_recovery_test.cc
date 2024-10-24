/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_storage/remote.h"
#include "cloud_storage/tests/produce_utils.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/cloud_metadata/uploader.h"
#include "cluster/config_frontend.h"
#include "cluster/controller_snapshot.h"
#include "cluster/feature_manager.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/partition.h"
#include "cluster/tests/topic_properties_generator.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/util/defer.hh>

class TopicRecoveryFixture
  : public seastar_test
  , public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    TopicRecoveryFixture()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{},
          httpd_port_number())
      , bucket(cloud_storage_clients::bucket_name("test-bucket")) {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
    }

    ss::future<> SetUpAsync() override {
        RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
            return app.storage.local().get_cluster_uuid().has_value();
        });
        cluster_uuid = app.storage.local().get_cluster_uuid().value();
    }

protected:
    const cloud_storage_clients::bucket_name bucket;
    model::cluster_uuid cluster_uuid = {};
    scoped_config test_local_cfg;
};

using tests::kafka_consume_transport;
using tests::kv_t;

TEST_F(TopicRecoveryFixture, TestTopicNamespaceOverrideRecovery) {
    test_local_cfg.get("cloud_storage_disable_upload_loop_for_tests")
      .set_value(true);

    model::partition_id pid{0};

    // The "source" topic namespace.
    model::topic_namespace src_tp_ns{
      model::kafka_namespace, model::topic{"cassava"}};
    model::ntp src_ntp(src_tp_ns.ns, src_tp_ns.tp, pid);

    cluster::topic_properties src_props;
    src_props.shadow_indexing = model::shadow_indexing_mode::full;
    src_props.retention_local_target_bytes = tristate<size_t>(1);

    add_topic(src_tp_ns, 1, src_props).get();
    wait_for_leader(src_ntp).get();

    static constexpr size_t num_records_to_produce = 10;
    const auto records = kv_t::sequence(0, num_records_to_produce);

    // Nested scope here, because the ptr to partition is going to be invalid
    // post cluster restart. Attempting to free it at the end of test fixture
    // scope will result in use-after-free.
    {
        auto partition = app.partition_manager.local().get(src_ntp);
        auto log = partition->log();
        auto archiver_ref = partition->archiver();
        ASSERT_TRUE(archiver_ref.has_value());
        auto& archiver = archiver_ref.value().get();

        // Produce some records to the partition.
        tests::remote_segment_generator gen(
          make_kafka_client().get(), *partition);

        ASSERT_EQ(
          num_records_to_produce,
          gen.records_per_batch(num_records_to_produce).produce().get());

        // Assert consuming from original topic works fine.
        kafka_consume_transport consumer(make_kafka_client().get());
        consumer.start().get();
        auto consumed_records
          = consumer.consume_from_partition(src_tp_ns.tp, pid, model::offset(0))
              .get();

        ASSERT_EQ(records.size(), consumed_records.size());
        for (int i = 0; i < records.size(); ++i) {
            ASSERT_EQ(records[i].key, consumed_records[i].key);
            ASSERT_EQ(records[i].val, consumed_records[i].val);
        }

        // Sync archiver, upload candidates (if needed) and upload manifest.
        archiver.sync_for_tests().get();
        std::ignore = archiver.upload_next_candidates().get();
        archiver.upload_topic_manifest().get();
    }

    // Restart the cluster, wiping all local data in the process.
    restart(should_wipe::yes);
    RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
        return app.storage.local().get_cluster_uuid().has_value();
    });

    // Now, create a new topic namespace that differs from the original,
    // but uses the original src_tp_ns as an override in topic_properties.
    // By setting recovery to true, it will attempt to use the src topic for
    // recovery upon creation.
    model::topic_namespace dest_tp_ns{
      model::kafka_namespace, model::topic{"tapioca"}};
    model::ntp dest_ntp(dest_tp_ns.ns, dest_tp_ns.tp, pid);

    // Expect that no requests will be made to the s3_imposter using the
    // destination topic.
    http_test_utils::response fail_response{
      .body = "Should not have recieved a request with tapioca in it",
      .status = http_test_utils::response::status_type::bad_request};
    req_pred_t fail_dest_topic_request =
      [](const http_test_utils::request_info& info) {
          return info.url.contains("tapioca");
      };
    fail_request_if(fail_dest_topic_request, fail_response);

    cluster::topic_properties dest_props;
    dest_props.shadow_indexing = model::shadow_indexing_mode::full;
    dest_props.retention_local_target_bytes = tristate<size_t>(1);
    dest_props.remote_topic_namespace_override = src_tp_ns;
    dest_props.recovery = true;

    add_topic(dest_tp_ns, 1, dest_props).get();
    wait_for_leader(dest_ntp).get();

    // Assert consuming from recovered destination topic works fine.
    kafka_consume_transport consumer(make_kafka_client().get());
    consumer.start().get();
    auto consumed_records
      = consumer.consume_from_partition(dest_tp_ns.tp, pid, model::offset(0))
          .get();

    ASSERT_EQ(records.size(), consumed_records.size());
    for (int i = 0; i < records.size(); ++i) {
        ASSERT_EQ(records[i].key, consumed_records[i].key);
        ASSERT_EQ(records[i].val, consumed_records[i].val);
    }

    // Produce more to the destination topic, s3_imposter will fail if any
    // "tapioca" related requests are made.
    auto partition = app.partition_manager.local().get(dest_ntp);
    auto archiver_ref = partition->archiver();
    ASSERT_TRUE(archiver_ref.has_value());
    auto& archiver = archiver_ref.value().get();

    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);

    ASSERT_EQ(
      num_records_to_produce,
      gen.num_segments(2)
        .records_per_batch(num_records_to_produce)
        .produce()
        .get());

    archiver.sync_for_tests().get();
    std::ignore = archiver.upload_next_candidates().get();

    // Check requests with the same predicate at end of scope, just to be
    // explicit about bad requests.
    const auto bad_requests = get_requests(fail_dest_topic_request);
    ASSERT_TRUE(bad_requests.empty());
}
