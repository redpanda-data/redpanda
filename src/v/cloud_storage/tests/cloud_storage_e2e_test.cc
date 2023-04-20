/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/ntp_archiver_service.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "config/configuration.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "redpanda/tests/fixture.h"
#include "storage/disk_log_impl.h"

#include <seastar/core/io_priority_class.hh>

using tests::kafka_consume_transport;
using tests::kafka_produce_transport;
using tests::kv_t;

class e2e_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    e2e_fixture()
      : redpanda_thread_fixture(
        redpanda_thread_fixture::init_cloud_storage_tag{},
        httpd_port_number()) {
        // No expectations: tests will PUT and GET organically.
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
    }
};

FIXTURE_TEST(test_produce_consume_from_cloud, e2e_fixture) {
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    cluster::topic_properties props;
    props.shadow_indexing = model::shadow_indexing_mode::full;
    props.retention_local_target_bytes = tristate<size_t>(1);
    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    // Do some sanity checks that our partition looks the way we expect (has a
    // log, archiver, etc).
    auto partition = app.partition_manager.local().get(ntp);
    auto* log = dynamic_cast<storage::disk_log_impl*>(
      partition->log().get_impl());
    auto archiver_ref = partition->archiver();
    BOOST_REQUIRE(archiver_ref.has_value());
    auto& archiver = archiver_ref.value().get();

    kafka_produce_transport producer(make_kafka_client().get());
    producer.start().get();
    std::vector<kv_t> records{
      {"key0", "val0"},
      {"key1", "val1"},
      {"key2", "val2"},
    };
    producer.produce_to_partition(topic_name, model::partition_id(0), records)
      .get();

    // Create a new segment so we have data to upload.
    log->flush().get();
    log->force_roll(ss::default_priority_class()).get();
    BOOST_REQUIRE_EQUAL(2, log->segments().size());

    // Upload the closed segment to object storage.
    auto res = archiver.upload_next_candidates().get();
    BOOST_REQUIRE_EQUAL(0, res.compacted_upload_result.num_succeeded);
    BOOST_REQUIRE_EQUAL(1, res.non_compacted_upload_result.num_succeeded);
    auto manifest_res = archiver.upload_manifest("test").get();
    BOOST_REQUIRE_EQUAL(manifest_res, cloud_storage::upload_result::success);
    archiver.flush_manifest_clean_offset().get();

    // Compact the local log to GC to the collectible offset.
    ss::abort_source as;
    storage::compaction_config compaction_conf(
      model::timestamp::min(),
      1,
      log->stm_manager()->max_collectible_offset(),
      ss::default_priority_class(),
      as);
    partition->log().compact(compaction_conf).get();
    // NOTE: the storage layer only initially requests eviction; it relies on
    // Raft to write a snapshot and subsequently truncate.
    tests::cooperative_spin_wait_with_timeout(3s, [log] {
        return log->segments().size() == 1;
    }).get();

    // Attempt to consume from the beginning of the log. Since our local log
    // has been truncated, this exercises reading from cloud storage.
    kafka_consume_transport consumer(make_kafka_client().get());
    consumer.start().get();
    auto consumed_records = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(0))
                              .get();
    BOOST_CHECK_EQUAL(records.size(), consumed_records.size());
    for (int i = 0; i < records.size(); ++i) {
        BOOST_CHECK_EQUAL(records[i].first, consumed_records[i].first);
        BOOST_CHECK_EQUAL(records[i].second, consumed_records[i].second);
    }
}
