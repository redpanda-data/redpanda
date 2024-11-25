/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "archival/ntp_archiver_service.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/tests/produce_utils.h"
#include "cloud_storage/tests/read_replica_fixture.h"
#include "cloud_storage/types.h"
#include "config/configuration.h"
#include "kafka/server/tests/delete_records_utils.h"
#include "kafka/server/tests/list_offsets_utils.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "storage/disk_log_impl.h"
#include "test_utils/scoped_config.h"

using tests::kafka_consume_transport;

FIXTURE_TEST(test_read_replica_basic_sync, read_replica_e2e_fixture) {
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    cluster::topic_properties props;
    props.shadow_indexing = model::shadow_indexing_mode::full;
    props.retention_local_target_bytes = tristate<size_t>(1);
    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    // Produce records to the source.
    auto partition = app.partition_manager.local().get(ntp).get();
    auto& archiver = partition->archiver()->get();
    BOOST_REQUIRE(archiver.sync_for_tests().get());
    archiver.upload_topic_manifest().get();
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    BOOST_REQUIRE_EQUAL(
      30, gen.records_per_batch(10).num_segments(3).produce().get());
    BOOST_REQUIRE_EQUAL(3, archiver.manifest().size());

    // Create the read replica application, now that we've initialized things
    // that rely globals (e.g. configs, etc).
    auto rr_rp = start_read_replica_fixture();

    cluster::topic_properties read_replica_props;
    read_replica_props.shadow_indexing = model::shadow_indexing_mode::fetch;
    read_replica_props.read_replica = true;
    read_replica_props.read_replica_bucket = "test-bucket";
    rr_rp
      ->add_topic({model::kafka_namespace, topic_name}, 1, read_replica_props)
      .get();
    rr_rp->wait_for_leader(ntp).get();
    auto rr_partition = rr_rp->app.partition_manager.local().get(ntp).get();
    auto rr_archiver_ref = rr_partition->archiver();
    BOOST_REQUIRE(rr_archiver_ref.has_value());
    auto& rr_archiver = rr_partition->archiver()->get();
    BOOST_REQUIRE(rr_archiver.sync_for_tests().get());
    rr_archiver.sync_manifest().get();
    BOOST_REQUIRE_EQUAL(3, rr_archiver.manifest().size());

    kafka_consume_transport consumer(rr_rp->make_kafka_client().get());
    consumer.start().get();
    model::offset next(0);
    while (next < model::offset(30)) {
        auto consumed_records = consumer
                                  .consume_from_partition(
                                    topic_name, model::partition_id(0), next)
                                  .get();
        BOOST_REQUIRE(!consumed_records.empty());
        for (const auto& [k, v] : consumed_records) {
            BOOST_REQUIRE_EQUAL(k, ssx::sformat("key{}", next()));
            next += model::offset(1);
        }
    }
}

FIXTURE_TEST(
  test_read_replica_rejects_delete_records, read_replica_e2e_fixture) {
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    cluster::topic_properties props;
    props.shadow_indexing = model::shadow_indexing_mode::full;
    props.retention_local_target_bytes = tristate<size_t>(1);
    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp).get();
    auto& archiver = partition->archiver()->get();
    BOOST_REQUIRE(archiver.sync_for_tests().get());
    archiver.upload_topic_manifest().get();

    auto rr_rp = start_read_replica_fixture();
    cluster::topic_properties read_replica_props;
    read_replica_props.shadow_indexing = model::shadow_indexing_mode::fetch;
    read_replica_props.read_replica = true;
    read_replica_props.read_replica_bucket = "test-bucket";
    rr_rp
      ->add_topic({model::kafka_namespace, topic_name}, 1, read_replica_props)
      .get();
    rr_rp->wait_for_leader(ntp).get();

    // Send the delete request to the _read replica_. This is not allowed.
    tests::kafka_delete_records_transport deleter(
      rr_rp->make_kafka_client().get());
    deleter.start().get();
    BOOST_REQUIRE_EXCEPTION(
      deleter
        .delete_records_from_partition(
          topic_name, model::partition_id(0), model::offset(0), 5s)
        .get(),
      std::runtime_error,
      [](std::runtime_error e) {
          return std::string(e.what()).find("policy_violation")
                 != std::string::npos;
      });
}

FIXTURE_TEST(test_read_replica_delete_records, read_replica_e2e_fixture) {
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    cluster::topic_properties props;
    props.shadow_indexing = model::shadow_indexing_mode::full;
    props.retention_local_target_bytes = tristate<size_t>(1);
    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    // Produce records to the source.
    auto partition = app.partition_manager.local().get(ntp).get();
    auto& archiver = partition->archiver()->get();
    BOOST_REQUIRE(archiver.sync_for_tests().get());
    archiver.upload_topic_manifest().get();
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    BOOST_REQUIRE_EQUAL(
      30, gen.batches_per_segment(10).num_segments(3).produce().get());
    BOOST_REQUIRE_EQUAL(3, archiver.manifest().size());

    // NOTE: we're creating the deleter here before starting the read replica
    // fixture because make_kafka_client() uses global configs that will be
    // overwritten by the new fixture.
    tests::kafka_delete_records_transport deleter(make_kafka_client().get());

    auto rr_rp = start_read_replica_fixture();
    cluster::topic_properties read_replica_props;
    read_replica_props.shadow_indexing = model::shadow_indexing_mode::fetch;
    read_replica_props.read_replica = true;
    read_replica_props.read_replica_bucket = "test-bucket";
    rr_rp
      ->add_topic({model::kafka_namespace, topic_name}, 1, read_replica_props)
      .get();
    rr_rp->wait_for_leader(ntp).get();
    auto rr_partition = rr_rp->app.partition_manager.local().get(ntp).get();
    auto& rr_archiver = rr_partition->archiver()->get();

    // Do an initial sync to download the manifest.
    BOOST_REQUIRE(rr_archiver.sync_for_tests().get());
    rr_archiver.sync_manifest().get();
    BOOST_REQUIRE_EQUAL(3, rr_archiver.manifest().size());

    kafka_consume_transport consumer(rr_rp->make_kafka_client().get());
    consumer.start().get();
    auto consumed_records = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(28))
                              .get();
    BOOST_REQUIRE(!consumed_records.empty());
    BOOST_CHECK_EQUAL("key28", consumed_records[0].key);
    BOOST_CHECK_EQUAL("val28", consumed_records[0].val);

    // DeleteRecords on the source cluster.
    deleter.start().get();
    auto new_start = model::offset(29);
    BOOST_REQUIRE_EQUAL(
      new_start,
      deleter
        .delete_records_from_partition(
          topic_name, model::partition_id(0), new_start, 5s)
        .get());
    BOOST_REQUIRE_EQUAL(
      cloud_storage::upload_result::success,
      archiver.upload_manifest("test").get());
    archiver.flush_manifest_clean_offset().get();

    // Once synced on the read replica, it should fail to be read.
    BOOST_REQUIRE(rr_archiver.sync_for_tests().get());
    BOOST_REQUIRE_EQUAL(
      cloud_storage::download_result::success,
      rr_archiver.sync_manifest().get());
    BOOST_REQUIRE_EQUAL(3, rr_archiver.manifest().size());
    BOOST_REQUIRE_EXCEPTION(
      consumer
        .consume_from_partition(
          topic_name, model::partition_id(0), model::offset(28))
        .get(),
      std::runtime_error,
      [](std::runtime_error e) {
          return std::string(e.what()).find("out_of_range")
                 != std::string::npos;
      });
    consumed_records = consumer
                         .consume_from_partition(
                           topic_name,
                           model::partition_id(0),
                           model::offset(29))
                         .get();
    BOOST_REQUIRE(!consumed_records.empty());
    BOOST_CHECK_EQUAL("key29", consumed_records[0].key);
    BOOST_CHECK_EQUAL("val29", consumed_records[0].val);
}

FIXTURE_TEST(
  test_read_replica_delete_records_in_local, read_replica_e2e_fixture) {
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    cluster::topic_properties props;
    props.shadow_indexing = model::shadow_indexing_mode::full;
    props.retention_local_target_bytes = tristate<size_t>(1);
    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp).get();
    auto& archiver = partition->archiver()->get();
    BOOST_REQUIRE(archiver.sync_for_tests().get());
    archiver.upload_topic_manifest().get();
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    BOOST_REQUIRE_EQUAL(
      40,
      gen.batches_per_segment(10)
        .num_segments(3)
        .additional_local_segments(1)
        .produce()
        .get());
    BOOST_REQUIRE_EQUAL(3, archiver.manifest().size());
    auto highest_kafka_offset_in_cloud
      = archiver.manifest().get_last_kafka_offset().value();

    // DeleteRecords in the region of the log that hasn't yet been uploaded.
    tests::kafka_delete_records_transport deleter(make_kafka_client().get());
    deleter.start().get();
    auto new_start = model::offset(35);
    BOOST_REQUIRE_EQUAL(
      new_start,
      deleter
        .delete_records_from_partition(
          topic_name, model::partition_id(0), new_start, 5s)
        .get());

    // Persist the deletion to the cloud without uploading the segments.
    BOOST_REQUIRE_EQUAL(
      cloud_storage::upload_result::success,
      archiver.upload_manifest("test").get());
    archiver.flush_manifest_clean_offset().get();

    tests::kafka_list_offsets_transport lister(make_kafka_client().get());
    lister.start().get();
    auto lwm = lister
                 .start_offset_for_partition(topic_name, model::partition_id(0))
                 .get();
    BOOST_REQUIRE_EQUAL(lwm, new_start);

    // A read replica will get the override, but be clamped to the highest
    // offset in uploaded segments.
    auto rr_rp = start_read_replica_fixture();
    cluster::topic_properties read_replica_props;
    read_replica_props.shadow_indexing = model::shadow_indexing_mode::fetch;
    read_replica_props.read_replica = true;
    read_replica_props.read_replica_bucket = "test-bucket";
    rr_rp
      ->add_topic({model::kafka_namespace, topic_name}, 1, read_replica_props)
      .get();
    rr_rp->wait_for_leader(ntp).get();
    auto rr_partition = rr_rp->app.partition_manager.local().get(ntp).get();
    auto& rr_archiver = rr_partition->archiver()->get();
    rr_archiver.sync_manifest().get();

    tests::kafka_list_offsets_transport rr_lister(
      rr_rp->make_kafka_client().get());
    rr_lister.start().get();
    auto rr_hwm = rr_lister
                    .high_watermark_for_partition(
                      topic_name, model::partition_id(0))
                    .get();
    BOOST_REQUIRE_EQUAL(
      rr_hwm,
      model::next_offset(kafka::offset_cast(highest_kafka_offset_in_cloud)));
    auto rr_lwm
      = rr_lister.start_offset_for_partition(topic_name, model::partition_id(0))
          .get();
    BOOST_REQUIRE_EQUAL(rr_lwm, rr_hwm);
}
