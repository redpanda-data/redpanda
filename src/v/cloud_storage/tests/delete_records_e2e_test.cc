/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "absl/container/flat_hash_set.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/tests/produce_utils.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "config/configuration.h"
#include "kafka/server/tests/delete_records_utils.h"
#include "kafka/server/tests/list_offsets_utils.h"
#include "kafka/server/tests/offset_for_leader_epoch_utils.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "redpanda/tests/fixture.h"
#include "storage/disk_log_impl.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <boost/algorithm/string/predicate.hpp>
#include <gtest/gtest.h>

#include <iterator>

using tests::kafka_consume_transport;
using tests::kafka_delete_records_transport;

static ss::logger e2e_test_log("delete_records_e2e_test");

namespace {

void check_consume_out_of_range(
  kafka_consume_transport& consumer,
  const model::topic& topic_name,
  const model::partition_id pid,
  model::offset kafka_offset) {
    // TODO(oren): add check for exception contents
    std::string msg{};
    ASSERT_NO_THROW([&]() mutable {
        try {
            consumer.consume_from_partition(topic_name, pid, kafka_offset)
              .get();
        } catch (std::runtime_error& e) {
            msg = std::string(e.what());
        }
    }());

    ASSERT_NE(msg.find("out_of_range"), std::string::npos);
};

// Returns the number of bytes in whole segments above the given Kafka offset
// in the manifest. Expects that the given offset exists in a segment.
void segment_bytes_above_offset(
  const cloud_storage::partition_manifest& stm_manifest,
  kafka::offset o,
  size_t& result) {
    auto it = stm_manifest.segment_containing(o);
    ASSERT_TRUE(it != stm_manifest.end());
    ASSERT_TRUE(++it != stm_manifest.end());
    size_t segment_bytes_past_override = 0;
    while (it != stm_manifest.end()) {
        segment_bytes_past_override += it->size_bytes;
        ++it;
    }
    result = segment_bytes_past_override;
}

} // namespace

class delete_records_e2e_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture
  , public ::testing::TestWithParam<model::cloud_storage_segment_upload_mode> {
public:
    static constexpr auto segs_per_spill = 10;
    delete_records_e2e_fixture()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{},
          httpd_port_number()) {
        // No expectations: tests will PUT and GET organically.
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();

        // Apply local retention frequently.
        test_local_cfg.get("log_compaction_interval_ms")
          .set_value(std::chrono::duration_cast<std::chrono::milliseconds>(1s));
        // We'll control uploads ourselves.
        test_local_cfg.get("cloud_storage_enable_segment_merging")
          .set_value(false);
        test_local_cfg.get("cloud_storage_disable_upload_loop_for_tests")
          .set_value(true);
        // Disable metrics to speed things up.
        test_local_cfg.get("enable_metrics_reporter").set_value(false);
        // Encourage spilling over.
        test_local_cfg.get("cloud_storage_spillover_manifest_max_segments")
          .set_value(std::make_optional<size_t>(segs_per_spill));
        test_local_cfg.get("cloud_storage_spillover_manifest_size")
          .set_value(std::optional<size_t>{});
        test_local_cfg.get("retention_local_strict").set_value(true);

        test_local_cfg.get("cloud_storage_segment_upload_mode")
          .set_value(GetParam());
        test_local_cfg.get("cloud_storage_segment_size_target")
          .set_value(std::make_optional<size_t>(1024));

        topic_name = model::topic("tapioca");
        ntp = model::ntp(model::kafka_namespace, topic_name, 0);

        // Create a tiered storage topic with very little local retention.
        cluster::topic_properties props;
        props.shadow_indexing = model::shadow_indexing_mode::full;
        props.retention_local_target_bytes = tristate<size_t>(1);
        props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::deletion;
        add_topic({model::kafka_namespace, topic_name}, 1, props).get();
        wait_for_leader(ntp).get();
        partition = app.partition_manager.local().get(ntp).get();
        log = partition->log();
    }

    void SetUp() override {
        auto archiver_ref = partition->archiver();
        ASSERT_TRUE(archiver_ref.has_value());
        archiver = &archiver_ref.value().get();
    }

    // Truncates by space, expecting the override offset is removed.
    void check_truncate_removes_override(size_t bytes) {
        auto props = partition->get_topic_config()->get().properties;
        props.retention_bytes = tristate<size_t>(bytes);
        partition->update_configuration(std::move(props)).get();
        auto& new_archiver = partition->archiver()->get();
        new_archiver.housekeeping().get();
        ASSERT_EQ(
          new_archiver.manifest().get_start_kafka_offset_override(),
          kafka::offset{});
    }

    scoped_config test_local_cfg;

    model::topic topic_name;
    model::ntp ntp;
    cluster::partition* partition;
    ss::shared_ptr<storage::log> log;
    archival::ntp_archiver* archiver;
};

TEST_P(delete_records_e2e_fixture, test_timequery_below_deleted_offset) {
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    // Use a starting timestamp and make sure each batch gets a different
    // timestamp.
    ASSERT_EQ(
      12,
      gen.num_segments(3)
        .batches_per_segment(3)
        .additional_local_segments(1)
        .base_timestamp(model::timestamp::now())
        .batch_time_delta_ms(10)
        .produce()
        .get());

    auto& stm_manifest = archiver->manifest();
    auto first_seg = stm_manifest.first_addressable_segment();
    auto first_seg_max_ts = first_seg->max_timestamp;
    tests::kafka_list_offsets_transport lister(make_kafka_client().get());
    lister.start().get();

    // Sanity check: timequery the end of the first cloud segment.
    auto offset = lister
                    .list_offset_for_partition(
                      topic_name, model::partition_id(0), first_seg_max_ts)
                    .get();
    ASSERT_EQ(first_seg->last_kafka_offset(), model::offset_cast(offset));
    auto second_seg = stm_manifest.segment_containing(
      first_seg->next_kafka_offset());

    // Delete into the second segment.
    kafka_delete_records_transport deleter(make_kafka_client("deleter").get());
    deleter.start().get();
    auto second_seg_end_offset = kafka::offset_cast(
      second_seg->last_kafka_offset());
    auto lwm
      = deleter
          .delete_records_from_partition(
            topic_name, model::partition_id(0), second_seg_end_offset, 5s)
          .get();
    ASSERT_EQ(second_seg_end_offset, lwm);

    // Timequeries into the first cloud segment should be bumped up.
    auto post_delete_offset = lister
                                .list_offset_for_partition(
                                  topic_name,
                                  model::partition_id(0),
                                  first_seg_max_ts)
                                .get();
    ASSERT_EQ(second_seg_end_offset, post_delete_offset);

    // Now trim again, but this time, trim the entire cloud range.
    auto first_local_offset = stm_manifest.last_segment()->next_kafka_offset();
    lwm = deleter
            .delete_records_from_partition(
              topic_name,
              model::partition_id(0),
              kafka::offset_cast(first_local_offset),
              5s)
            .get();
    ASSERT_EQ(first_local_offset, model::offset_cast(lwm));

    // Timequeries into the cloud region should be bumped up.
    post_delete_offset = lister
                           .list_offset_for_partition(
                             topic_name,
                             model::partition_id(0),
                             first_seg_max_ts)
                           .get();
    ASSERT_EQ(first_local_offset, model::offset_cast(post_delete_offset));
}

TEST_P(delete_records_e2e_fixture, test_leader_epoch_below_deleted_offset) {
    // Step down some to have more than one term in the log.
    // In each term, we'll write three segments.
    for (int i = 0; i < 3; i++) {
        partition->raft()->step_down("test_stepdown").get();
        wait_for_leader(ntp, 10s).get();
        tests::remote_segment_generator gen(
          make_kafka_client().get(), *partition);
        ASSERT_EQ(
          9,
          // 3 segments each with 3 batches, + 1 segment for the leadership
          // change.
          gen.num_segments(4 * (i + 1)).batches_per_segment(3).produce().get());
    }
    tests::kafka_offset_for_epoch_transport offer(make_kafka_client().get());
    offer.start().get();
    auto last_in_term_2 = offer
                            .offset_for_leader_partition(
                              topic_name,
                              model::partition_id(0),
                              model::term_id(2))
                            .get();
    ASSERT_EQ(model::offset(9), last_in_term_2);
    kafka_delete_records_transport deleter(make_kafka_client().get());
    deleter.start().get();
    auto lwm = deleter
                 .delete_records_from_partition(
                   topic_name, model::partition_id(0), model::offset(13), 5s)
                 .get();
    EXPECT_EQ(model::offset(13), lwm);

    // After deleting the last offset in the term, the same query gets bumped
    // to the new start offset.
    last_in_term_2 = offer
                       .offset_for_leader_partition(
                         topic_name, model::partition_id(0), model::term_id(2))
                       .get();
    ASSERT_EQ(model::offset(13), last_in_term_2);
}

// Test consuming after truncating the STM manifest.
TEST_P(delete_records_e2e_fixture, test_delete_from_stm_consume) {
    // Create a segment with three distinct batches.
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    ASSERT_EQ(9, gen.num_segments(3).batches_per_segment(3).produce().get());
    ASSERT_EQ(3, archiver->manifest().size());

    // Delete in the middle of a segment.
    kafka_delete_records_transport deleter(make_kafka_client("deleter").get());
    deleter.start().get();
    auto lwm = deleter
                 .delete_records_from_partition(
                   topic_name, model::partition_id(0), model::offset(1), 5s)
                 .get();
    EXPECT_EQ(model::offset(1), lwm);
    RPTEST_REQUIRE_EVENTUALLY(
      10s, [this] { return log->segment_count() == 1; });

    kafka_consume_transport consumer(make_kafka_client().get());
    consumer.start().get();
    auto consumed_records = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(1))
                              .get();
    EXPECT_GE(consumed_records.size(), 1);
    EXPECT_EQ("key1", consumed_records[0].key);
    EXPECT_EQ("val1", consumed_records[0].val);
    check_consume_out_of_range(
      consumer, topic_name, model::partition_id(0), model::offset(0));
}

// Test consuming after truncating the archive manifests.
TEST_P(delete_records_e2e_fixture, test_delete_from_archive_consume) {
    auto partition = app.partition_manager.local().get(ntp);
    auto& archiver = partition->archiver()->get();
    archiver.sync_for_tests().get();

    const auto records_per_seg = 5;
    const auto num_segs = 40;
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    auto total_records = gen.num_segments(num_segs)
                           .batches_per_segment(records_per_seg)
                           .produce()
                           .get();
    ASSERT_GE(total_records, 200);
    archiver.apply_spillover().get();
    auto& stm_manifest = archiver.manifest();
    ASSERT_EQ(stm_manifest.get_archive_start_offset(), model::offset(0));
    ASSERT_GT(
      stm_manifest.get_start_offset(), stm_manifest.get_archive_start_offset());
    ASSERT_EQ(
      stm_manifest.get_spillover_map().size(), num_segs / segs_per_spill - 1);
    ASSERT_EQ(stm_manifest.size(), segs_per_spill);
    ASSERT_EQ(
      archiver.upload_manifest("test").get(),
      cloud_storage::upload_result::success);
    archiver.flush_manifest_clean_offset().get();

    // Delete at every offset, ensuring we consume properly at each offset.
    kafka_delete_records_transport deleter(make_kafka_client().get());
    deleter.start().get();
    kafka_consume_transport consumer(make_kafka_client().get());
    consumer.start().get();
    for (int i = 1; i < total_records; i++) {
        auto lwm = deleter
                     .delete_records_from_partition(
                       topic_name, model::partition_id(0), model::offset(i), 5s)
                     .get();
        ASSERT_EQ(model::offset(i), lwm);
        check_consume_out_of_range(
          consumer, topic_name, model::partition_id(0), model::offset(i - 1));
        auto consumed_records = consumer
                                  .consume_from_partition(
                                    topic_name,
                                    model::partition_id(0),
                                    model::offset(i))
                                  .get();
        ASSERT_TRUE(!consumed_records.empty());
        auto key = consumed_records[0].key;
        ASSERT_EQ(key, ssx::sformat("key{}", i));
    }
}

// Test that truncation is applied to cloud storage as expected when a
// DeleteRecords request lands in local storage.
TEST_P(delete_records_e2e_fixture, test_delete_from_local_storage_truncation) {
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    size_t records_per_seg = 5;
    ASSERT_GE(
      250,
      gen.batches_per_segment(records_per_seg)
        .num_segments(40)
        .additional_local_segments(10)
        .produce()
        .get());
    ASSERT_TRUE(archiver->sync_for_tests().get());
    archiver->apply_spillover().get();
    auto& stm_manifest = archiver->manifest();
    ASSERT_EQ(stm_manifest.get_spillover_map().size(), 3);
    ASSERT_EQ(stm_manifest.size(), segs_per_spill);

    // DeleteRecords to just before the end of the local log.
    kafka_delete_records_transport deleter(make_kafka_client().get());
    deleter.start().get();
    auto new_start_offset = kafka::offset(245);
    ASSERT_EQ(
      new_start_offset,
      model::offset_cast(deleter
                           .delete_records_from_partition(
                             topic_name,
                             model::partition_id(0),
                             kafka::offset_cast(new_start_offset),
                             5s)
                           .get()));

    // The first housekeeping should remove all spillover segments.
    auto archive_start = stm_manifest.get_archive_start_offset();
    auto stm_start_before = stm_manifest.get_start_offset();
    ASSERT_EQ(archive_start, model::offset(0));
    ASSERT_TRUE(archiver->sync_for_tests().get());
    archiver->housekeeping().get();
    ASSERT_EQ(stm_manifest.get_archive_start_offset(), model::offset{});
    ASSERT_EQ(stm_manifest.get_archive_clean_offset(), model::offset{});
    ASSERT_TRUE(stm_manifest.get_spillover_map().empty());
    ASSERT_EQ(10, archiver->manifest().size());
    ASSERT_EQ(stm_start_before, stm_manifest.get_start_offset());
    ASSERT_NE(stm_manifest.get_start_kafka_offset_override(), kafka::offset{});

    // The next should remove all STM segments.
    ASSERT_TRUE(archiver->sync_for_tests().get());
    archiver->housekeeping().get();
    ASSERT_TRUE(!archiver->manifest().get_start_offset().has_value());
    ASSERT_EQ(stm_manifest.get_archive_start_offset(), model::offset{});
    ASSERT_EQ(stm_manifest.get_archive_clean_offset(), model::offset{});

    // The start offset override still exists because we haven't truncated it.
    ASSERT_NE(stm_manifest.get_start_kafka_offset_override(), kafka::offset{});

    // Produce more data and upload to cloud.
    auto produced_kafka_base_offset = model::offset_cast(
      gen.producer()
        .produce_to_partition(
          topic_name,
          model::partition_id(0),
          tests::kv_t::sequence(0, records_per_seg))
        .get());
    log->flush().get();
    log->force_roll().get();
    ASSERT_GT(produced_kafka_base_offset, new_start_offset);
    while (produced_kafka_base_offset > stm_manifest.get_next_kafka_offset()) {
        ASSERT_TRUE(archiver->sync_for_tests().get());
        ASSERT_EQ(
          archiver
            ->upload_next_candidates(
              archival::archival_stm_fence{.emit_rw_fence_cmd = false})
            .get()
            .non_compacted_upload_result.num_failed,
          0);
    }
    ASSERT_TRUE(archiver->sync_for_tests().get());
    ASSERT_EQ(
      cloud_storage::upload_result::success,
      archiver->upload_manifest("test").get());
    ASSERT_TRUE(stm_manifest.get_start_kafka_offset().has_value());
    ASSERT_EQ(
      stm_manifest.get_start_kafka_offset().value(), kafka::offset(200));

    // Segment boundaries are less predictable here in mode::v2, but
    // that's not really under test
    if (GetParam() == model::cloud_storage_segment_upload_mode::v1) {
        // When truncating the STM, we should still honor the requested start.
        archiver->housekeeping().get();
        ASSERT_TRUE(stm_manifest.get_start_kafka_offset().has_value());
        ASSERT_EQ(
          stm_manifest.get_start_kafka_offset().value(), new_start_offset);
        ASSERT_EQ(
          stm_manifest.get_start_kafka_offset_override(), kafka::offset(245));

        size_t size_above_override;
        segment_bytes_above_offset(
          stm_manifest, kafka::offset(245), std::ref(size_above_override));
        check_truncate_removes_override(size_above_override);
    }
}

// Test that truncation is applied to cloud storage as expected when a
// DeleteRecords request lands in the STM manifest.
TEST_P(delete_records_e2e_fixture, test_delete_from_stm_truncation) {
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    size_t records_per_seg = 5;
    ASSERT_GE(
      250,
      gen.batches_per_segment(records_per_seg)
        .num_segments(40)
        .additional_local_segments(10)
        .produce()
        .get());
    ASSERT_TRUE(archiver->sync_for_tests().get());
    archiver->apply_spillover().get();
    auto& stm_manifest = archiver->manifest();
    ASSERT_EQ(stm_manifest.get_spillover_map().size(), 3);
    ASSERT_EQ(stm_manifest.size(), segs_per_spill);
    kafka_delete_records_transport deleter(make_kafka_client().get());
    deleter.start().get();

    // Truncate within the STM at the bound, leaving one segment.
    auto stm_truncate_offset
      = archiver->manifest().get_last_kafka_offset().value();
    deleter
      .delete_records_from_partition(
        topic_name,
        model::partition_id(0),
        kafka::offset_cast(stm_truncate_offset),
        5s)
      .get();
    // The first housekeeping will cleanup the archive.
    ASSERT_TRUE(archiver->sync_for_tests().get());
    archiver->housekeeping().get();
    ASSERT_EQ(10, archiver->manifest().size());
    ASSERT_EQ(stm_manifest.get_archive_start_offset(), model::offset{});
    ASSERT_EQ(stm_manifest.get_archive_clean_offset(), model::offset{});
    ASSERT_TRUE(stm_manifest.get_spillover_map().empty());
    ASSERT_EQ(
      stm_manifest.get_start_kafka_offset_override(), stm_truncate_offset);

    // The next will clear the STM manifest.
    ASSERT_TRUE(archiver->sync_for_tests().get());
    archiver->housekeeping().get();
    ASSERT_EQ(1, archiver->manifest().size());
    ASSERT_EQ(stm_manifest.get_archive_start_offset(), model::offset{});
    ASSERT_EQ(
      stm_manifest.get_start_kafka_offset_override(), stm_truncate_offset);

    // Upload more and truncate the manifest past the override.
    ASSERT_TRUE(archiver->sync_for_tests().get());
    ASSERT_EQ(
      archiver
        ->upload_next_candidates(
          archival::archival_stm_fence{.emit_rw_fence_cmd = false})
        .get()
        .non_compacted_upload_result.num_failed,
      0);
    ASSERT_GT(archiver->manifest().size(), 1);
    ASSERT_EQ(
      stm_manifest.get_start_kafka_offset_override(), stm_truncate_offset);
    size_t size_above_override{};
    segment_bytes_above_offset(
      stm_manifest, stm_truncate_offset, std::ref(size_above_override));
    check_truncate_removes_override(size_above_override);
}

// Test that truncation is applied to cloud storage as expected when a
// DeleteRecords request lands in the archive.
TEST_P(delete_records_e2e_fixture, test_delete_from_archive_truncation) {
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    size_t records_per_seg = 5;
    ASSERT_GE(
      250,
      gen.batches_per_segment(records_per_seg)
        .num_segments(40)
        .additional_local_segments(10)
        .produce()
        .get());
    ASSERT_TRUE(archiver->sync_for_tests().get());
    archiver->apply_spillover().get();
    auto& stm_manifest = archiver->manifest();
    ASSERT_EQ(stm_manifest.get_spillover_map().size(), 3);
    ASSERT_EQ(stm_manifest.size(), segs_per_spill);
    kafka_delete_records_transport deleter(make_kafka_client().get());
    deleter.start().get();

    // Truncate within the bounds of the first archive segment. Nothing should
    // be removed.
    auto& spillover = stm_manifest.get_spillover_map();
    auto first_seg_commit_offset
      = *spillover.get_committed_offset_column().at_index(0);
    auto first_seg_delta_end
      = *spillover.get_delta_offset_end_column().at_index(0);
    auto first_seg_last_kafka_offset = first_seg_commit_offset
                                       - first_seg_delta_end;
    deleter
      .delete_records_from_partition(
        topic_name,
        model::partition_id(0),
        model::offset(first_seg_last_kafka_offset),
        5s)
      .get();
    ASSERT_TRUE(archiver->sync_for_tests().get());
    archiver->housekeeping().get();
    ASSERT_EQ(model::offset(0), stm_manifest.get_archive_start_offset());
    ASSERT_EQ(stm_manifest.get_spillover_map().size(), 3);
    ASSERT_EQ(stm_manifest.size(), segs_per_spill);

    // Now truncate just past the bounds of the first archive segment. It
    // should actually be removed.
    deleter
      .delete_records_from_partition(
        topic_name,
        model::partition_id(0),
        model::offset(first_seg_last_kafka_offset + 1),
        5s)
      .get();
    ASSERT_TRUE(archiver->sync_for_tests().get());
    archiver->housekeeping().get();
    auto new_archive_start = stm_manifest.get_archive_start_offset();
    ASSERT_GT(new_archive_start, model::offset(0));
    ASSERT_EQ(stm_manifest.get_spillover_map().size(), 3);
    ASSERT_EQ(stm_manifest.size(), segs_per_spill);
    ASSERT_NE(stm_manifest.get_start_kafka_offset_override(), kafka::offset{});

    // Truncate right at the end of the archive. The archive should retain a
    // single segment.
    auto stm_kafka_start = stm_manifest.get_start_kafka_offset().value();
    deleter
      .delete_records_from_partition(
        topic_name,
        model::partition_id(0),
        model::offset(stm_kafka_start() - 1),
        5s)
      .get();
    ASSERT_TRUE(archiver->sync_for_tests().get());
    archiver->housekeeping().get();
    ASSERT_GT(stm_manifest.get_archive_start_offset(), new_archive_start);
    ASSERT_EQ(stm_manifest.size(), segs_per_spill);
    ASSERT_NE(stm_manifest.get_start_kafka_offset_override(), kafka::offset{});

    // Truncate to the beginning of the STM manifest. This should remove the
    // archive entirely.
    deleter
      .delete_records_from_partition(
        topic_name,
        model::partition_id(0),
        model::offset(stm_kafka_start()),
        5s)
      .get();
    ASSERT_TRUE(archiver->sync_for_tests().get());
    archiver->housekeeping().get();
    ASSERT_EQ(stm_manifest.get_archive_start_offset(), model::offset{});
    ASSERT_EQ(stm_manifest.get_archive_clean_offset(), model::offset{});
    ASSERT_TRUE(stm_manifest.get_spillover_map().empty());
    ASSERT_EQ(stm_manifest.size(), segs_per_spill);
    ASSERT_EQ(stm_manifest.get_start_kafka_offset_override(), stm_kafka_start);

    // Truncate the manifest past the override.
    size_t size_above_override{};
    segment_bytes_above_offset(
      stm_manifest, stm_kafka_start, std::ref(size_above_override));
    check_truncate_removes_override(size_above_override);
}

INSTANTIATE_TEST_SUITE_P(
  DeleteRecordsE2E,
  delete_records_e2e_fixture,
  ::testing::Values(
    model::cloud_storage_segment_upload_mode::v1,
    model::cloud_storage_segment_upload_mode::v2));
