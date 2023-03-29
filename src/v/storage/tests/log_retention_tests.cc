// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "storage/tests/utils/disk_log_builder.h"
// fixture
#include "test_utils/fixture.h"

#include <optional>

struct gc_fixture {
    storage::disk_log_builder builder;
};

FIXTURE_TEST(empty_log_garbage_collect, gc_fixture) {
    builder | storage::start()
      | storage::garbage_collect(
        model::timestamp::now(), std::make_optional<size_t>(1024))
      | storage::stop();
}

FIXTURE_TEST(retention_test_time, gc_fixture) {
    auto base_ts = model::timestamp{123000};
    builder.set_time(base_ts);
    builder | storage::start() | storage::add_segment(0)
      | storage::add_random_batch(0, 100, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(100, 2, storage::maybe_compress_batches::yes)
      | storage::add_segment(102)
      | storage::add_random_batch(102, 2, storage::maybe_compress_batches::yes)
      | storage::add_segment(104) | storage::add_random_batches(104, 3);
    BOOST_TEST_MESSAGE(
      "Should not collect segments with timestamp older than 1");
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 3);
    builder | storage::garbage_collect(model::timestamp(1), std::nullopt);
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 3);

    BOOST_TEST_MESSAGE("Should not collect segments because size is infinity");
    builder
      | storage::garbage_collect(
        model::timestamp(1),
        std::make_optional<size_t>(std::numeric_limits<size_t>::max()));
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 3);

    BOOST_TEST_MESSAGE("Should leave one active segment");
    builder
      | storage::garbage_collect(
        model::timestamp{base_ts() + 1000}, std::nullopt)
      | storage::stop();

    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 1);
}

FIXTURE_TEST(retention_test_size, gc_fixture) {
    builder | storage::start() | storage::add_segment(0)
      | storage::add_random_batch(0, 100, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(100, 2, storage::maybe_compress_batches::yes)
      | storage::add_segment(102)
      | storage::add_random_batch(102, 2, storage::maybe_compress_batches::yes)
      | storage::add_segment(104) | storage::add_random_batches(104, 3);
    BOOST_TEST_MESSAGE("Should not collect segments because size equal to "
                       "current partition size");
    builder
      | storage::garbage_collect(
        model::timestamp(1),
        std::make_optional(
          builder.get_disk_log_impl().get_probe().partition_size()));
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 3);

    BOOST_TEST_MESSAGE("Should collect all segments");
    builder
      | storage::garbage_collect(model::timestamp(1), std::optional<size_t>(0))
      | storage::stop();

    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 0);
}

/*
 * test that both time and size are applied. the test works like this:
 *
 * Three segments
 * - active segment (1mb, ignored)
 * - third segment (1mb)
 * - second segment (1mb)
 * - first segment (2mb)
 * - all records have "last week" timestamp
 * - retention size is set to 4 mb
 *
 * When gc is run size based retention will remove the first segment which will
 * satisfy size based retention goals. however, time based retention was trigger
 * for all batches. so we expect that all segments except the active segment to
 * be removed.
 */
FIXTURE_TEST(retention_test_size_time, gc_fixture) {
    const auto last_week = model::to_timestamp(
      model::timestamp_clock::now() - std::chrono::days(7));

    const auto yesterday = model::to_timestamp(
      model::timestamp_clock::now() - std::chrono::days(1));

    const size_t num_records = 10;
    const auto part_size = [this] {
        return builder.get_disk_log_impl().get_probe().partition_size();
    };

    // first segment
    model::offset offset{0};
    builder | storage::start() | storage::add_segment(offset);
    size_t start_size = part_size();
    while ((part_size() - start_size) < 2_MiB) {
        builder
          | storage::add_random_batch(
            offset,
            num_records,
            storage::maybe_compress_batches::no,
            model::record_batch_type::raft_data,
            storage::append_config(),
            storage::disk_log_builder::should_flush_after::no,
            last_week);
        offset += model::offset(num_records);
    }

    BOOST_CHECK_LT(
      (builder.disk_usage(model::timestamp::now(), 0).get().reclaim.retention
       - 2_MiB),
      20_KiB);
    BOOST_CHECK_LT(
      (builder.disk_usage(model::timestamp::now(), 0).get().usage.total()
       - 2_MiB),
      20_KiB);

    // second segment
    builder | storage::add_segment(offset);
    start_size = part_size();
    while ((part_size() - start_size) < 1_MiB) {
        builder
          | storage::add_random_batch(
            offset,
            num_records,
            storage::maybe_compress_batches::no,
            model::record_batch_type::raft_data,
            storage::append_config(),
            storage::disk_log_builder::should_flush_after::no,
            last_week);
        offset += model::offset(num_records);
    }

    // the first segment is now eligible for reclaim
    BOOST_CHECK_LT(
      (builder.disk_usage(model::timestamp::now(), 0).get().reclaim.retention
       - 3_MiB),
      20_KiB);
    BOOST_CHECK_LT(
      (builder.disk_usage(model::timestamp::now(), 0).get().usage.total()
       - 3_MiB),
      20_KiB);

    // third segment
    builder | storage::add_segment(offset);
    start_size = part_size();
    while ((part_size() - start_size) < 1_MiB) {
        builder
          | storage::add_random_batch(
            offset,
            num_records,
            storage::maybe_compress_batches::no,
            model::record_batch_type::raft_data,
            storage::append_config(),
            storage::disk_log_builder::should_flush_after::no,
            last_week);
        offset += model::offset(num_records);
    }

    // the first,second segment is now eligible for reclaim
    BOOST_CHECK_LT(
      (builder.disk_usage(model::timestamp::now(), 0).get().reclaim.retention
       - 4_MiB),
      20_KiB);
    BOOST_CHECK_LT(
      (builder.disk_usage(model::timestamp::now(), 0).get().usage.total()
       - 4_MiB),
      20_KiB);

    // active segment
    builder | storage::add_segment(offset);
    start_size = part_size();
    while ((part_size() - start_size) < 1_MiB) {
        builder
          | storage::add_random_batch(
            offset,
            num_records,
            storage::maybe_compress_batches::no,
            model::record_batch_type::raft_data,
            storage::append_config(),
            storage::disk_log_builder::should_flush_after::no,
            last_week);
        offset += model::offset(num_records);
    }

    // the first,second segment is now eligible for reclaim
    BOOST_CHECK_LT(
      (builder.disk_usage(model::timestamp::now(), 0).get().reclaim.retention
       - 5_MiB),
      20_KiB);
    BOOST_CHECK_LT(
      (builder.disk_usage(model::timestamp::now(), 0).get().usage.total()
       - 5_MiB),
      20_KiB);

    builder | storage::garbage_collect(yesterday, 4_MiB);

    // right after gc runs there shouldn't be anything reclaimable
    BOOST_CHECK_EQUAL(
      builder.disk_usage(model::timestamp::now(), 0).get().reclaim.retention,
      0);
    BOOST_CHECK_EQUAL(
      builder.disk_usage(model::timestamp::now(), 0).get().usage.total(), 0);

    builder | storage::stop();

    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 0);
}
FIXTURE_TEST(retention_test_after_truncation, gc_fixture) {
    BOOST_TEST_MESSAGE("Should be safe to garbage collect after truncation");
    builder | storage::start() | storage::add_segment(0)
      | storage::add_random_batch(0, 100, storage::maybe_compress_batches::yes)
      | storage::truncate_log(model::offset(0))
      | storage::garbage_collect(model::timestamp::now(), std::nullopt)
      | storage::stop();
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 0);
    BOOST_CHECK_EQUAL(
      builder.get_disk_log_impl().get_probe().partition_size(), 0);
}

FIXTURE_TEST(retention_by_size_with_remote_write, gc_fixture) {
    /*
     * This test sets the size retention limit on a cloud storage topic
     * via the rention.local.target.bytes topic configuration option.
     *
     * Fixed size segments are added until the limit is breached.
     * After each segment is added compaction is triggered and we check
     * if it acted correctly.
     */

    config::shard_local_cfg().get("cloud_storage_enabled").set_value(true);

    size_t size_limit = 1000;

    storage::ntp_config config{
      storage::log_builder_ntp(), builder.get_log_config().base_dir};

    storage::ntp_config::default_overrides overrides;
    overrides.shadow_indexing_mode = model::shadow_indexing_mode::full;
    overrides.retention_local_target_bytes = tristate<size_t>{size_limit};
    config.set_overrides(overrides);

    auto batch_builder = [](size_t offset, size_t size) {
        return model::test::make_random_batch(
          model::offset{offset},
          1,
          true,
          model::record_batch_type::raft_data,
          std::vector<size_t>{size});
    };

    auto partition_size = 0;
    size_t dirty_offset = 0;

    builder.start(std::move(config)).get();
    while (partition_size <= size_limit) {
        builder.add_segment(model::offset{dirty_offset}).get();
        builder.add_batch(batch_builder(dirty_offset, 100)).get();

        ++dirty_offset;
        partition_size
          = builder.get_disk_log_impl().get_probe().partition_size();

        auto segment_count_before_gc = builder.get_log().segment_count();
        builder
          .gc(
            model::timestamp(1),
            std::make_optional<size_t>(std::numeric_limits<size_t>::max()))
          .get();
        auto segment_count_after_gc = builder.get_log().segment_count();

        if (partition_size > size_limit) {
            BOOST_CHECK_GT(segment_count_before_gc, segment_count_after_gc);
        } else {
            BOOST_CHECK_EQUAL(segment_count_before_gc, segment_count_after_gc);
        }
    }

    auto final_partition_size
      = builder.get_disk_log_impl().get_probe().partition_size();

    BOOST_CHECK_GE(size_limit, final_partition_size);

    builder.stop().get();
}

FIXTURE_TEST(retention_by_time_with_remote_write, gc_fixture) {
    /*
     * This test sets the time retention limit on a cloud storage topic
     * via the rention.local.target.ms topic configuration option.
     */
    using namespace std::chrono_literals;
    auto batch_age = std::chrono::duration_cast<std::chrono::milliseconds>(1h);

    config::shard_local_cfg().get("cloud_storage_enabled").set_value(true);

    storage::ntp_config config{
      storage::log_builder_ntp(), builder.get_log_config().base_dir};

    storage::ntp_config::default_overrides overrides;
    overrides.shadow_indexing_mode = model::shadow_indexing_mode::full;
    config.set_overrides(overrides);

    auto log_creation_time = model::timestamp{
      model::timestamp::now().value() - batch_age.count()};

    // Create two log segments that are 1h old.
    builder | storage::start(std::move(config)) | storage::add_segment(0)
      | storage::add_random_batch(
        0,
        100,
        storage::maybe_compress_batches::yes,
        model::record_batch_type::raft_data,
        storage::append_config(),
        storage::disk_log_builder::should_flush_after::yes,
        log_creation_time)
      | storage::add_segment(100)
      | storage::add_random_batch(
        100,
        100,
        storage::maybe_compress_batches::yes,
        model::record_batch_type::raft_data,
        storage::append_config(),
        storage::disk_log_builder::should_flush_after::yes,
        log_creation_time);

    // Try to garbage collet the segments. None should get collected
    // because we are currently using the default local target retention.
    builder | storage::garbage_collect(model::timestamp{1}, std::nullopt);
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 2);

    // Override the local target retention.
    storage::ntp_config::default_overrides time_override;
    time_override.shadow_indexing_mode = model::shadow_indexing_mode::full;
    time_override.retention_local_target_ms
      = tristate<std::chrono::milliseconds>{0ms};
    builder.update_configuration(time_override).get();

    // Collect again. All segments should be removed this time.
    builder | storage::garbage_collect(model::timestamp{1}, std::nullopt)
      | storage::stop();
    BOOST_CHECK_EQUAL(builder.get_log().segment_count(), 0);
}

FIXTURE_TEST(non_collectible_disk_usage_test, gc_fixture) {
    // a few helpers
    const auto last_week = model::to_timestamp(
      model::timestamp_clock::now() - std::chrono::days(7));
    const size_t num_records = 10;
    const auto part_size = [this] {
        return builder.get_disk_log_impl().get_probe().partition_size();
    };

    // setup non-collectible
    auto overrides = std::make_unique<storage::ntp_config::default_overrides>();
    overrides->cleanup_policy_bitflags = model::cleanup_policy_bitflags::none;
    storage::ntp_config config(
      storage::log_builder_ntp(),
      builder.get_log_config().base_dir,
      std::move(overrides));

    // first segment
    model::offset offset{0};
    builder | storage::start(std::move(config)) | storage::add_segment(offset);
    size_t start_size = part_size();
    while ((part_size() - start_size) < 2_MiB) {
        builder
          | storage::add_random_batch(
            offset,
            num_records,
            storage::maybe_compress_batches::no,
            model::record_batch_type::raft_data,
            storage::append_config(),
            storage::disk_log_builder::should_flush_after::no,
            last_week);
        offset += model::offset(num_records);
    }

    // second segment
    builder | storage::add_segment(offset);
    start_size = part_size();
    while ((part_size() - start_size) < 1_MiB) {
        builder
          | storage::add_random_batch(
            offset,
            num_records,
            storage::maybe_compress_batches::no,
            model::record_batch_type::raft_data,
            storage::append_config(),
            storage::disk_log_builder::should_flush_after::no,
            last_week);
        offset += model::offset(num_records);
    }

    BOOST_REQUIRE_EQUAL(
      builder.get_disk_log_impl().config().is_collectable(), false);

    BOOST_CHECK_EQUAL(
      builder.disk_usage(model::timestamp::now(), 0).get().reclaim.retention,
      0);
    BOOST_CHECK_LT(
      (builder.disk_usage(model::timestamp::now(), 0).get().usage.total()
       - 3_MiB),
      20_KiB);

    builder | storage::stop();
}
