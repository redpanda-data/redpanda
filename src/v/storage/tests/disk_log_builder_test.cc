// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "storage/ntp_config.h"
#include "storage/tests/disk_log_builder_fixture.h"
#include "test_utils/test_macros.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <optional>

TEST_F(log_builder_fixture, kitchen_sink) {
    using namespace storage; // NOLINT

    auto batch = model::test::make_random_batch(model::offset(104), 1, false);

    b | start() | add_segment(0)
      | add_random_batch(0, 100, maybe_compress_batches::yes)
      | add_random_batch(100, 2, maybe_compress_batches::yes) | add_segment(102)
      | add_random_batch(102, 2, maybe_compress_batches::yes) | add_segment(104)
      | add_batch(std::move(batch)) | add_random_batches(105, 3);

    auto stats = get_stats().get();

    b | stop();
    EXPECT_EQ(stats.seg_count, 3);
    EXPECT_EQ(stats.batch_count, 7);
    EXPECT_GE(stats.record_count, 105);
}

TEST_F(log_builder_fixture, size_bytes_after_offset) {
    using namespace storage;
    // see issues/15417, this test segfaults on the first block and returns
    // wrong results on the rest

    {
        SCOPED_TRACE("empty log (sanity check)");
        b | start();
        auto _ = ss::defer([&] { b | stop(); });
        EXPECT_EQ(get_stats().get().seg_count, 0);
        EXPECT_EQ(
          b.get_disk_log_impl().size_bytes_after_offset(model::offset::min()),
          0);
        EXPECT_EQ(
          b.get_disk_log_impl().size_bytes_after_offset(model::offset{0}), 0);
        EXPECT_EQ(
          b.get_disk_log_impl().size_bytes_after_offset(model::offset::max()),
          0);
    }

    {
        SCOPED_TRACE("one segment");
        b | start() | add_segment(0) | add_random_batch(0, 100);
        auto _ = ss::defer([&] { b | stop(); });

        EXPECT_EQ(get_stats().get().seg_count, 1);
        EXPECT_GT(
          b.get_disk_log_impl().size_bytes_after_offset(model::offset::min()),
          0);
        EXPECT_GT(
          b.get_disk_log_impl().size_bytes_after_offset(model::offset{0}), 0);
        EXPECT_EQ(
          b.get_disk_log_impl().size_bytes_after_offset(model::offset::max()),
          0);
    }

    {
        SCOPED_TRACE("more than one segment");
        b | start() | add_segment(0) | add_random_batch(0, 100)
          | add_segment(100) | add_random_batch(100, 100);
        auto _ = ss::defer([&] { b | stop(); });

        EXPECT_EQ(get_stats().get().seg_count, 2);
        EXPECT_GT(
          b.get_disk_log_impl().size_bytes_after_offset(model::offset::min()),
          0);
        EXPECT_GT(
          b.get_disk_log_impl().size_bytes_after_offset(model::offset{0}), 0);
        EXPECT_GT(
          b.get_disk_log_impl().size_bytes_after_offset(model::offset{100}), 0);
        EXPECT_EQ(
          b.get_disk_log_impl().size_bytes_after_offset(model::offset::max()),
          0);
    }

    {
        SCOPED_TRACE("In the middle of the segment");
        b | start() | add_segment(0)
          | add_random_batches(
            model::offset(0), 2000, maybe_compress_batches::no);

        auto next = model::next_offset(b.get_log()->offsets().dirty_offset);
        b | add_segment(next)
          | add_random_batches(next, 2000, maybe_compress_batches::no);

        auto _ = ss::defer([&] { b | stop(); });
        disk_log_impl& d_log = b.get_disk_log_impl();
        EXPECT_EQ(get_stats().get().seg_count, 2);
        auto& s0 = d_log.segments()[0];
        auto& s1 = d_log.segments()[1];
        // all data
        EXPECT_EQ(
          d_log.size_bytes_after_offset(model::offset(0)),
          s0->size_bytes() + s1->size_bytes());
        // somewhere in the middle of the first segment
        auto offset = model::offset(s0->offsets().get_committed_offset()() / 2);
        EXPECT_GT(d_log.size_bytes_after_offset(offset), s1->size_bytes());

        // somewhere in the middle of the second segment
        auto offset_2 = s1->offsets().get_base_offset()
                        + model::offset(
                          s1->offsets().get_committed_offset()
                          - s1->offsets().get_base_offset() / 2);
        EXPECT_LT(d_log.size_bytes_after_offset(offset_2), s1->size_bytes());

        // only the second segment
        EXPECT_EQ(
          d_log.size_bytes_after_offset(s0->offsets().get_committed_offset()),
          s1->size_bytes());
    }
}

static void do_write_zeroes(ss::sstring name) {
    auto fd = ss::open_file_dma(
                name, ss::open_flags::create | ss::open_flags::rw)
                .get();
    auto out = ss::make_file_output_stream(std::move(fd)).get();
    ss::temporary_buffer<char> b(4096);
    std::memset(b.get_write(), 0, 4096);
    out.write(b.get(), b.size()).get();
    out.flush().get();
    out.close().get();
}

static void do_write_garbage(ss::sstring name) {
    auto fd = ss::open_file_dma(
                name, ss::open_flags::create | ss::open_flags::rw)
                .get();
    auto out = ss::make_file_output_stream(std::move(fd)).get();
    const auto b = random_generators::gen_alphanum_string(100);
    out.write(b.data(), b.size()).get();
    out.flush().get();
    out.close().get();
}

TEST_F(log_builder_fixture, test_valid_segment_name_with_zeroes_data) {
    using namespace storage; // NOLINT
    auto ntp = model::ntp(
      model::ns("test.ns"), model::topic("zeroes"), model::partition_id(66));
    storage::ntp_config ncfg(ntp, b.get_log_config().base_dir);
    const ss::sstring dir = ncfg.work_directory();
    // 1. write valid segment names in the namespace with 0 data so crc passes
    recursive_touch_directory(dir).get();
    do_write_zeroes(ssx::sformat("{}/270-1850-v1.log", dir));
    do_write_zeroes(ssx::sformat("{}/270-1850-v1.base_index", dir));
    do_write_garbage(ssx::sformat("{}/271-1850-v1.log", dir));
    do_write_garbage(ssx::sformat("{}/271-1850-v1.base_index", dir));

    b | start(ntp);
    auto stats = get_stats().get();
    b | stop();

    ASSERT_FALSE(
      ss::file_exists(ssx::sformat("{}/270-1850-v1.log", dir)).get());
    EXPECT_EQ(stats.seg_count, 0);
    EXPECT_EQ(stats.batch_count, 0);
    EXPECT_GE(stats.record_count, 0);
}

TEST_F(log_builder_fixture, iterator_invalidation) {
    using namespace storage; // NOLINT
    constexpr const model::record_batch_type configuration
      = model::record_batch_type::raft_configuration;
    constexpr const model::record_batch_type data
      = model::record_batch_type::raft_data;

    b | start() | add_segment(0)
      | add_random_batch(0, 1, maybe_compress_batches::yes, configuration)
      | add_segment(1)
      | add_random_batch(1, 1, maybe_compress_batches::yes, data)
      | add_segment(2)
      | add_random_batch(2, 1, maybe_compress_batches::yes, configuration)
      | add_segment(3)
      | add_random_batch(3, 1, maybe_compress_batches::yes, data);

    auto data_batches = b.consume(local_log_reader_config(
                                    model::offset(0),
                                    model::model_limits<model::offset>::max(),
                                    std::numeric_limits<size_t>::max(),
                                    data,
                                    std::nullopt,
                                    std::nullopt))
                          .get();
    auto config_batches = b.consume(local_log_reader_config(
                                      model::offset(0),
                                      model::model_limits<model::offset>::max(),
                                      std::numeric_limits<size_t>::max(),
                                      configuration,
                                      std::nullopt,
                                      std::nullopt))
                            .get();
    auto all_batches = b.consume().get();
    b | stop();
    ASSERT_EQ(data_batches.size(), 2);
    ASSERT_EQ(config_batches.size(), 2);
    ASSERT_EQ(all_batches.size(), 4);
}

TEST_F(log_builder_fixture, test_skipping_compaction_below_start_offset) {
    using namespace storage;

    ss::abort_source abs;
    temporary_dir tmp_dir("storage_e2e");
    auto data_path = tmp_dir.get_path();

    storage::ntp_config config{{"test_ns", "test_tpc", 0}, {data_path}};

    storage::ntp_config::default_overrides overrides;
    overrides.retention_bytes = tristate<size_t>{1};
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction
        | model::cleanup_policy_bitflags::deletion;

    config.set_overrides(overrides);

    // Create a log and populate it with two segments,
    // while making sure to close the first segment before
    // opening the second.
    b | start(std::move(config));

    auto& log = b.get_disk_log_impl();

    b | add_segment(0) | add_random_batch(0, 100);

    log.force_roll().get();

    b | add_segment(100) | add_random_batch(100, 100);

    ASSERT_EQ(log.segment_count(), 2);

    housekeeping_config cfg{
      model::timestamp::max(),
      1,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      std::chrono::milliseconds{0},
      abs};

    // Call into `disk_log_impl::gc` and listen for the eviction
    // notification being created.
    auto eviction_future = log.monitor_eviction(abs);
    auto new_start_offset = b.apply_retention(cfg.gc).get();
    ASSERT_TRUE(new_start_offset);

    ASSERT_EQ(log.segment_count(), 2);

    // Grab the new start offset from the notification and
    // update the removable offset and start offsets.
    auto evict_at_offset = eviction_future.get();
    ASSERT_EQ(*new_start_offset, model::next_offset(evict_at_offset));
    ASSERT_TRUE(b.update_start_offset(*new_start_offset).get());

    // Call into `disk_log_impl::compact`. The only segment eligible for
    // compaction is the below the start offset and it should be ignored.
    auto& first_seg = log.segments().front();
    ASSERT_EQ(first_seg->has_self_compact_timestamp(), false);

    b.apply_adjacent_merge_compaction(cfg.compact, *new_start_offset).get();

    ASSERT_EQ(first_seg->has_self_compact_timestamp(), false);

    b.stop().get();
}
