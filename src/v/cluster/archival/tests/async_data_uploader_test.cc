/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/async_data_uploader.h"
#include "cluster/archival/tests/async_data_uploader_fixture.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "random/generators.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

using namespace std::chrono_literals;
using namespace archival;

static ss::logger test_log("async-uploader-test");

TEST(AsyncDataUploderTest, test_inclusive_offset_range1) {
    inclusive_offset_range range(model::offset(3), model::offset(3));
    std::vector<model::offset> res;
    for (const auto o : range) {
        res.push_back(o);
    }
    ASSERT_TRUE(res.size() == 1);
    ASSERT_TRUE(res.at(0) == model::offset(3));
}

TEST(AsyncDataUploaderTest, test_inclusive_offset_range2) {
    inclusive_offset_range range(model::offset(3), model::offset(7));
    std::vector<model::offset> res;
    for (const auto o : range) {
        res.push_back(o);
    }
    ASSERT_TRUE(res.size() == 5);
    std::vector<model::offset> expected(
      {model::offset(3),
       model::offset(4),
       model::offset(5),
       model::offset(6),
       model::offset(7)});
    ASSERT_TRUE(res == expected);
}

class AsyncDataUploaderFixture
  : public async_data_uploader_fixture
  , public ::testing::Test {};

using namespace archival;

TEST_F(AsyncDataUploaderFixture, test_async_segment_upload_full_non_compacted) {
    random_records_generator generator;
    generate_partition(1000, generator);

    auto partition = get_test_partition();

    ASSERT_TRUE(get_partition_log()->segments().size() == 1);

    // Produce upload based on reverse-consuming from the partition
    // and compare it to the log segment
    const auto& offsets = partition->log()->offsets();
    vlog(
      test_log.info,
      "Offset range: {}-{}",
      offsets.start_offset,
      offsets.committed_offset);

    auto range = inclusive_offset_range(
      offsets.start_offset, offsets.committed_offset);
    std::optional<iobuf> actual;
    read_offset_range(range, actual);
    auto expected = load_log_segment(
      get_partition_log()->segments().front(), range);

    ASSERT_TRUE(actual.value() == expected);
}

TEST_F(AsyncDataUploaderFixture, test_async_segment_upload_full_compacted) {
    random_records_generator generator;
    generator.key_space_size = 10;
    generate_partition(1000, generator, true);

    auto partition = get_test_partition();
    // Capture the offsets before the log is rolled or anything
    // is compacted.
    const auto& offsets = partition->log()->offsets();
    auto range = inclusive_offset_range(
      offsets.start_offset, offsets.committed_offset);

    vlog(
      test_log.info,
      "Offset range: {}-{}",
      offsets.start_offset,
      offsets.committed_offset);

    // Calculate original segment size to make sure that it's compacted.
    auto non_compacted_size = load_log_segment(
                                get_partition_log()->segments().front(), range)
                                .size_bytes();

    // Force roll and compaction
    roll_segment();
    compact_segments(range.last);

    std::optional<iobuf> actual;
    read_offset_range(range, actual, true);
    auto expected = load_log_segment(
      get_partition_log()->segments().front(), range);

    vlog(
      test_log.info,
      "Non-compacted size: {}, compacted size: {}, upload size: {}",
      non_compacted_size,
      expected.size_bytes(),
      actual->size_bytes());

    ASSERT_TRUE(actual.value() == expected);
}

TEST_F(
  AsyncDataUploaderFixture, test_async_segment_upload_partial_non_compacted) {
    random_records_generator generator;
    generate_partition(1000, generator);
    auto log_map = get_log_map();
    auto partition = get_test_partition();

    ASSERT_TRUE(get_partition_log()->segments().size() == 1);

    // Produce upload based on reverse-consuming from the partition
    // and compare it to the log segment
    const auto& offsets = partition->log()->offsets();

    auto f_it = log_map.base_offsets.begin();
    f_it++;
    f_it++;
    auto b_it = log_map.base_offsets.rbegin();
    b_it++;
    b_it++;

    auto range = inclusive_offset_range(*f_it, *b_it);

    vlog(
      test_log.info,
      "Partition offset range: {}-{}",
      offsets.start_offset,
      offsets.committed_offset);

    vlog(test_log.info, "Upload offset range: {}-{}", range.base, range.last);

    std::optional<iobuf> actual;
    read_offset_range(range, actual);
    auto expected = load_log_segment_concat(range);

    ASSERT_TRUE(actual.value() == expected);
}
namespace {
struct fuzz_test_case {
    model::offset base;
    model::offset last;

    size_t expected_size{0};
};

template<class Fn>
inline void get_random_test_cases_impl(
  std::vector<fuzz_test_case>& cases,
  AsyncDataUploaderFixture::log_map& map,
  int num_results,
  Fn pick_offsets) {
    // Generate a bunch of random test cases
    std::vector<size_t> ix;
    for (size_t i = 0; i < map.base_offsets.size(); i++) {
        ix.push_back(i);
        vlog(
          test_log.debug,
          "log_map {} - {} - {}",
          map.base_offsets[i],
          map.timestamps[i],
          map.batch_types[i]);
    }

    for (int i = 0; i < num_results; i++) {
        auto [ix_first, ix_second] = pick_offsets(ix.size());
        // compute size of the offset range
        size_t range_size = 0;
        for (size_t ix = ix_first; ix <= ix_second; ix++) {
            range_size += map.batch_sizes.at(ix);
        }
        fuzz_test_case tc{
          .base = map.base_offsets.at(ix_first),
          .last = map.last_offsets.at(ix_second),
          .expected_size = range_size,
        };
        vlog(
          test_log.debug,
          "Test case #{}/{}: offsets: {}-{}",
          i,
          num_results,
          tc.base,
          tc.last);
        cases.push_back(tc);
    }
}

/// Generate randomized test cases with arbitrary alignment
inline void get_random_test_cases(
  std::vector<fuzz_test_case>& out,
  AsyncDataUploaderFixture::log_map& map,
  int num_results) {
    auto fn = [&](size_t ix_size) {
        auto ix_first = random_generators::get_int((size_t)0, ix_size - 1);
        auto ix_second = random_generators::get_int(ix_first, ix_size - 1);
        return std::make_pair(ix_first, ix_second);
    };
    get_random_test_cases_impl(out, map, num_results, fn);
}

/// Generate randomized test cases that contains only single batches
inline void get_random_single_batch_test_cases(
  std::vector<fuzz_test_case>& out,
  AsyncDataUploaderFixture::log_map& map,
  int num_results) {
    auto fn = [&](size_t ix_size) {
        auto ix_first = random_generators::get_int((size_t)0, ix_size - 1);
        return std::make_pair(ix_first, ix_first);
    };
    get_random_test_cases_impl(out, map, num_results, fn);
}

inline void get_random_test_cases_that_start_on_config_batches(
  std::vector<fuzz_test_case>& out,
  AsyncDataUploaderFixture::log_map& map,
  int num_results) {
    std::vector<size_t> ix_config;
    for (size_t i = 0; i < map.base_offsets.size(); i++) {
        if (map.batch_types[i] != model::record_batch_type::raft_data) {
            ix_config.push_back(i);
        }
    }
    // Produce a test case that starts at config batch
    auto start_fn = [&](size_t ix_size) {
        auto i = random_generators::get_int((size_t)0, ix_config.size() - 1);
        auto ix_first = ix_config[i];
        auto ix_second = random_generators::get_int(ix_first, ix_size - 1);
        return std::make_pair(ix_first, ix_second);
    };
    // Produce a test case that end at config batch
    auto stop_fn = [&](size_t) {
        auto i = random_generators::get_int((size_t)0, ix_config.size() - 1);
        auto ix_second = ix_config[i];
        auto ix_first = random_generators::get_int((size_t)0, ix_second);
        return std::make_pair(ix_first, ix_second);
    };
    // Produce a test case that starts and stops at config batches
    auto full_fn = [&](size_t) {
        auto i = random_generators::get_int((size_t)0, ix_config.size() - 1);
        auto j = random_generators::get_int(i, ix_config.size() - 1);
        auto ix_first = ix_config[i];
        auto ix_second = ix_config[j];
        return std::make_pair(ix_first, ix_second);
    };
    get_random_test_cases_impl(out, map, num_results / 3, start_fn);
    get_random_test_cases_impl(out, map, num_results / 3, stop_fn);
    get_random_test_cases_impl(out, map, num_results / 3, full_fn);
}

int num_cases_small() {
#ifdef NDEBUG
    return 100;
#else
    return 10;
#endif
}

int num_cases_large() {
#ifdef NDEBUG
    return 500;
#else
    return 50;
#endif
}

} // namespace

TEST_F(
  AsyncDataUploaderFixture, test_async_segment_upload_random_not_compacted) {
    vlog(
      test_log.info,
      "Seed used for the test: {}",
      random_generators::global().initial_seed());
    create_topic();
    for (int i = 0; i < 10; i++) {
        random_records_generator generator;
        produce_data(100, generator);
        ss::abort_source as;
        get_test_partition()
          ->archival_meta_stm()
          ->cleanup_archive(
            model::offset(0), 0, ss::lowres_clock::now() + 1s, as)
          .get();
        roll_segment();
    }
    auto log_map = get_log_map();
    auto partition = get_test_partition();

    std::vector<fuzz_test_case> cases;
    get_random_test_cases(cases, log_map, num_cases_small());
    get_random_single_batch_test_cases(cases, log_map, num_cases_small());
    get_random_test_cases_that_start_on_config_batches(
      cases, log_map, num_cases_small());

    const auto& offsets = partition->log()->offsets();
    vlog(
      test_log.info,
      "Partition offset range: {}-{}",
      offsets.start_offset,
      offsets.committed_offset);

    for (auto tc : cases) {
        vlog(test_log.info, "Test case, offsets: {}-{}", tc.base, tc.last);

        auto range = inclusive_offset_range(tc.base, tc.last);
        std::optional<iobuf> actual;
        read_offset_range(range, actual);
        auto expected = load_log_segment_concat(range);

        ASSERT_TRUE(actual.has_value());
        ASSERT_EQ(actual.value(), expected);
        ASSERT_EQ(actual.value().size_bytes(), tc.expected_size);
    }
}

TEST_F(AsyncDataUploaderFixture, test_async_segment_upload_random_compacted) {
    vlog(
      test_log.info,
      "Seed used for the test: {}",
      random_generators::global().initial_seed());
    create_topic(true);
    for (int i = 0; i < 10; i++) {
        random_records_generator generator;
        generator.key_space_size = 5;
        produce_data(100, generator);
        ss::abort_source as;
        get_test_partition()
          ->archival_meta_stm()
          ->cleanup_archive(
            model::offset(0), 0, ss::lowres_clock::now() + 1s, as)
          .get();
        roll_segment();
    }

    size_t pre_compaction_size = get_on_disk_size();

    vlog(
      test_log.info,
      "High watermark: {}",
      get_test_partition()->high_watermark());

    roll_segment();
    compact_segments(get_test_partition()->log()->offsets().committed_offset);

    size_t post_compaction_size = get_on_disk_size();

    vlog(
      test_log.info,
      "Non-compacted size: {}, compacted size: {}",
      pre_compaction_size,
      post_compaction_size);

    auto log_map = get_log_map();
    auto partition = get_test_partition();

    std::vector<fuzz_test_case> cases;
    get_random_test_cases(cases, log_map, num_cases_small());
    get_random_single_batch_test_cases(cases, log_map, num_cases_small());
    get_random_test_cases_that_start_on_config_batches(
      cases, log_map, num_cases_small());

    const auto& offsets = partition->log()->offsets();
    vlog(
      test_log.info,
      "Partition offset range: {}-{}",
      offsets.start_offset,
      offsets.committed_offset);

    for (auto tc : cases) {
        vlog(test_log.info, "Test case, offsets: {}-{}", tc.base, tc.last);

        auto range = inclusive_offset_range(tc.base, tc.last);
        std::optional<iobuf> actual;
        read_offset_range(range, actual, true);
        auto expected = load_log_segment_concat(range);

        ASSERT_TRUE(actual.has_value());
        ASSERT_TRUE(actual.value() == expected);
        ASSERT_TRUE(actual.value().size_bytes() == tc.expected_size);
    }
}

TEST_F(
  AsyncDataUploaderFixture,
  test_async_segment_upload_random_size_limited_not_compacted) {
    vlog(
      test_log.info,
      "Seed used for the test: {}",
      random_generators::global().initial_seed());
    create_topic();
    for (int i = 0; i < 10; i++) {
        random_records_generator generator;
        produce_data(100, generator);
        ss::abort_source as;
        get_test_partition()
          ->archival_meta_stm()
          ->cleanup_archive(
            model::offset(0), 0, ss::lowres_clock::now() + 1s, as)
          .get();
        roll_segment();
    }
    auto log_map = get_log_map();
    auto partition = get_test_partition();

    std::vector<fuzz_test_case> cases;
    get_random_test_cases(cases, log_map, num_cases_large());
    get_random_single_batch_test_cases(cases, log_map, num_cases_large());
    get_random_test_cases_that_start_on_config_batches(
      cases, log_map, num_cases_large());

    const auto& offsets = partition->log()->offsets();
    vlog(
      test_log.info,
      "Partition offset range: {}-{}",
      offsets.start_offset,
      offsets.committed_offset);

    size_t progress = 0;
    for (auto tc : cases) {
        vlog(test_log.info, "Test case, offsets: {}-{}", tc.base, tc.last);

        auto range = size_limited_offset_range(
          tc.base, tc.expected_size, tc.expected_size);
        std::optional<size_limited_upl_result> actual;
        read_offset_range(range, std::ref(actual));

        if (!actual.has_value()) {
            vlog(
              test_log.info,
              "Test case ignored, offsets: {}-{}",
              tc.base,
              tc.last);
            continue;
        }

        auto i_range = actual->range;
        auto expected = load_log_segment_concat(i_range);

        ASSERT_TRUE(actual.has_value());
        ASSERT_EQ(actual.value().payload, expected);
        ASSERT_EQ(actual.value().payload.size_bytes(), expected.size_bytes());
        progress++;
    }

    ASSERT_TRUE(progress > 0);
}
