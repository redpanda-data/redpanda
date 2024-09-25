// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "gtest/gtest.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "storage/tests/manual_mixin.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"
#include "utils/directory_walker.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/sleep.hh>

#include <absl/container/btree_map.h>

#include <chrono>
#include <numeric>

using namespace std::chrono_literals;

namespace {
ss::logger cmp_testlog("cmp_testlog");
} // anonymous namespace

struct work_dir_summary {
    explicit work_dir_summary(ss::sstring path)
      : dir_path(std::move(path)) {}

    ss::sstring dir_path;
    std::vector<ss::sstring> staging_files;
    std::unordered_map<ss::sstring, size_t> index_files;
    std::unordered_map<ss::sstring, size_t> segment_files;
    std::unordered_map<ss::sstring, size_t> compacted_index_files;

    ss::future<> add(const ss::directory_entry& de) {
        auto filename = de.name;
        if (
          filename.ends_with(".staging")
          || filename.ends_with(".log.compaction.compaction_index")
          || filename.ends_with(".log.compaction.base_index")) {
            staging_files.emplace_back(filename);
            co_return;
        }
        auto sz = co_await ss::file_size(
          ssx::sformat("{}/{}", dir_path, filename));
        // Ignore empty segments (likely the active segment)..
        if (filename.ends_with(".log") && sz > 0) {
            segment_files.emplace(filename, sz);
            co_return;
        }
        if (filename.ends_with(".compaction_index")) {
            compacted_index_files.emplace(filename, sz);
            co_return;
        }
        if (filename.ends_with(".base_index")) {
            index_files.emplace(filename, sz);
            co_return;
        }
    }

    // Ensures that we have exactly the number of files we expect.
    // NOTE: expected to be run after compaction; if run before compaction, may
    // be flaky if segments/indexes aren't flushed.
    void check_clean(size_t expected_segs) {
        EXPECT_TRUE(staging_files.empty()) << staging_files;
        EXPECT_EQ(index_files.size(), expected_segs) << index_files;
        EXPECT_EQ(compacted_index_files.size(), expected_segs)
          << compacted_index_files;
        EXPECT_EQ(segment_files.size(), expected_segs) << segment_files;
    }
};

class CompactionFixtureTest
  : public storage_manual_mixin
  , public redpanda_thread_fixture
  , public seastar_test {
public:
    using map_t = absl::btree_map<ss::sstring, std::optional<ss::sstring>>;

    ss::future<> SetUpAsync() override {
        test_local_cfg.get("election_timeout_ms").set_value(100ms);
        cluster::topic_properties props;
        props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        co_await add_topic({model::kafka_namespace, topic_name}, 1, props);
        co_await wait_for_leader(ntp);

        partition = app.partition_manager.local().get(ntp).get();
        log = partition->log().get();
    }

    ss::future<work_dir_summary> dir_summary() {
        auto dir_path = log->config().work_directory();
        directory_walker walker;
        work_dir_summary summary(dir_path);
        co_await walker.walk(
          dir_path, [&summary](const ss::directory_entry& de) {
              return summary.add(de);
          });
        co_return summary;
    }

    ss::future<> generate_data(
      size_t num_segments,
      size_t cardinality,
      size_t batches_per_segment,
      size_t records_per_batch = 1,
      size_t starting_value = 0,
      bool produce_tombstones = false,
      map_t* latest_kv = nullptr) {
        tests::kafka_produce_transport producer(co_await make_kafka_client());
        co_await producer.start();

        // Generate some segments.
        size_t val_count = starting_value;
        for (size_t i = 0; i < num_segments; i++) {
            for (int r = 0; r < batches_per_segment; r++) {
                auto kvs = tests::kv_t::sequence(
                  val_count,
                  records_per_batch,
                  val_count,
                  cardinality,
                  produce_tombstones);
                if (latest_kv) {
                    for (const auto& kv : kvs) {
                        latest_kv->insert_or_assign(kv.key, kv.val);
                    }
                }
                co_await producer.produce_to_partition(
                  topic_name, model::partition_id(0), std::move(kvs));
                val_count += records_per_batch;
            }
            co_await log->flush();
            co_await log->force_roll(ss::default_priority_class());
        }
    }

    ss::future<> generate_tombstones(
      size_t num_segments,
      size_t cardinality,
      size_t batches_per_segment,
      size_t records_per_batch = 1,
      size_t starting_value = 0,
      map_t* latest_kv = nullptr) {
        return generate_data(
          num_segments,
          cardinality,
          batches_per_segment,
          records_per_batch,
          starting_value,
          true,
          latest_kv);
    }

    // Generates a random mixture of records (tombstones optionally included).
    // Returns a map of the most recently produce k-v pair.
    ss::future<> generate_random_assorted_data(
      size_t num_segments,
      size_t cardinality,
      size_t batches_per_segment,
      size_t records_per_batch = 1,
      bool include_tombstones = false,
      map_t* latest_kv = nullptr) {
        tests::kafka_produce_transport producer(co_await make_kafka_client());
        co_await producer.start();

        // Generate some segments.
        for (size_t s = 0; s < num_segments; ++s) {
            for (size_t b = 0; b < batches_per_segment; ++b) {
                std::vector<tests::kv_t> kvs;
                kvs.reserve(records_per_batch);
                for (size_t r = 0; r < records_per_batch; ++r) {
                    const auto random_int = random_generators::get_int(
                      cardinality);
                    auto key = ssx::sformat("key{}", random_int);
                    const bool is_tombstone
                      = include_tombstones
                          ? random_generators::random_choice({false, true})
                          : false;
                    if (is_tombstone) {
                        kvs.emplace_back(std::move(key));
                    } else {
                        kvs.emplace_back(
                          std::move(key), ssx::sformat("val{}", random_int));
                    }
                }
                if (latest_kv) {
                    for (const auto& kv : kvs) {
                        latest_kv->insert_or_assign(kv.key, kv.val);
                    }
                }
                co_await producer.produce_to_partition(
                  topic_name, model::partition_id(0), std::move(kvs));
            }
            co_await log->flush();
            co_await log->force_roll(ss::default_priority_class());
        }
    }

    ss::future<std::vector<tests::kv_t>>
    check_records(size_t cardinality, size_t max_duplicates) {
        tests::kafka_consume_transport consumer(co_await make_kafka_client());
        co_await consumer.start();
        auto consumed_kvs = co_await consumer.consume_from_partition(
          topic_name, model::partition_id(0), model::offset(0));
        EXPECT_GE(consumed_kvs.size(), cardinality);
        auto num_duplicates = consumed_kvs.size() - cardinality;
        EXPECT_LE(num_duplicates, max_duplicates);
        co_return consumed_kvs;
    }

    ss::future<bool> do_sliding_window_compact(
      model::offset max_collect_offset,
      std::optional<std::chrono::milliseconds> tombstone_ret_ms,
      std::optional<size_t> max_keys = std::nullopt) {
        // Compact, allowing the map to grow as large as we need.
        ss::abort_source never_abort;
        storage::compaction_config cfg(
          max_collect_offset,
          tombstone_ret_ms,
          ss::default_priority_class(),
          never_abort,
          std::nullopt,
          max_keys,
          nullptr,
          nullptr);
        auto& disk_log = dynamic_cast<storage::disk_log_impl&>(*log);
        // sliding_window_compact takes cfg by const&, so return will be a
        // use-after-free
        co_return co_await disk_log.sliding_window_compact(cfg);
    }

protected:
    const model::topic topic_name{"compaction_e2e_test_topic"};
    const model::ntp ntp{model::kafka_namespace, topic_name, 0};
    cluster::partition* partition;
    storage::log* log;
    scoped_config test_local_cfg;
};

class CompactionFixtureParamTest
  : public CompactionFixtureTest
  , public ::testing::WithParamInterface<size_t> {};

// Test where the entire key space fits in the offset map, and compaction
// finishes in one pass.
TEST_P(CompactionFixtureParamTest, TestDedupeOnePass) {
    auto duplicates_per_key = GetParam();
    auto num_segments = 10;
    auto total_records = 100;
    auto cardinality = total_records / duplicates_per_key;
    size_t records_per_segment = total_records / num_segments;
    generate_data(num_segments, cardinality, records_per_segment).get();

    // Sanity check we created the right number of segments.
    // NOTE: ignore the active segment.
    auto segment_count_before = log->segment_count() - 1;
    ASSERT_EQ(segment_count_before, num_segments);

    // Compact, allowing the map to grow as large as we need.
    ss::abort_source never_abort;
    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(*log);
    storage::compaction_config cfg(
      disk_log.segments().back()->offsets().get_base_offset(),
      std::nullopt,
      ss::default_priority_class(),
      never_abort,
      std::nullopt,
      cardinality);
    disk_log.sliding_window_compact(cfg).get();

    // Another sanity check after compaction.
    auto segment_count_after = log->segment_count() - 1;
    ASSERT_EQ(num_segments, segment_count_after);
    auto summary_after = dir_summary().get();
    ASSERT_NO_FATAL_FAILURE(summary_after.check_clean(num_segments));

    // The number of duplicates can't exceed the number of segments - 1: the
    // latest closed segment should have no duplicates, and at worst, each
    // preceding segment will have 1 duplicate (the last record).
    auto consumed_kvs = check_records(cardinality, num_segments - 1).get();
    ASSERT_NO_FATAL_FAILURE();

    // Compacting again won't attempt again since the segments are marked as
    // compacted.
    auto segments_compacted = disk_log.get_probe().get_segments_compacted();
    disk_log.sliding_window_compact(cfg).get();
    auto segments_compacted_again
      = disk_log.get_probe().get_segments_compacted();
    ASSERT_EQ(segments_compacted, segments_compacted_again);

    // Consume again after restarting and ensure our assertions about
    // duplicates are still valid.
    restart(should_wipe::no);

    wait_for_leader(ntp).get();
    partition = app.partition_manager.local().get(ntp).get();
    log = partition->log().get();
    auto restart_summary = dir_summary().get();

    tests::kafka_consume_transport second_consumer(make_kafka_client().get());
    second_consumer.start().get();
    auto consumed_kvs_restarted = second_consumer
                                    .consume_from_partition(
                                      topic_name,
                                      model::partition_id(0),
                                      model::offset(0))
                                    .get();
    ASSERT_EQ(consumed_kvs, consumed_kvs_restarted);
}

INSTANTIATE_TEST_SUITE_P(
  DuplicatesPerKey, CompactionFixtureParamTest, ::testing::Values(1, 10, 100));

// Test where the key space doesn't fit in the offset map, forcing multiple
// compactions.
TEST_F(CompactionFixtureTest, TestDedupeMultiPass) {
    constexpr auto duplicates_per_key = 10;
    constexpr auto num_segments = 25;
    constexpr auto total_records = 100;
    constexpr auto cardinality = total_records / duplicates_per_key; // 10
    size_t records_per_segment = total_records / num_segments;       // 4
    generate_data(num_segments, cardinality, records_per_segment).get();

    // Compact, but with a map size that requires us to compact multiple times
    // to compact everything.
    ss::abort_source never_abort;
    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(*log);
    storage::compaction_config cfg(
      disk_log.segments().back()->offsets().get_base_offset(),
      std::nullopt,
      ss::default_priority_class(),
      never_abort,
      std::nullopt,
      cardinality - 1);
    disk_log.sliding_window_compact(cfg).get();
    auto segments_compacted = disk_log.get_probe().get_segments_compacted();

    // Another attempt to compact will actually rewrite segments.
    disk_log.sliding_window_compact(cfg).get();
    auto segments_compacted_2 = disk_log.get_probe().get_segments_compacted();
    ASSERT_LT(segments_compacted, segments_compacted_2);

    // But the above compaction should deduplicate any remaining keys.
    // Subsequent compactions will be no-ops.
    disk_log.sliding_window_compact(cfg).get();
    auto segments_compacted_3 = disk_log.get_probe().get_segments_compacted();
    ASSERT_EQ(segments_compacted_2, segments_compacted_3);

    ASSERT_NO_FATAL_FAILURE(check_records(cardinality, num_segments - 1).get());
}

TEST_F(CompactionFixtureTest, TestDedupeMultiPassAddedSegment) {
    constexpr auto duplicates_per_key = 10;
    constexpr auto num_segments = 25;
    constexpr auto total_records = 100;
    constexpr auto cardinality = total_records / duplicates_per_key; // 10
    size_t records_per_segment = total_records / num_segments;       // 4
    generate_data(num_segments, cardinality, records_per_segment).get();

    // Compact, but with a map size that requires us to compact multiple times
    // to compact everything.
    ss::abort_source never_abort;
    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(*log);
    storage::compaction_config cfg(
      disk_log.segments().back()->offsets().get_base_offset(),
      std::nullopt,
      ss::default_priority_class(),
      never_abort,
      std::nullopt,
      cardinality - 1);
    disk_log.sliding_window_compact(cfg).get();
    const auto& segs = disk_log.segments();

    auto segments_compacted = disk_log.get_probe().get_segments_compacted();

    // After first round of compaction, we should have a value for the window
    // start offset.
    ASSERT_TRUE(disk_log.get_last_compaction_window_start_offset().has_value());

    // Add an additional segment. This won't be considered for sliding window
    // compaction until the first window of segments is fully compacted.
    generate_data(1, cardinality, 1, 1, total_records).get();

    // Another attempt to compact will actually rewrite segments, but not the
    // last one.
    disk_log.sliding_window_compact(cfg).get();
    auto segments_compacted_2 = disk_log.get_probe().get_segments_compacted();
    ASSERT_LT(segments_compacted, segments_compacted_2);

    // segs.size() - 2 to account for active segment.
    for (int i = 0; i < segs.size() - 2; ++i) {
        auto& seg = segs[i];
        ASSERT_TRUE(seg->finished_windowed_compaction());
        ASSERT_TRUE(seg->finished_self_compaction());
        ASSERT_TRUE(seg->index().has_clean_compact_timestamp());
    }

    // The last added segment should not have had any compaction operations
    // performed.
    ASSERT_FALSE(segs[segs.size() - 2]->finished_windowed_compaction());
    ASSERT_FALSE(segs[segs.size() - 2]->finished_self_compaction());
    ASSERT_FALSE(segs[segs.size() - 2]->index().has_clean_compact_timestamp());

    // We should have compacted all the way down to the start of the log, and
    // reset the start offset.
    ASSERT_FALSE(
      disk_log.get_last_compaction_window_start_offset().has_value());

    // Another round of compaction to cleanly compact the newly added segment.
    disk_log.sliding_window_compact(cfg).get();

    // Now, these values should be set.
    ASSERT_TRUE(segs[segs.size() - 2]->finished_windowed_compaction());
    ASSERT_TRUE(segs[segs.size() - 2]->finished_self_compaction());
    ASSERT_TRUE(segs[segs.size() - 2]->index().has_clean_compact_timestamp());

    auto segments_compacted_3 = disk_log.get_probe().get_segments_compacted();
    ASSERT_LT(segments_compacted_2, segments_compacted_3);

    // We would have fully indexed the new segment, and since the rest of the
    // segments are already cleanly compacted, our start window should once
    // again have been reset.
    ASSERT_FALSE(
      disk_log.get_last_compaction_window_start_offset().has_value());

    // But the above compaction should deduplicate any remaining keys.
    // Subsequent compactions will be no-ops.
    disk_log.sliding_window_compact(cfg).get();
    auto segments_compacted_4 = disk_log.get_probe().get_segments_compacted();
    ASSERT_EQ(segments_compacted_3, segments_compacted_4);

    ASSERT_FALSE(
      disk_log.get_last_compaction_window_start_offset().has_value());

    ASSERT_NO_FATAL_FAILURE(check_records(cardinality, num_segments - 1).get());
}

class CompactionFixtureBatchSizeParamTest
  : public CompactionFixtureTest
  , public ::testing::WithParamInterface<size_t> {};

TEST_P(CompactionFixtureBatchSizeParamTest, TestRecompactWithNewData) {
    auto records_per_batch = GetParam();
    constexpr auto duplicates_per_key = 10;
    constexpr auto num_segments = 10;
    constexpr auto total_records = 100;
    constexpr auto cardinality = total_records / duplicates_per_key; // 10
    size_t records_per_segment = total_records / num_segments;       // 10
    size_t batches_per_segment = records_per_segment / records_per_batch;
    generate_data(
      num_segments, cardinality, batches_per_segment, records_per_batch)
      .get();

    // Compact everything in one go.
    ss::abort_source never_abort;
    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(*log);
    storage::compaction_config cfg(
      disk_log.segments().back()->offsets().get_base_offset(),
      std::nullopt,
      ss::default_priority_class(),
      never_abort,
      std::nullopt,
      cardinality);
    disk_log.sliding_window_compact(cfg).get();
    auto segments_compacted = disk_log.get_probe().get_segments_compacted();
    auto compaction_ratio = disk_log.compaction_ratio().get();

    // Subsequent compaction doesn't do anything.
    disk_log.sliding_window_compact(cfg).get();
    auto segments_compacted_2 = disk_log.get_probe().get_segments_compacted();
    auto compaction_ratio_2 = disk_log.compaction_ratio().get();
    ASSERT_EQ(segments_compacted, segments_compacted_2);
    ASSERT_EQ(compaction_ratio, compaction_ratio_2);

    // But once we add more data, we become eligible for compaction again.
    generate_data(1, cardinality, records_per_segment).get();
    storage::compaction_config new_cfg(
      disk_log.segments().back()->offsets().get_base_offset(),
      std::nullopt,
      ss::default_priority_class(),
      never_abort,
      std::nullopt,
      cardinality);
    disk_log.sliding_window_compact(new_cfg).get();

    // Most segments have already compacted their segments away entirely,
    // except their last record. Such segments shouldn't be compacted. Three
    // segments should be compacted:
    // - the new segment is compacted twice (self + windowed)
    // - the segment that previously had the latest keys should be compacted
    auto segments_compacted_3 = disk_log.get_probe().get_segments_compacted();
    auto compaction_ratio_3 = disk_log.compaction_ratio().get();
    ASSERT_EQ(segments_compacted + 3, segments_compacted_3);

    // Check for a reasonable compaction ratio.
    ASSERT_LT(compaction_ratio_3, 0.65);

    // Compared to our first compaction ratio that windowed compacted many
    // segments in a row, one self-compaction + windowed compaction will have a
    // worse compaction ratio.
    ASSERT_LT(compaction_ratio, compaction_ratio_3);
}
INSTANTIATE_TEST_SUITE_P(
  RecordsPerBatch,
  CompactionFixtureBatchSizeParamTest,
  ::testing::Values(1, 5, 10));

// Regression test for a bug when compacting when the last segment is all
// non-data batches. Previously such segments would appear uncompacted, and
// subsequent compactions would needlessly attempt to recompact.
TEST_F(CompactionFixtureTest, TestCompactWithNonDataBatches) {
    ss::abort_source never_abort;
    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(*log);
    auto raft = partition->raft();
    generate_data(10, 10, 10).get();
    auto orig_term = raft->term();

    // Create some segments with only non-data batches.
    while (raft->term()() < orig_term() + 5) {
        raft->step_down("test").get();
        RPTEST_REQUIRE_EVENTUALLY(5s, [&] { return raft->is_leader(); });
    }

    auto before_compaction_count
      = disk_log.get_probe().get_segments_compacted();
    storage::compaction_config new_cfg(
      disk_log.segments().back()->offsets().get_base_offset(),
      std::nullopt,
      ss::default_priority_class(),
      never_abort,
      std::nullopt);
    disk_log.sliding_window_compact(new_cfg).get();

    // The first time around, we should actually compact.
    auto after_compaction_count = disk_log.get_probe().get_segments_compacted();
    ASSERT_GT(after_compaction_count, before_compaction_count);
    for (const auto& seg : disk_log.segments()) {
        if (seg->has_appender()) {
            continue;
        }
        ASSERT_TRUE(seg->finished_windowed_compaction());
    }

    // But a subsequent attempt at compaction should do nothing.
    disk_log.sliding_window_compact(new_cfg).get();
    auto after_second_compaction_count
      = disk_log.get_probe().get_segments_compacted();
    ASSERT_EQ(after_second_compaction_count, after_compaction_count);
}

struct filled_read_result {
    size_t num_ghost_batches{0};
};

// Param: whether to consume to the end of the log, or have readers stop at a
// random offset.
class CompactionFilledReaderTest
  : public CompactionFixtureTest
  , public ::testing::WithParamInterface<bool> {};

// Test that validates gaps created by compactions can be filled in by a log
// reader to form a contiguous offset space.
TEST_P(CompactionFilledReaderTest, ReadFilledGaps) {
    auto consume_to_end = GetParam();
    ss::abort_source never_abort;
    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(*log);
    auto raft = partition->raft();
    int cardinality = 10;
    generate_data(
      /*num_segments=*/4, cardinality, /*batches_per_segment=*/10)
      .get();

    // Reads starting at `start_offset`, validating that each batch has an
    // offset one higher than the previous.
    auto log_end_offset = disk_log.offsets().committed_offset;
    ASSERT_GE(log_end_offset(), 40);
    auto validate_filled_read_from = [&](model::offset start_offset) {
        model::offset end_offset = consume_to_end
                                     ? model::offset::max()
                                     : model::offset{random_generators::get_int(
                                         start_offset(), log_end_offset())};

        storage::log_reader_config reader_cfg{
          start_offset,
          end_offset,
          ss::default_priority_class(),
          std::nullopt,
          std::nullopt};
        reader_cfg.fill_gaps = true;
        auto reader = disk_log.make_reader(reader_cfg).get();
        auto batches = model::consume_reader_to_memory(
                         std::move(reader), model::no_timeout)
                         .get();
        filled_read_result res;
        model::offset expected_next{start_offset};
        for (const auto& b : batches) {
            EXPECT_EQ(expected_next, b.base_offset());
            expected_next = model::next_offset(b.last_offset());
            if (b.header().type == model::record_batch_type::ghost_batch) {
                ++res.num_ghost_batches;
            }
        }
        return res;
    };
    // NOTE: randomized to encourage different  orderings of caching.
    for (auto i :
         random_generators::randomized_range(long(0), log_end_offset())) {
        const auto res = validate_filled_read_from(model::offset{i});
        ASSERT_EQ(res.num_ghost_batches, 0);
    }

    // Compaction should leave behind gaps, but those gaps should be filled
    // when reading.
    storage::compaction_config cfg(
      disk_log.segments().back()->offsets().get_base_offset(),
      std::nullopt,
      ss::default_priority_class(),
      never_abort,
      std::nullopt,
      10);
    disk_log.sliding_window_compact(cfg).get();
    for (auto i :
         random_generators::randomized_range(long(0), log_end_offset())) {
        const auto res = validate_filled_read_from(model::offset{i});
        // The last batches won't have anything removed, since they will be the
        // latest values for their respective keys.
        if (i >= log_end_offset - cardinality) {
            ASSERT_EQ(res.num_ghost_batches, 0);
        } else if (consume_to_end) {
            ASSERT_GT(res.num_ghost_batches, 0);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
  ValidatorConsumesToEnd, CompactionFilledReaderTest, ::testing::Bool());

TEST_F(CompactionFixtureTest, TestReadFilledGapsWithTerms) {
    ss::abort_source never_abort;
    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(*log);
    auto raft = partition->raft();
    auto orig_term = raft->term();
    int cardinality = 10;

    // Write some in different terms.
    while (raft->term()() < orig_term() + 5) {
        generate_data(
          /*num_segments=*/2, cardinality, /*batches_per_segment=*/10)
          .get();
        raft->step_down("test").get();
        RPTEST_REQUIRE_EVENTUALLY(5s, [&] { return raft->is_leader(); });
    }
    storage::log_reader_config reader_cfg{
      model::offset(0),
      model::offset::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt};
    reader_cfg.fill_gaps = true;

    // Collect the original term of each batch.
    auto orig_reader = disk_log.make_reader(reader_cfg).get();
    auto orig_batches = model::consume_reader_to_memory(
                          std::move(orig_reader), model::no_timeout)
                          .get();
    absl::btree_map<model::offset, model::term_id> terms_per_offset;
    for (const auto& b : orig_batches) {
        for (auto o = b.base_offset(); o <= b.last_offset(); o++) {
            terms_per_offset[o] = b.term();
        }
    }

    storage::compaction_config cfg(
      disk_log.segments().back()->offsets().get_base_offset(),
      std::nullopt,
      ss::default_priority_class(),
      never_abort,
      std::nullopt,
      10);
    disk_log.sliding_window_compact(cfg).get();

    // After compaction, the terms should not have changed, even for gaps that
    // were filled in.
    auto compacted_reader = disk_log.make_reader(reader_cfg).get();
    auto compacted_batches = model::consume_reader_to_memory(
                               std::move(compacted_reader), model::no_timeout)
                               .get();
    model::offset expected_next{0};
    size_t num_ghost_batches{0};
    for (const auto& b : compacted_batches) {
        for (auto o = b.base_offset(); o <= b.last_offset(); o++) {
            ASSERT_EQ(terms_per_offset[o], b.term());
        }
        EXPECT_EQ(expected_next, b.base_offset());
        expected_next = model::next_offset(b.last_offset());
        if (b.header().type == model::record_batch_type::ghost_batch) {
            ++num_ghost_batches;
        }
    }
    ASSERT_GT(num_ghost_batches, 0);
}

TEST_F(CompactionFixtureTest, TestTombstones) {
    auto duplicates_per_key = 2;
    auto total_records = 10;
    auto cardinality = total_records / duplicates_per_key;
    auto num_segments = 5;
    size_t record_batches_per_segment = total_records / num_segments;

    generate_data(num_segments, cardinality, record_batches_per_segment).get();
    // Generate a tombstone record for "key0".
    generate_tombstones(1, 1, 1).get();

    auto tombstone_retention_ms = 1000ms;

    auto num_tombstone_segments = 1;
    auto total_segments = num_segments + num_tombstone_segments;

    auto log_segment_count_before = log->segment_count();
    // Sanity check we created the right number of segments.
    // NOTE: ignore the active segment.
    auto segment_count_before = log_segment_count_before - 1;
    ASSERT_EQ(segment_count_before, total_segments);

    // Perform first round of sliding window compaction.
    bool did_compact = do_sliding_window_compact(
                         log->segments().back()->offsets().get_base_offset(),
                         tombstone_retention_ms)
                         .get();

    ASSERT_TRUE(did_compact);

    // Another sanity check after compaction.
    auto segment_count_after = log->segment_count() - 1;
    ASSERT_EQ(total_segments, segment_count_after);

    auto summary_after = dir_summary().get();
    ASSERT_NO_FATAL_FAILURE(summary_after.check_clean(total_segments));

    // The number of duplicates can't exceed the number of segments - 1: the
    // latest closed segment should have no duplicates, and at worst, each
    // preceding segment will have 1 duplicate (the last record).
    {
        auto consumed_kvs
          = check_records(cardinality, total_segments - 1).get();
        ASSERT_NO_FATAL_FAILURE();
    }

    // Every segment sans the active segment should have a
    // clean_compact_timestamp set, since we fully indexed all of them.
    int num_clean_before = 0;
    for (const auto& seg : log->segments()) {
        if (seg->index().has_clean_compact_timestamp()) {
            ++num_clean_before;
        }
    }
    ASSERT_EQ(num_clean_before, total_segments);

    // Requesting a second round of compaction won't occur since the segments
    // are marked as compacted.
    auto segments_compacted = log->get_probe().get_segments_compacted();
    did_compact = do_sliding_window_compact(
                    log->segments().back()->offsets().get_base_offset(),
                    tombstone_retention_ms)
                    .get();

    ASSERT_FALSE(did_compact);
    auto segments_compacted_again = log->get_probe().get_segments_compacted();
    ASSERT_EQ(segments_compacted, segments_compacted_again);

    // Check that the clean_compact_timestamps got persisted in the index_state.
    int num_clean_again = 0;
    for (const auto& seg : log->segments()) {
        if (seg->index().has_clean_compact_timestamp()) {
            ++num_clean_again;
        }
    }
    ASSERT_EQ(num_clean_again, num_clean_before);

    // Consume again after restarting and ensure our assertions about
    // duplicates are still valid.
    restart(should_wipe::no);

    wait_for_leader(ntp).get();
    partition = app.partition_manager.local().get(ntp).get();
    log = partition->log().get();

    ASSERT_EQ(log->segment_count(), log_segment_count_before);

    // Check that the clean_compact_timestamps got persisted in the index_state,
    // even after a restart.
    int num_clean_after = 0;
    for (const auto& seg : log->segments()) {
        if (seg->index().has_clean_compact_timestamp()) {
            ++num_clean_after;
        }
    }
    ASSERT_EQ(num_clean_after, num_clean_before);

    // Sleep for tombstone.retention.ms time, so that the next time we attempt
    // to compact the tombstone record will be eligible for deletion.
    ss::sleep(tombstone_retention_ms).get();

    did_compact = do_sliding_window_compact(
                    log->segments().back()->offsets().get_base_offset(),
                    tombstone_retention_ms)
                    .get();

    ASSERT_TRUE(did_compact);

    {
        tests::kafka_consume_transport consumer(make_kafka_client().get());
        consumer.start().get();
        auto consumed_kvs = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(0))
                              .get();

        // We should have cardinality-1 k-v pairs due to produced tombstone.
        ASSERT_EQ(consumed_kvs.size(), cardinality - 1);

        // Assert there is no tombstone record left.
        auto find_tombstone_record = [](const auto& kv) {
            return kv.is_tombstone();
        };

        auto tombstone_it = std::find_if(
          consumed_kvs.begin(), consumed_kvs.end(), find_tombstone_record);

        // The tombstone should have been removed after second round of
        // compaction post tombstone.retention.ms.
        ASSERT_EQ(tombstone_it, consumed_kvs.end());

        // Assert the "key0" value which was used for a tombstone record is not
        // present. Redundant, sanity check.
        auto find_key0_value = [](const auto& kv) { return kv.key == "key0"; };

        auto key0_it = std::find_if(
          consumed_kvs.begin(), consumed_kvs.end(), find_key0_value);

        ASSERT_EQ(key0_it, consumed_kvs.end());
    }
}

class CompactionFixtureTombstonesParamTest
  : public CompactionFixtureTest
  , public ::testing::WithParamInterface<size_t> {};

TEST_P(CompactionFixtureTombstonesParamTest, TestTombstonesCompletelyEmptyLog) {
    const auto num_segments = GetParam();

    // Generate 1 tombstone records for each segment.
    const auto num_tombstones = num_segments;
    const auto batches_per_segment = 1;
    const auto tombstones_per_batch = num_tombstones / batches_per_segment;

    generate_tombstones(
      num_segments, num_tombstones, batches_per_segment, tombstones_per_batch)
      .get();

    // Perform first round of sliding window compaction.
    bool did_compact = do_sliding_window_compact(
                         log->segments().back()->offsets().get_base_offset(),
                         std::nullopt)
                         .get();

    ASSERT_TRUE(did_compact);
    for (int i = 0; i < num_segments; ++i) {
        ASSERT_TRUE(log->segments()[i]->index().has_clean_compact_timestamp());
    }

    {
        tests::kafka_consume_transport consumer(make_kafka_client().get());
        consumer.start().get();
        auto consumed_kvs = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(0))
                              .get();

        ASSERT_NO_FATAL_FAILURE();

        // Sanity check the number of records consumed.
        ASSERT_EQ(consumed_kvs.size(), num_tombstones);

        // Every record should be a tombstone without a value.
        for (const auto& kv : consumed_kvs) {
            ASSERT_FALSE(kv.val.has_value());
            ASSERT_TRUE(kv.is_tombstone());
        }
    }

    // Sleep for a very short amount of time to ensure that tombstone records
    // will be eligible for deletion during the next round of compaction
    ss::sleep(100ms).get();

    // Use 1ms to ensure segment is considered for self-compaction, due to
    // having removable tombstones.
    did_compact = do_sliding_window_compact(
                    log->segments().back()->offsets().get_base_offset(), 1ms)
                    .get();

    ASSERT_TRUE(did_compact);
    {
        tests::kafka_consume_transport consumer(make_kafka_client().get());
        consumer.start().get();
        auto consumed_kvs = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(0))
                              .get();
        // The tombstones should have been removed after second round of
        // compaction post tombstone.retention.ms.
        ASSERT_TRUE(consumed_kvs.empty());
    }
}

INSTANTIATE_TEST_SUITE_P(
  NumSegments,
  CompactionFixtureTombstonesParamTest,
  ::testing::Values(1, 10, 100));

struct TombstonesRandomArgs {
    static TombstonesRandomArgs create() {
        static constexpr size_t max_segments = 100;
        static constexpr size_t max_records = 1000;
        static constexpr size_t max_cardinality = max_records;
        static constexpr size_t max_batches_per_segment = 5;
        return TombstonesRandomArgs{
          .num_segments = random_generators::get_int(size_t{1}, max_segments),
          .total_records = random_generators::get_int(size_t{1}, max_records),
          .cardinality = random_generators::get_int(size_t{1}, max_cardinality),
          .batches_per_segment = random_generators::get_int(
            size_t{1}, max_batches_per_segment)};
    }

    size_t num_segments;
    size_t total_records;
    size_t cardinality;
    size_t batches_per_segment;
};

class CompactionFixtureTombstonesRandomParamTest
  : public CompactionFixtureTest
  , public ::testing::WithParamInterface<bool> {};

TEST_P(
  CompactionFixtureTombstonesRandomParamTest,
  TestTombstonesRandomDistribution) {
    const auto data_args = TombstonesRandomArgs::create();
    const auto wait_for_retention_ms = GetParam();
    const auto& [num_segments, total_records, cardinality, batches_per_segment]
      = data_args;

    const auto num_batches = num_segments * batches_per_segment;
    // May not divide evenly.
    const auto records_per_batch = std::max(
      size_t{1}, total_records / num_batches);

    map_t latest_kv_map;
    generate_random_assorted_data(
      num_segments,
      cardinality,
      batches_per_segment,
      records_per_batch,
      true,
      &latest_kv_map)
      .get();

    auto num_tombstones_produced = std::accumulate(
      latest_kv_map.begin(),
      latest_kv_map.end(),
      0,
      [](size_t acc, const auto& p) {
          return acc + size_t{!p.second.has_value()};
      });

    auto num_records_produced = latest_kv_map.size() - num_tombstones_produced;

    // Perform first round of sliding window compaction.
    // Don't allow for tombstone clean-up to occur.
    bool did_compact = do_sliding_window_compact(
                         log->segments().back()->offsets().get_base_offset(),
                         std::nullopt)
                         .get();

    ASSERT_TRUE(did_compact);

    {
        tests::kafka_consume_transport consumer(make_kafka_client().get());
        consumer.start().get();
        auto consumed_kvs = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(0))
                              .get();
        ASSERT_NO_FATAL_FAILURE();

        ASSERT_EQ(consumed_kvs.size(), latest_kv_map.size());

        // Assert the key consumed is in the latest_kv_map.
        for (const auto& kv : consumed_kvs) {
            ASSERT_TRUE(latest_kv_map.contains(kv.key));
            ASSERT_EQ(kv.val, latest_kv_map[kv.key]);
            if (!latest_kv_map[kv.key].has_value()) {
                ASSERT_TRUE(kv.is_tombstone());
            }
        }
    }

    std::optional<std::chrono::milliseconds> tombstone_retention_ms
      = wait_for_retention_ms ? 1000ms
                              : std::optional<std::chrono::milliseconds>{};

    // Maybe sleep for tombstone.retention.ms time, so that the next time we
    // attempt to compact the tombstone records will be eligible for deletion.
    if (wait_for_retention_ms) {
        ss::sleep(tombstone_retention_ms.value()).get();
    }

    did_compact = do_sliding_window_compact(
                    log->segments().back()->offsets().get_base_offset(),
                    tombstone_retention_ms)
                    .get();

    // Compaction will only have occurred if tombstones were eligible for
    // deletion.
    ASSERT_EQ(did_compact, wait_for_retention_ms);

    {
        tests::kafka_consume_transport consumer(make_kafka_client().get());
        consumer.start().get();
        auto consumed_kvs = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(0))
                              .get();

        // Assert the key consumed is in the latest_kv_map.
        // latest_kv_map.size() != consumed_kvs.size() if we waited for
        // retention_ms, due to tombstone removal.
        for (const auto& kv : consumed_kvs) {
            ASSERT_TRUE(latest_kv_map.contains(kv.key));
            ASSERT_EQ(kv.val, latest_kv_map[kv.key]);
            if (!latest_kv_map[kv.key].has_value()) {
                ASSERT_TRUE(kv.is_tombstone());
            }
        }

        if (wait_for_retention_ms) {
            // Assert there is no tombstone record left.
            auto find_tombstone_record = [](const auto& kv) {
                return kv.is_tombstone();
            };

            auto tombstone_it = std::find_if(
              consumed_kvs.begin(), consumed_kvs.end(), find_tombstone_record);

            // The tombstones should have been removed after second round of
            // compaction post tombstone.retention.ms.
            ASSERT_EQ(tombstone_it, consumed_kvs.end());
            ASSERT_EQ(consumed_kvs.size(), num_records_produced);
        } else {
            ASSERT_EQ(consumed_kvs.size(), latest_kv_map.size());
            ASSERT_EQ(
              consumed_kvs.size(),
              num_tombstones_produced + num_records_produced);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
  RandomDistribution,
  CompactionFixtureTombstonesRandomParamTest,
  ::testing::Bool());

class CompactionFixtureTombstonesMultiPassRandomParamTest
  : public CompactionFixtureTest
  , public ::testing::WithParamInterface<std::tuple<bool, size_t>> {};

TEST_P(
  CompactionFixtureTombstonesMultiPassRandomParamTest,
  TestTombstonesMultiPassRandomDistribution) {
    const auto data_args = TombstonesRandomArgs::create();
    auto [wait_for_retention_ms, max_keys] = GetParam();
    const auto& [num_segments, total_records, cardinality, batches_per_segment]
      = data_args;

    const auto num_batches = num_segments * batches_per_segment;
    // May not divide evenly.
    const auto records_per_batch = std::max(
      size_t{1}, total_records / num_batches);

    // Need to ensure we can fully index a segment with max_keys at a minimum.
    auto records_per_segment = records_per_batch * batches_per_segment;
    max_keys = std::max(max_keys, records_per_segment);

    map_t latest_kv_map;
    generate_random_assorted_data(
      num_segments,
      cardinality,
      batches_per_segment,
      records_per_batch,
      true,
      &latest_kv_map)
      .get();

    auto num_tombstones_produced = std::accumulate(
      latest_kv_map.begin(),
      latest_kv_map.end(),
      0,
      [](size_t acc, const auto& p) {
          return acc + size_t{!p.second.has_value()};
      });

    auto num_records_produced = latest_kv_map.size() - num_tombstones_produced;

    int prev_num_clean_compacted = 0;
    bool did_compact = true;
    // Perform as many rounds of sliding window compaction as required.
    // Don't allow for tombstone clean-up to occur.
    while (did_compact) {
        did_compact = do_sliding_window_compact(
                        log->segments().back()->offsets().get_base_offset(),
                        std::nullopt,
                        max_keys)
                        .get();

        auto num_clean_compacted = 0;
        for (const auto& seg : log->segments()) {
            if (seg->index().has_clean_compact_timestamp()) {
                ++num_clean_compacted;
            }
        }
        ASSERT_GE(num_clean_compacted, prev_num_clean_compacted);
        prev_num_clean_compacted = num_clean_compacted;
    }

    // All segments should be clean, minus the active segment.
    ASSERT_EQ(prev_num_clean_compacted, log->segment_count() - 1);

    {
        tests::kafka_consume_transport consumer(make_kafka_client().get());
        consumer.start().get();
        auto consumed_kvs = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(0))
                              .get();
        ASSERT_NO_FATAL_FAILURE();

        // Assert the key consumed is in the latest_kv_map.
        for (const auto& kv : consumed_kvs) {
            ASSERT_TRUE(latest_kv_map.contains(kv.key));
            ASSERT_EQ(kv.val, latest_kv_map[kv.key]);
            if (!latest_kv_map[kv.key].has_value()) {
                ASSERT_TRUE(kv.is_tombstone());
            }
        }
        ASSERT_EQ(consumed_kvs.size(), latest_kv_map.size());
    }

    std::optional<std::chrono::milliseconds> tombstone_retention_ms
      = wait_for_retention_ms ? 1000ms
                              : std::optional<std::chrono::milliseconds>{};

    // Maybe sleep for tombstone.retention.ms time, so that the next time we
    // attempt to compact the tombstone records will be eligible for deletion.
    if (wait_for_retention_ms) {
        ss::sleep(tombstone_retention_ms.value()).get();
    }

    did_compact = do_sliding_window_compact(
                    log->segments().back()->offsets().get_base_offset(),
                    tombstone_retention_ms)
                    .get();

    // Compaction will only have occurred if tombstones were eligible for
    // deletion.
    ASSERT_EQ(did_compact, wait_for_retention_ms);

    {
        tests::kafka_consume_transport consumer(make_kafka_client().get());
        consumer.start().get();
        auto consumed_kvs = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(0))
                              .get();

        // Assert the key consumed is in the latest_kv_map.
        // latest_kv_map.size() != consumed_kvs.size() if we waited for
        // retention_ms, due to tombstone removal.
        for (const auto& kv : consumed_kvs) {
            ASSERT_TRUE(latest_kv_map.contains(kv.key));
            ASSERT_EQ(kv.val, latest_kv_map[kv.key]);
            if (!latest_kv_map[kv.key].has_value()) {
                ASSERT_TRUE(kv.is_tombstone());
            }
        }

        if (wait_for_retention_ms) {
            // Assert there is no tombstone record left.
            auto find_tombstone_record = [](const auto& kv) {
                return kv.is_tombstone();
            };

            auto tombstone_it = std::find_if(
              consumed_kvs.begin(), consumed_kvs.end(), find_tombstone_record);

            // The tombstones should have been removed after second round of
            // compaction post tombstone.retention.ms.
            ASSERT_EQ(tombstone_it, consumed_kvs.end());
            ASSERT_EQ(consumed_kvs.size(), num_records_produced);
        } else {
            ASSERT_EQ(consumed_kvs.size(), latest_kv_map.size());
            ASSERT_EQ(
              consumed_kvs.size(),
              num_tombstones_produced + num_records_produced);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
  RandomDistributionMultiPass,
  CompactionFixtureTombstonesMultiPassRandomParamTest,
  ::testing::Combine(::testing::Bool(), ::testing::Values(10, 25, 100)));
