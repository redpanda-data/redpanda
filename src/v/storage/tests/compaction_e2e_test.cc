// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
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

#include <absl/container/btree_map.h>

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
      size_t records_per_batch = 1) {
        tests::kafka_produce_transport producer(co_await make_kafka_client());
        co_await producer.start();

        // Generate some segments.
        size_t val_count = 0;
        absl::btree_map<ss::sstring, ss::sstring> latest_kv;
        for (size_t i = 0; i < num_segments; i++) {
            for (int r = 0; r < batches_per_segment; r++) {
                auto kvs = tests::kv_t::sequence(
                  val_count, records_per_batch, val_count, cardinality);
                for (const auto& [k, v] : kvs) {
                    latest_kv[k] = v;
                }
                co_await producer.produce_to_partition(
                  topic_name, model::partition_id(0), std::move(kvs));
                val_count += records_per_batch;
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
