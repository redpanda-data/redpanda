/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "cloud_topics/level_one/maintenance/compaction/compaction_sink.h"
#include "cloud_topics/level_one/maintenance/compaction/compaction_source.h"
#include "cloud_topics/level_one/maintenance/compaction/tests/in_memory_sink.h"
#include "cloud_topics/level_one/maintenance/compaction/tests/throwing_compaction_sink.h"
#include "cloud_topics/level_one/maintenance/logger.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/worker_probe.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"
#include "compaction/tests/simple_reducer.h"
#include "config/property.h"
#include "container/chunked_circular_buffer.h"
#include "container/chunked_vector.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/batch_compression.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "storage/tests/batch_generators.h"
#include "utils/prefix_logger.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <limits>
#include <numeric>
#include <variant>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace {

static prefix_logger logger(l1::compaction_log, "reducer_test");

using latest_kv_map_t
  = absl::btree_map<ss::sstring, std::optional<ss::sstring>>;
chunked_circular_buffer<model::record_batch> generate_batches(
  size_t num_batches,
  size_t cardinality,
  size_t records_per_batch = 1,
  size_t starting_value = 0,
  bool produce_tombstones = false,
  latest_kv_map_t* latest_kv = nullptr,
  model::compression compression_type = model::compression::none,
  std::optional<model::timestamp> ts_override = std::nullopt,
  size_t base = 0) {
    chunked_circular_buffer<model::record_batch> batches;
    size_t val_count = starting_value;
    auto ts = ts_override.value_or(model::timestamp::now());
    for (size_t i = 0; i < num_batches; i++) {
        auto kvs = tests::kv_t::sequence(
          val_count,
          records_per_batch,
          val_count,
          cardinality,
          produce_tombstones,
          base);
        if (latest_kv) {
            for (const auto& kv : kvs) {
                latest_kv->insert_or_assign(kv.key, kv.val);
            }
        }
        auto batch_base_offset = model::offset(records_per_batch * i);
        auto batch = tests::batch_from_kvs(
          kvs, batch_base_offset, ts, compression_type);
        batches.push_back(std::move(batch));
        val_count += records_per_batch;
    }
    return batches;
}

ss::future<> do_compact(
  model::topic_id_partition tidp,
  model::ntp ntp,
  l1::metastore::compaction_offsets_response offsets_response,
  l1::metastore::compaction_epoch expected_compaction_epoch,
  kafka::offset start_offset,
  l1::metastore* metastore,
  l1::io* io,
  std::chrono::milliseconds min_compaction_lag_ms = 0ms,
  kafka::offset max_compactible_offset = kafka::offset::max(),
  size_t max_object_size = 128_MiB,
  size_t commit_interval_bytes = 512_MiB) {
    ss::abort_source as;
    auto state = l1::compaction_job_state::running;
    auto map = compaction::simple_key_offset_map();
    auto dirty_range_intervals = offsets_response.dirty_ranges.to_vec();
    l1::compaction_worker_probe probe;
    auto src = std::make_unique<l1::compaction_source>(
      ntp,
      tidp,
      dirty_range_intervals,
      offsets_response.removable_tombstone_ranges,
      start_offset,
      max_compactible_offset,
      &map,
      min_compaction_lag_ms,
      metastore,
      io,
      as,
      state,
      probe,
      nullptr,
      logger);
    auto sink = std::make_unique<l1::compaction_sink>(
      tidp,
      dirty_range_intervals,
      offsets_response.removable_tombstone_ranges,
      expected_compaction_epoch,
      start_offset,
      io,
      metastore,
      as,
      config::mock_binding<size_t>(max_object_size),
      config::mock_binding<size_t>(commit_interval_bytes),
      16_MiB,
      probe,
      logger);
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    co_await std::move(reducer).run();
}

ss::future<> do_compact_with_throwing_sink(
  model::topic_id_partition tidp,
  model::ntp ntp,
  l1::metastore::compaction_offsets_response offsets_response,
  l1::metastore::compaction_epoch expected_compaction_epoch,
  kafka::offset start_offset,
  l1::metastore* metastore,
  l1::io* io,
  l1::throwing_compaction_sink::predicate_t should_roll,
  l1::throwing_compaction_sink::predicate_t should_throw,
  std::chrono::milliseconds min_compaction_lag_ms = 0ms,
  kafka::offset max_compactible_offset = kafka::offset::max(),
  size_t max_object_size = 128_MiB,
  size_t commit_interval_bytes = 512_MiB) {
    ss::abort_source as;
    auto state = l1::compaction_job_state::running;
    auto map = compaction::simple_key_offset_map();
    auto dirty_range_intervals = offsets_response.dirty_ranges.to_vec();
    l1::compaction_worker_probe probe;
    auto src = std::make_unique<l1::compaction_source>(
      ntp,
      tidp,
      dirty_range_intervals,
      offsets_response.removable_tombstone_ranges,
      start_offset,
      max_compactible_offset,
      &map,
      min_compaction_lag_ms,
      metastore,
      io,
      as,
      state,
      probe,
      nullptr,
      logger);
    // By default max_object_size is very large to disable size-based rolls,
    // leaving rolling to the throwing_compaction_sink's should_roll
    // predicate.
    auto inner_sink = std::make_unique<l1::compaction_sink>(
      tidp,
      dirty_range_intervals,
      offsets_response.removable_tombstone_ranges,
      expected_compaction_epoch,
      start_offset,
      io,
      metastore,
      as,
      config::mock_binding<size_t>(max_object_size),
      config::mock_binding<size_t>(commit_interval_bytes),
      16_MiB,
      probe,
      logger);
    auto sink = std::make_unique<l1::throwing_compaction_sink>(
      std::move(inner_sink), std::move(should_roll), std::move(should_throw));
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    co_await std::move(reducer).run();
}

} // namespace

TEST(ReducerTest, InMemoryReducer) {
    const auto test_ntp = model::ntp(
      model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));
    const auto test_tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), test_ntp.tp.partition);

    int num_batches = 10;
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false, .count = 1};
    auto input_batches = gen(spec, num_batches);

    auto src = std::make_unique<compaction::simple_source>(
      std::move(input_batches), test_ntp);
    chunked_vector<l1::in_memory_sink::object_output_t> output_objs;
    auto sink = std::make_unique<l1::in_memory_sink>(test_tidp, &output_objs);
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    std::move(reducer).run().get();

    ASSERT_EQ(output_objs.size(), 1);
    auto& [info, object] = output_objs.front();
    auto rdr = l1::object_reader::create(
      make_iobuf_input_stream(std::move(object)));
    auto close_rdr = ss::defer([&rdr] { rdr->close().get(); });

    chunked_circular_buffer<model::record_batch> output_batches;
    while (true) {
        l1::object_reader::result res = rdr->read_next().get();
        if (std::holds_alternative<model::record_batch>(res)) {
            output_batches.push_back(
              std::move(std::get<model::record_batch>(res)));
        }
        if (std::holds_alternative<l1::object_reader::eof>(res)) {
            break;
        }
    }

    ASSERT_EQ(output_batches.size(), num_batches);
    linear_int_kv_batch_generator::validate_post_compaction(
      std::move(output_batches));
}

ss::sstring iobuf_to_string(iobuf buf) {
    iobuf_parser parser{std::move(buf)};
    return parser.read_string_unsafe(parser.bytes_left());
}

class ReducerTestFixture : public l1::l1_reader_fixture {
public:
    void verify_compacted_log(
      const model::ntp& ntp,
      const model::topic_id_partition& tidp,
      const latest_kv_map_t& latest_kv_map,
      size_t expected_num_records,
      size_t expected_num_batches) {
        auto reader = make_reader(ntp, tidp);
        auto output_batches = read_all(std::move(reader));

        ASSERT_EQ(output_batches.size(), expected_num_batches);
        int output_num_records = std::accumulate(
          output_batches.begin(),
          output_batches.end(),
          int{0},
          [](int acc, model::record_batch& b) {
              return acc + b.record_count();
          });
        ASSERT_EQ(output_num_records, expected_num_records);

        for (auto& batch : output_batches) {
            if (batch.compressed()) {
                batch = model::decompress_batch_sync(batch);
            }
            batch.for_each_record([&latest_kv_map](model::record rec) {
                auto key = iobuf_to_string(rec.release_key());
                std::optional<ss::sstring> val;
                if (rec.has_value()) {
                    val = iobuf_to_string(rec.release_value());
                }
                EXPECT_TRUE(latest_kv_map.contains(key));
                EXPECT_EQ(val, latest_kv_map.at(key));
            });
        }
    }
};

TEST_F(ReducerTestFixture, LinearKeyValueReducer) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    int num_batches = 10;
    int num_records = 10;
    kafka::offset start_offset{0};
    kafka::offset last_offset{num_batches * num_records - 1};
    auto gen = linear_int_kv_batch_generator();
    auto ts = model::timestamp::now();
    auto spec = model::test::record_batch_spec{
      .allow_compression = true,
      .count = num_records,
      .timestamp = ts,
      .all_records_have_same_timestamp = true};
    auto batches = gen(spec, num_batches);
    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    ss::abort_source as;

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();

    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 1.0);
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
      start_offset, last_offset));

    auto dirty_range_intervals
      = compaction_info->offsets_response.dirty_ranges.to_vec();

    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      compaction_info->compaction_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io)
      .get();

    auto reader = make_reader(ntp, tidp);
    auto output_batches = read_all(std::move(reader));

    ASSERT_EQ(output_batches.size(), num_batches);
    int output_num_records = std::accumulate(
      output_batches.begin(),
      output_batches.end(),
      int{0},
      [](int acc, model::record_batch& b) { return acc + b.record_count(); });
    ASSERT_EQ(output_num_records, num_batches);
    linear_int_kv_batch_generator::validate_post_compaction(
      std::move(output_batches));

    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.0);
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());
}

TEST_F(ReducerTestFixture, LinearKeyValueReducerSetStartOffset) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    int num_batches = 10;
    int num_records = 10;
    kafka::offset last_offset{num_batches * num_records - 1};
    auto gen = linear_int_kv_batch_generator();
    auto ts = model::timestamp::now();
    auto spec = model::test::record_batch_spec{
      .allow_compression = true,
      .count = num_records,
      .timestamp = ts,
      .all_records_have_same_timestamp = true};
    auto batches = gen(spec, num_batches);
    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    ss::abort_source as;

    auto new_start_offset = kafka::offset{5};

    auto set_start_offset_res
      = _metastore.set_start_offset(tidp, new_start_offset).get();
    ASSERT_TRUE(set_start_offset_res.has_value());

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();

    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 1.0);
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
      new_start_offset, last_offset));

    auto dirty_range_intervals
      = compaction_info->offsets_response.dirty_ranges.to_vec();

    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      compaction_info->compaction_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io)
      .get();

    auto reader = make_reader(ntp, tidp);
    auto output_batches = read_all(std::move(reader));

    ASSERT_EQ(output_batches.size(), num_batches);
    int output_num_records = std::accumulate(
      output_batches.begin(),
      output_batches.end(),
      int{0},
      [](int acc, model::record_batch& b) { return acc + b.record_count(); });
    ASSERT_EQ(output_num_records, num_batches);
    linear_int_kv_batch_generator::validate_post_compaction(
      std::move(output_batches));

    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.0);
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());
}

// Retention can move the start offset under a running compaction job, and
// the job still commits: it removes records, but its cleaned ranges are all
// rejected against the start offset it snapshotted before the truncation,
// so nothing is marked clean. Verify the emptied range is left dirty with
// the epoch advanced, and that a fresh pass then converges the log.
TEST_F(ReducerTestFixture, RetentionRaceEmptiesRangeWithoutCleaning) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Three extents, two keys, newest copies at the head:
    //   [0,1]: a b
    //   [2,3]: a b
    //   [4,5]: a b
    auto add_extent = [&](kafka::offset base, const ss::sstring& sfx) {
        std::vector<tests::kv_t> kvs;
        kvs.emplace_back("a", "a" + sfx);
        kvs.emplace_back("b", "b" + sfx);
        chunked_circular_buffer<model::record_batch> batches;
        batches.push_back(
          tests::batch_from_kvs(
            kvs,
            model::offset{base()},
            model::timestamp::now(),
            model::compression::none));
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        return make_l1_objects(std::move(tidp_batches));
    };
    add_extent(kafka::offset{0}, "0").get();
    add_extent(kafka::offset{2}, "2").get();
    add_extent(kafka::offset{4}, "4").get();

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};

    // The job snapshots its inputs: start offset 0, the whole log dirty.
    auto before_truncate = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(before_truncate.has_value());
    ASSERT_EQ(before_truncate->start_offset, kafka::offset{0});
    ASSERT_TRUE(before_truncate->offsets_response.dirty_ranges.covers(
      kafka::offset{0}, kafka::offset{5}));
    const auto initial_epoch = before_truncate->compaction_epoch;

    // Retention fires mid-job: truncate to 2, deleting extent [0,1].
    auto sso_res = _metastore.set_start_offset(tidp, kafka::offset{2}).get();
    ASSERT_TRUE(sso_res.has_value());

    // The job finishes with its pre-truncation snapshot. Dedup rewrites
    // [2,3] empty -- both records are superseded by the copies at [4,5] --
    // and the max compactible offset stops it before it reaches [4,5]
    // itself.
    do_compact(
      tidp,
      ntp,
      std::move(before_truncate->offsets_response),
      before_truncate->compaction_epoch,
      /*pre-truncation start_offset=*/kafka::offset{0},
      &_metastore,
      &_io,
      0ms,
      /*max_compactible_offset=*/kafka::offset{3})
      .get();

    // The job's commit went through: the epoch advanced and [2,3]'s records
    // are gone. Nothing was recorded clean, since the compaction did't read
    // [0,1] and believed the start offset to be 0.
    auto after_compact = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(after_compact.has_value());
    EXPECT_EQ(
      after_compact->compaction_epoch,
      l1::metastore::compaction_epoch{initial_epoch() + 1});
    EXPECT_EQ(after_compact->start_offset, kafka::offset{2});
    ASSERT_FLOAT_EQ(after_compact->dirty_ratio, 1.0);
    ASSERT_TRUE(after_compact->offsets_response.dirty_ranges.covers(
      kafka::offset{2}, kafka::offset{5}));
    {
        auto batches = read_all(make_reader(ntp, tidp, kafka::offset{2}));
        ASSERT_EQ(batches.size(), 1);
        EXPECT_EQ(batches.front().base_offset(), model::offset{4});
        EXPECT_EQ(batches.front().record_count(), 2);
    }

    // A fresh pass converges: [2,3] is deemed clean since it is empty, [4,5]
    // gets cleaned.
    auto fresh = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(fresh.has_value());
    do_compact(
      tidp,
      ntp,
      std::move(fresh->offsets_response),
      fresh->compaction_epoch,
      fresh->start_offset,
      &_metastore,
      &_io)
      .get();

    auto final_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(final_info.has_value());
    EXPECT_FLOAT_EQ(final_info->dirty_ratio, 0.0);
    EXPECT_TRUE(final_info->offsets_response.dirty_ranges.empty());

    latest_kv_map_t latest_kv;
    latest_kv.insert_or_assign("a", "a4");
    latest_kv.insert_or_assign("b", "b4");
    verify_compacted_log(ntp, tidp, latest_kv, /*records=*/2, /*batches=*/1);
}

TEST_F(ReducerTestFixture, TombstoneReducer) {
    ss::abort_source as;
    auto [ntp, tidp] = make_ntidp("test_topic");
    int num_batches = 10;
    int records_per_batch = 150;
    int cardinality = 100;
    latest_kv_map_t latest_kv_map;
    auto batches = generate_batches(
      num_batches, cardinality, records_per_batch, 0, true, &latest_kv_map);
    kafka::offset start_offset{0};
    kafka::offset last_offset{num_batches * records_per_batch - 1};
    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};

    // First time obtaining compaction info and compacting. Expect a fully dirty
    // log.
    {
        auto compaction_info = _metastore.get_compaction_info(info_spec).get();

        ASSERT_TRUE(compaction_info.has_value());
        ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 1.0);
        ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
          start_offset, last_offset));
        ASSERT_TRUE(
          compaction_info->offsets_response.removable_tombstone_ranges.empty());

        do_compact(
          tidp,
          ntp,
          std::move(compaction_info->offsets_response),
          compaction_info->compaction_epoch,
          compaction_info->start_offset,
          &_metastore,
          &_io)
          .get();
    }

    // After compaction, verify a fully compacted log with latest_kv_map.
    verify_compacted_log(ntp, tidp, latest_kv_map, cardinality, 1);

    // Fully clean log has a dirty ratio of 0.0, but there are still removable
    // tombstones present.
    {
        auto compaction_info = _metastore.get_compaction_info(info_spec).get();
        ASSERT_TRUE(compaction_info.has_value());
        ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.0);
        ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());
        ASSERT_FALSE(
          compaction_info->offsets_response.removable_tombstone_ranges.empty());
        ASSERT_TRUE(
          compaction_info->offsets_response.removable_tombstone_ranges.covers(
            start_offset, last_offset));

        do_compact(
          tidp,
          ntp,
          std::move(compaction_info->offsets_response),
          compaction_info->compaction_epoch,
          compaction_info->start_offset,
          &_metastore,
          &_io)
          .get();
    }

    // Last compaction should have removed all tombstones from the log.
    {
        auto compaction_info = _metastore.get_compaction_info(info_spec).get();
        ASSERT_TRUE(compaction_info.has_value());
        ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.0);
        ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());
        ASSERT_TRUE(
          compaction_info->offsets_response.removable_tombstone_ranges.empty());
    }

    // All tombstones should have been removed.
    verify_compacted_log(ntp, tidp, latest_kv_map, 0, 0);
}

TEST_F(ReducerTestFixture, MinCompactionLagMsReducerIncreasingTimestamps) {
    // Produced objects have the following timestamps:
    // [0] - ts - 1h
    // [1] - ts - 2h
    // [2] - ts - 3h
    // [3] - ts - 4h
    // This test will compact repeatedly with a `min.compaction.lag.ms` such
    // that a new extent is eligible for compaction everytime, starting with no
    // objects compacted, and then again from [3].
    // We should expect that the dirty ranges of the log remain constant until
    // all extents in the log have become eligible for compaction.

    auto [ntp, tidp] = make_ntidp("test_topic");
    int num_produce_rounds = 4;
    int num_batches = 10;
    int num_records = 10;
    kafka::offset start_offset{0};
    kafka::offset last_offset{
      (num_produce_rounds * num_batches * num_records) - 1};
    auto gen = linear_int_kv_batch_generator();

    auto base_ts = model::timestamp::now();
    auto ago = [&](auto d) {
        return model::timestamp(
          base_ts.value()
          - std::chrono::duration_cast<std::chrono::milliseconds>(d).count());
    };

    auto make_l1_objects_with_ts = [&](model::timestamp ts) {
        model::test::record_batch_spec spec{
          .allow_compression = true,
          .count = num_records,
          .timestamp = ts,
          .all_records_have_same_timestamp = true};
        auto batches = gen(spec, num_batches);
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        return make_l1_objects(std::move(tidp_batches));
    };

    auto min_compaction_lag_ms
      = std::chrono::duration_cast<std::chrono::milliseconds>(4.5h);

    for (int i = 0; i < num_produce_rounds; ++i) {
        auto delta = std::chrono::hours(i + 1);
        make_l1_objects_with_ts(ago(delta)).get();
    }

    ss::abort_source as;

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};

    for (int i = 0; i <= num_produce_rounds; ++i) {
        auto compaction_info = _metastore.get_compaction_info(info_spec).get();
        ASSERT_TRUE(compaction_info.has_value());
        ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 1.0);
        ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
          start_offset, last_offset));

        auto dirty_range_intervals
          = compaction_info->offsets_response.dirty_ranges.to_vec();

        do_compact(
          tidp,
          ntp,
          std::move(compaction_info->offsets_response),
          compaction_info->compaction_epoch,
          compaction_info->start_offset,
          &_metastore,
          &_io,
          min_compaction_lag_ms)
          .get();

        auto reader = make_reader(ntp, tidp);
        auto output_batches = read_all(std::move(reader));

        ASSERT_EQ(output_batches.size(), num_batches * num_produce_rounds);
        int output_num_records = std::accumulate(
          output_batches.begin(),
          output_batches.end(),
          int{0},
          [](int acc, model::record_batch& b) {
              return acc + b.record_count();
          });

        // We have fully compacted `i` extents and left the others untouched.
        auto num_compacted = i;
        auto num_uncompacted = num_produce_rounds - num_compacted;
        auto expected_records = (num_batches * num_records * num_uncompacted)
                                + (num_batches * num_compacted);
        ASSERT_EQ(output_num_records, expected_records);

        min_compaction_lag_ms -= 1h;
    }

    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.0);
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());

    auto reader = make_reader(ntp, tidp);
    auto output_batches = read_all(std::move(reader));
    linear_int_kv_batch_generator::validate_post_compaction(
      std::move(output_batches));
}

TEST_F(ReducerTestFixture, MinCompactionLagMsReducerDecreasingTimestamps) {
    // Produced objects have the following timestamps:
    // [0] - ts - 4h
    // [1] - ts - 3h
    // [2] - ts - 2h
    // [3] - ts - 1h
    // This test will compact repeatedly with a `min.compaction.lag.ms` such
    // that a new extent is eligible for compaction everytime, starting from
    // [0].
    // We should expect that the dirty ranges of the log are incrementally
    // removed during each compaction run.

    auto [ntp, tidp] = make_ntidp("test_topic");
    int num_produce_rounds = 4;
    int num_batches = 10;
    int num_records = 10;
    kafka::offset start_offset{0};
    kafka::offset last_offset{
      (num_produce_rounds * num_batches * num_records) - 1};
    auto gen = linear_int_kv_batch_generator();

    auto base_ts = model::timestamp::now();
    auto ago = [&](auto d) {
        return model::timestamp(
          base_ts.value()
          - std::chrono::duration_cast<std::chrono::milliseconds>(d).count());
    };

    auto make_l1_objects_with_ts = [&](model::timestamp ts) {
        model::test::record_batch_spec spec{
          .allow_compression = true,
          .count = num_records,
          .timestamp = ts,
          .all_records_have_same_timestamp = true};
        auto batches = gen(spec, num_batches);
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        return make_l1_objects(std::move(tidp_batches));
    };

    auto min_compaction_lag_ms
      = std::chrono::duration_cast<std::chrono::milliseconds>(3.5h);

    for (int i = 0; i < num_produce_rounds; ++i) {
        auto delta = std::chrono::hours(num_produce_rounds - i);
        make_l1_objects_with_ts(ago(delta)).get();
    }

    ss::abort_source as;

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};

    auto prev_dirty_ratio = std::numeric_limits<double>::max();
    for (int i = 0; i < num_produce_rounds; ++i) {
        auto compaction_info = _metastore.get_compaction_info(info_spec).get();
        ASSERT_TRUE(compaction_info.has_value());
        ASSERT_LT(compaction_info->dirty_ratio, prev_dirty_ratio);
        prev_dirty_ratio = compaction_info->dirty_ratio;

        auto dirty_start_offset
          = start_offset + kafka::offset_delta(i * num_batches * num_records);
        ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
          dirty_start_offset, last_offset));

        auto dirty_range_intervals
          = compaction_info->offsets_response.dirty_ranges.to_vec();

        do_compact(
          tidp,
          ntp,
          std::move(compaction_info->offsets_response),
          compaction_info->compaction_epoch,
          compaction_info->start_offset,
          &_metastore,
          &_io,
          min_compaction_lag_ms)
          .get();

        auto reader = make_reader(ntp, tidp);
        auto output_batches = read_all(std::move(reader));

        ASSERT_EQ(output_batches.size(), num_batches * num_produce_rounds);
        int output_num_records = std::accumulate(
          output_batches.begin(),
          output_batches.end(),
          int{0},
          [](int acc, model::record_batch& b) {
              return acc + b.record_count();
          });

        // We have fully compacted `i+1` extents and left the others untouched.
        auto num_compacted = i + 1;
        auto num_uncompacted = num_produce_rounds - num_compacted;
        auto expected_records = (num_batches * num_records * num_uncompacted)
                                + (num_batches * num_compacted);
        ASSERT_EQ(output_num_records, expected_records);

        min_compaction_lag_ms -= 1h;
    }

    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.0);
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());

    auto reader = make_reader(ntp, tidp);
    auto output_batches = read_all(std::move(reader));
    linear_int_kv_batch_generator::validate_post_compaction(
      std::move(output_batches));
}

TEST_F(ReducerTestFixture, MinCompactionLagMsReducerInterleavedTimestamps) {
    // Produced objects have the following timestamps:
    // [0] - ts - 4h
    // [1] - ts - 2h
    // [2] - ts - 3h
    // [3] - ts - 1h
    // [4] - ts - 3h
    // This test will compact repeatedly with a `min.compaction.lag.ms` such
    // that new extent(s) are eligible for compaction in the order [0] ->
    // [2],[4] -> [1] -> [3]). We should expect that the dirty ranges of the log
    // may or may not be removed during each compaction run, depending on which
    // extent(s) have now become eligible for compaction.

    auto [ntp, tidp] = make_ntidp("test_topic");
    int num_produce_rounds = 5;
    int num_compact_rounds = 4;
    int num_batches = 10;
    int num_records = 10;
    kafka::offset last_offset{
      (num_produce_rounds * num_batches * num_records) - 1};
    auto gen = linear_int_kv_batch_generator();

    auto base_ts = model::timestamp::now();
    auto ago = [&](auto d) {
        return model::timestamp(
          base_ts.value()
          - std::chrono::duration_cast<std::chrono::milliseconds>(d).count());
    };

    auto make_l1_objects_with_ts = [&](model::timestamp ts) {
        model::test::record_batch_spec spec{
          .allow_compression = true,
          .count = num_records,
          .timestamp = ts,
          .all_records_have_same_timestamp = true};
        auto batches = gen(spec, num_batches);
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        return make_l1_objects(std::move(tidp_batches));
    };

    auto min_compaction_lag_ms
      = std::chrono::duration_cast<std::chrono::milliseconds>(3.5h);

    make_l1_objects_with_ts(ago(4h)).get();
    make_l1_objects_with_ts(ago(2h)).get();
    make_l1_objects_with_ts(ago(3h)).get();
    make_l1_objects_with_ts(ago(1h)).get();
    make_l1_objects_with_ts(ago(3h)).get();

    // The number of expected compacted extents for each round of compaction
    std::vector<size_t> expected_num_compacted_extents = {1, 3, 4, 5};
    using ko = kafka::offset;

    // The expected dirty start offset for each round of compaction
    std::vector<ko> expected_dirty_start_offsets = {
      ko{0},                             // No compacted extents
      ko{1 * num_batches * num_records}, // [1] left uncompacted
      ko{1 * num_batches * num_records}, // [1] left uncompacted
      ko{3 * num_batches * num_records}  // [3] left uncompacted
    };

    ss::abort_source as;

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};

    auto prev_dirty_ratio = std::numeric_limits<double>::max();
    for (int i = 0; i < num_compact_rounds; ++i) {
        auto compaction_info = _metastore.get_compaction_info(info_spec).get();
        ASSERT_TRUE(compaction_info.has_value());
        ASSERT_LE(compaction_info->dirty_ratio, prev_dirty_ratio);
        prev_dirty_ratio = compaction_info->dirty_ratio;

        auto dirty_start_offset = expected_dirty_start_offsets[i];
        ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
          dirty_start_offset, last_offset));

        auto dirty_range_intervals
          = compaction_info->offsets_response.dirty_ranges.to_vec();

        do_compact(
          tidp,
          ntp,
          std::move(compaction_info->offsets_response),
          compaction_info->compaction_epoch,
          compaction_info->start_offset,
          &_metastore,
          &_io,
          min_compaction_lag_ms)
          .get();

        auto reader = make_reader(ntp, tidp);
        auto output_batches = read_all(std::move(reader));

        ASSERT_EQ(output_batches.size(), num_batches * num_produce_rounds);
        int output_num_records = std::accumulate(
          output_batches.begin(),
          output_batches.end(),
          int{0},
          [](int acc, model::record_batch& b) {
              return acc + b.record_count();
          });

        // We have fully compacted some extents and left the others untouched.
        auto num_compacted = expected_num_compacted_extents[i];
        auto num_uncompacted = num_produce_rounds - num_compacted;
        auto expected_records = (num_batches * num_records * num_uncompacted)
                                + (num_batches * num_compacted);
        ASSERT_EQ(output_num_records, expected_records);

        min_compaction_lag_ms -= 1h;
    }

    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.0);
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());

    auto reader = make_reader(ntp, tidp);
    auto output_batches = read_all(std::move(reader));
    linear_int_kv_batch_generator::validate_post_compaction(
      std::move(output_batches));
}

// An extent's max timestamp may be ahead of the local clock: producers may set
// create-time up to log_message_timestamp_after_max_ms (1h by default) in the
// future, and the system clock may step backwards. With min.compaction.lag.ms
// set, such an extent is held back until the clock catches up (as in Kafka's
// `largestTimestamp > now - minCompactionLagMs`), but with the lag unset the
// timestamp must not be consulted at all and the extent must still compact.
TEST_F(ReducerTestFixture, MinCompactionLagMsUnsetTimestampsInFuture) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    int num_produce_rounds = 4;
    int num_batches = 10;
    int num_records = 10;
    kafka::offset start_offset{0};
    kafka::offset last_offset{
      (num_produce_rounds * num_batches * num_records) - 1};
    auto gen = linear_int_kv_batch_generator();

    const auto future_ts = model::timestamp(
      model::timestamp::now().value() + std::chrono::milliseconds{1h}.count());

    for (int i = 0; i < num_produce_rounds; ++i) {
        model::test::record_batch_spec spec{
          .allow_compression = true,
          .count = num_records,
          .timestamp = future_ts,
          .all_records_have_same_timestamp = true};
        auto batches = gen(spec, num_batches);
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};

    // With a lag configured, the future timestamps hold every extent back.
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
      start_offset, last_offset));
    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      compaction_info->compaction_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io,
      /*min_compaction_lag_ms=*/1h)
      .get();

    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
      start_offset, last_offset));

    // With the lag unset, the same extents compact despite the future
    // timestamps.
    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      compaction_info->compaction_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io,
      /*min_compaction_lag_ms=*/0ms)
      .get();

    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.0);
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());

    auto reader = make_reader(ntp, tidp);
    auto output_batches = read_all(std::move(reader));
    linear_int_kv_batch_generator::validate_post_compaction(
      std::move(output_batches));
}

TEST_F(ReducerTestFixture, MaxCompactibleOffsetReducer) {
    // This test verifies that compaction respects the max_compactible_offset
    // boundary. We create multiple extents and set max_compactible_offset to
    // fall within the middle of the log, expecting only extents below that
    // offset to be compacted.
    auto [ntp, tidp] = make_ntidp("test_topic");
    int num_produce_rounds = 4;
    int num_batches = 10;
    int num_records = 10;
    int records_per_extent = num_batches * num_records;
    kafka::offset start_offset{0};
    kafka::offset last_offset{(num_produce_rounds * records_per_extent) - 1};
    auto gen = linear_int_kv_batch_generator();
    auto ts = model::timestamp::now();

    // Create 4 extents.
    for (int i = 0; i < num_produce_rounds; ++i) {
        model::test::record_batch_spec spec{
          .allow_compression = true,
          .count = num_records,
          .timestamp = ts,
          .all_records_have_same_timestamp = true};
        auto batches = gen(spec, num_batches);
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();

    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 1.0);
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
      start_offset, last_offset));

    // Set max_compactible_offset to the start of the 3rd extent.
    // This should allow compaction of extents [0] and [1], but not [2] or [3].
    auto max_compactible_offset = kafka::offset{2 * records_per_extent};

    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      compaction_info->compaction_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io,
      0ms,
      max_compactible_offset)
      .get();

    auto reader = make_reader(ntp, tidp);
    auto output_batches = read_all(std::move(reader));

    // Extents [0] and [1] should be compacted (num_batches records each after
    // dedup), extents [2] and [3] should remain untouched (num_batches *
    // num_records each).
    int output_num_records = std::accumulate(
      output_batches.begin(),
      output_batches.end(),
      int{0},
      [](int acc, model::record_batch& b) { return acc + b.record_count(); });

    auto compacted_records = 2 * num_batches;          // 2 compacted extents
    auto uncompacted_records = 2 * records_per_extent; // 2 untouched extents
    ASSERT_EQ(output_num_records, compacted_records + uncompacted_records);

    // Verify dirty ranges still include the uncompacted extents.
    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_GT(compaction_info->dirty_ratio, 0.0);
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
      max_compactible_offset, last_offset));
}

// The sink commits accumulated output objects as partial compaction commits
// at source extent boundaries, each bumping the compaction epoch, then
// records the job's compaction metadata in a final metadata-only commit that
// bumps it once more. A tiny max object size makes every source extent's
// output exceed the threshold, so the job issues one partial commit per
// source extent.
TEST_F(ReducerTestFixture, IncrementalCommitsAdvanceEpochPerPartialCommit) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    constexpr int num_extents = 3;
    constexpr int batches_per_extent = 10;
    constexpr int records_per_batch = 5;
    constexpr int total_records = num_extents * batches_per_extent
                                  * records_per_batch;

    // Split one contiguous batch stream across several source extents; all
    // keys are distinct so nothing is deduplicated away and every extent
    // produces enough output to reach the commit interval.
    latest_kv_map_t latest_kv_map;
    auto batches = generate_batches(
      num_extents * batches_per_extent,
      /*cardinality=*/total_records,
      records_per_batch,
      /*starting_value=*/0,
      /*produce_tombstones=*/false,
      &latest_kv_map);
    for (int i = 0; i < num_extents; ++i) {
        chunked_circular_buffer<model::record_batch> extent_batches;
        for (int j = 0; j < batches_per_extent; ++j) {
            extent_batches.push_back(std::move(batches.front()));
            batches.pop_front();
        }
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(extent_batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    auto initial_epoch = compaction_info->compaction_epoch;

    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      compaction_info->compaction_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io,
      0ms,
      kafka::offset::max(),
      /*max_object_size=*/512,
      /*commit_interval_bytes=*/512)
      .get();

    // One partial commit per source extent, plus the final metadata-only
    // commit.
    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    EXPECT_EQ(
      compaction_info->compaction_epoch(), initial_epoch() + num_extents + 1);
    EXPECT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());

    // Nothing was lost: every distinct key survives.
    verify_compacted_log(
      ntp,
      tidp,
      latest_kv_map,
      total_records,
      num_extents * batches_per_extent);
}

// Commit cadence is governed by the commit interval, not the object size:
// with an interval larger than the job's entire output, no boundary commit
// fires even though every boundary has multiple finished objects pending,
// and the whole job commits as one batch at finalize (plus the final
// metadata-only commit).
TEST_F(ReducerTestFixture, CommitIntervalGovernsCommitCadence) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    constexpr int num_extents = 3;
    constexpr int batches_per_extent = 8;
    constexpr int records_per_batch = 5;
    constexpr int total_records = num_extents * batches_per_extent
                                  * records_per_batch;

    latest_kv_map_t latest_kv_map;
    auto batches = generate_batches(
      num_extents * batches_per_extent,
      /*cardinality=*/total_records,
      records_per_batch,
      /*starting_value=*/0,
      /*produce_tombstones=*/false,
      &latest_kv_map);
    for (int i = 0; i < num_extents; ++i) {
        chunked_circular_buffer<model::record_batch> extent_batches;
        for (int j = 0; j < batches_per_extent; ++j) {
            extent_batches.push_back(std::move(batches.front()));
            batches.pop_front();
        }
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(extent_batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    auto initial_epoch = compaction_info->compaction_epoch;

    // Objects roll at 512 bytes, so every boundary has finished objects
    // pending — but the commit interval is far larger than the job's whole
    // output, so no boundary commit fires. The job's output lands in one
    // commit from the finalize path, followed by the metadata-only commit:
    // exactly two epoch bumps.
    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      initial_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io,
      0ms,
      kafka::offset::max(),
      /*max_object_size=*/512,
      /*commit_interval_bytes=*/1_MiB)
      .get();

    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    EXPECT_EQ(compaction_info->compaction_epoch(), initial_epoch() + 2);
    EXPECT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());

    verify_compacted_log(
      ntp,
      tidp,
      latest_kv_map,
      total_records,
      num_extents * batches_per_extent);
}

// After a partial commit cuts at an extent boundary, the next extent's
// leading batches may be deduplicated away entirely (all their records
// superseded later in the log), so the first batch reaching the sink sits
// past the extent base. The next object must still be anchored at the
// extent base — a mid-extent leading edge would fail the metastore's
// span-exact validation and reject every subsequent partial commit.
TEST_F(ReducerTestFixture, PostCommitObjectAnchorsAtExtentBase) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    constexpr int records_per_batch = 5;
    constexpr int batches_per_extent = 4;
    constexpr int records_per_extent = records_per_batch * batches_per_extent;

    // Three extents. Extent 2's first batch reuses keys that extent 3's
    // first batch overwrites, so it is fully deduplicated away; every
    // other batch has unique keys and survives.
    latest_kv_map_t latest_kv_map;
    auto make_extent_batches = [&](
                                 int extent_idx, bool reused_first_batch_keys) {
        chunked_circular_buffer<model::record_batch> batches;
        for (int b = 0; b < batches_per_extent; ++b) {
            auto base_offset = model::offset(
              extent_idx * records_per_extent + b * records_per_batch);
            // The reused key range lives at [1000, 1005); unique keys
            // equal their record offsets.
            auto kvs = reused_first_batch_keys && b == 0
                         ? tests::kv_t::sequence(
                             /*start=*/extent_idx * records_per_extent,
                             records_per_batch,
                             /*val_start=*/extent_idx * records_per_extent,
                             /*cardinality=*/records_per_batch,
                             /*produce_tombstones=*/false,
                             /*base=*/1000)
                         : tests::kv_t::sequence(
                             /*start=*/base_offset(),
                             records_per_batch,
                             /*val_start=*/base_offset(),
                             /*cardinality=*/10000);
            for (const auto& kv : kvs) {
                latest_kv_map.insert_or_assign(kv.key, kv.val);
            }
            batches.push_back(
              tests::batch_from_kvs(
                kvs,
                base_offset,
                model::timestamp::now(),
                model::compression::none));
        }
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    };
    make_extent_batches(0, false);
    make_extent_batches(1, true);
    make_extent_batches(2, true);

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    auto initial_epoch = compaction_info->compaction_epoch;

    // Per-boundary commits: extent 1's commit leaves the anchor, extent 2's
    // first batch is dropped by the filter, and extent 2's commit must
    // still span exactly from the extent base.
    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      initial_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io,
      0ms,
      kafka::offset::max(),
      /*max_object_size=*/512,
      /*commit_interval_bytes=*/512)
      .get();

    // Two partial commits (extent 2's output alone stays under the commit
    // interval, so extents 2 and 3 land together at extent 3's boundary —
    // still spanning from extent 2's base) plus the metadata-only commit,
    // and a fully clean log.
    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    EXPECT_EQ(compaction_info->compaction_epoch(), initial_epoch() + 3);
    EXPECT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());

    // Extent 2's first batch is gone; everything else survives.
    verify_compacted_log(
      ntp,
      tidp,
      latest_kv_map,
      /*expected_records=*/3 * records_per_extent - records_per_batch,
      /*expected_batches=*/3 * batches_per_extent - 1);
}

// A boundary commit cuts coverage; the next object is anchored at the
// boundary's successor when the next extent is prepared. If that extent is
// fully deduplicated away (all its keys rewritten in a later extent) and a
// live extent follows contiguously, the object must stay open across the
// dead extent and swallow its span: the live extent's batches land in an
// object based at the dead extent's base, so the next commit replaces both
// extents exactly.
TEST_F(ReducerTestFixture, DeadInteriorExtentIsSwallowedAfterCut) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    constexpr int records_per_batch = 5;
    constexpr int batches_per_extent = 4;
    constexpr int records_per_extent = records_per_batch * batches_per_extent;
    constexpr int num_extents = 3;

    // Extent 0 [0,19]: unique keys, survives and triggers the boundary
    // commit. Extent 1 [20,39]: keys 1000..1019 with old values. Extent 2
    // [40,59]: the same keys with new values, so every record of extent 1
    // is superseded while extent 2 survives in full.
    latest_kv_map_t latest_kv_map;
    for (int extent_idx = 0; extent_idx < num_extents; ++extent_idx) {
        const bool reused_keys = extent_idx >= 1;
        chunked_circular_buffer<model::record_batch> batches;
        for (int b = 0; b < batches_per_extent; ++b) {
            auto base_offset = model::offset(
              extent_idx * records_per_extent + b * records_per_batch);
            auto kvs = reused_keys ? tests::kv_t::sequence(
                                       /*start=*/b * records_per_batch,
                                       records_per_batch,
                                       /*val_start=*/base_offset(),
                                       /*cardinality=*/records_per_extent,
                                       /*produce_tombstones=*/false,
                                       /*base=*/1000)
                                   : tests::kv_t::sequence(
                                       /*start=*/base_offset(),
                                       records_per_batch,
                                       /*val_start=*/base_offset(),
                                       /*cardinality=*/10000);
            for (const auto& kv : kvs) {
                latest_kv_map.insert_or_assign(kv.key, kv.val);
            }
            batches.push_back(
              tests::batch_from_kvs(
                kvs,
                base_offset,
                model::timestamp::now(),
                model::compression::none));
        }
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    auto initial_epoch = compaction_info->compaction_epoch;

    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      initial_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io,
      0ms,
      kafka::offset::max(),
      /*max_object_size=*/512,
      /*commit_interval_bytes=*/512)
      .get();

    // Two data commits — extent 0's boundary cut, then the object spanning
    // dead extent 1 plus live extent 2 — and the metadata-only commit. A
    // mis-anchored object would fail span-exact validation and abort the
    // run instead.
    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    EXPECT_EQ(compaction_info->compaction_epoch(), initial_epoch() + 3);
    EXPECT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());

    // Extent 1's superseded records are gone; extents 0 and 2 survive.
    verify_compacted_log(
      ntp,
      tidp,
      latest_kv_map,
      /*expected_records=*/2 * records_per_extent,
      /*expected_batches=*/2 * batches_per_extent);
}

// A boundary commit cuts coverage — but the very next extent is skipped by
// min_compaction_lag_ms (fresh timestamps), so the source jumps with
// nothing processed since the cut. No object may claim anything between
// the cut and the jump: the next object must re-anchor at the live
// extent's base, and the skipped extent must stay dirty with no claims
// over it.
TEST_F(ReducerTestFixture, JumpAfterCutReanchorsAtLiveExtent) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    constexpr int records_per_batch = 5;
    constexpr int batches_per_extent = 4;
    constexpr int records_per_extent = records_per_batch * batches_per_extent;
    constexpr int num_extents = 3;
    constexpr auto lag = std::chrono::hours(1);

    // Extents 0 [0,19] and 2 [40,59] are older than the compaction lag;
    // extent 1 [20,39] is fresh, so the deduplication pass jumps over it.
    // All keys are unique so every streamed record survives.
    const auto old_ts = model::timestamp(
      model::timestamp::now()() - 2 * std::chrono::milliseconds(lag).count());
    latest_kv_map_t latest_kv_map;
    for (int extent_idx = 0; extent_idx < num_extents; ++extent_idx) {
        const auto ts = extent_idx == 1 ? model::timestamp::now() : old_ts;
        chunked_circular_buffer<model::record_batch> batches;
        for (int b = 0; b < batches_per_extent; ++b) {
            auto base_offset = model::offset(
              extent_idx * records_per_extent + b * records_per_batch);
            auto kvs = tests::kv_t::sequence(
              /*start=*/base_offset(),
              records_per_batch,
              /*val_start=*/base_offset(),
              /*cardinality=*/10000);
            for (const auto& kv : kvs) {
                latest_kv_map.insert_or_assign(kv.key, kv.val);
            }
            batches.push_back(
              tests::batch_from_kvs(
                kvs, base_offset, ts, model::compression::none));
        }
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    auto initial_epoch = compaction_info->compaction_epoch;

    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      initial_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io,
      /*min_compaction_lag_ms=*/lag,
      kafka::offset::max(),
      /*max_object_size=*/512,
      /*commit_interval_bytes=*/512)
      .get();

    // Two data commits — extent 0's boundary cut and extent 2's rewrite —
    // plus the metadata-only commit; nothing contiguous follows either
    // cut, so no object is started until the re-anchor at extent 2.
    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    EXPECT_EQ(compaction_info->compaction_epoch(), initial_epoch() + 3);

    // Only the range below the skipped extent may be claimed clean; the
    // skipped extent and everything above it stay dirty.
    EXPECT_FALSE(compaction_info->offsets_response.dirty_ranges.covers(
      kafka::offset{0}, kafka::offset{records_per_extent - 1}));
    EXPECT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
      kafka::offset{records_per_extent},
      kafka::offset{3 * records_per_extent - 1}));

    // Nothing was lost, including the skipped (lag-protected) extent.
    verify_compacted_log(
      ntp,
      tidp,
      latest_kv_map,
      /*expected_records=*/num_extents * records_per_extent,
      /*expected_batches=*/num_extents * batches_per_extent);

    // Once the lag expires, a second pass compacts the skipped extent and
    // the claims converge over the whole log.
    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      compaction_info->compaction_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io,
      /*min_compaction_lag_ms=*/0ms,
      kafka::offset::max(),
      /*max_object_size=*/512,
      /*commit_interval_bytes=*/512)
      .get();

    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    EXPECT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());
    verify_compacted_log(
      ntp,
      tidp,
      latest_kv_map,
      /*expected_records=*/num_extents * records_per_extent,
      /*expected_batches=*/num_extents * batches_per_extent);
}

// The non-contiguous variant of the dead-tail case below: a boundary
// commit cuts coverage at extent 0 and extent 1 is fully deduplicated
// away (all its keys rewritten in extent 3), and then the source jumps
// over extent 2 — skipped by min_compaction_lag_ms, its timestamps being
// newer than the extents around it — so prepare_iteration() re-anchors at
// extent 3 and nothing would replace extent 1, even though the final
// metadata claims its range clean. The sink must claim the dead span with
// an (empty) object before the jump.
TEST_F(ReducerTestFixture, DeadTailExtentIsReplacedBeforeJump) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    constexpr int records_per_batch = 5;
    constexpr int batches_per_extent = 4;
    constexpr int records_per_extent = records_per_batch * batches_per_extent;
    constexpr int num_extents = 4;
    constexpr auto lag = std::chrono::hours(1);

    // Extents 0, 1, 3 are older than the compaction lag; extent 2 is
    // fresh, so the deduplication pass skips over it. Extent 1's keys are
    // all rewritten in extent 3, making every one of its records
    // superseded.
    const auto old_ts = model::timestamp(
      model::timestamp::now()() - 2 * std::chrono::milliseconds(lag).count());
    latest_kv_map_t latest_kv_map;
    for (int extent_idx = 0; extent_idx < num_extents; ++extent_idx) {
        const bool reused_keys = extent_idx == 1 || extent_idx == 3;
        const auto ts = extent_idx == 2 ? model::timestamp::now() : old_ts;
        chunked_circular_buffer<model::record_batch> batches;
        for (int b = 0; b < batches_per_extent; ++b) {
            auto base_offset = model::offset(
              extent_idx * records_per_extent + b * records_per_batch);
            auto kvs = reused_keys ? tests::kv_t::sequence(
                                       /*start=*/b * records_per_batch,
                                       records_per_batch,
                                       /*val_start=*/base_offset(),
                                       /*cardinality=*/records_per_extent,
                                       /*produce_tombstones=*/false,
                                       /*base=*/1000)
                                   : tests::kv_t::sequence(
                                       /*start=*/base_offset(),
                                       records_per_batch,
                                       /*val_start=*/base_offset(),
                                       /*cardinality=*/10000);
            for (const auto& kv : kvs) {
                latest_kv_map.insert_or_assign(kv.key, kv.val);
            }
            batches.push_back(
              tests::batch_from_kvs(
                kvs, base_offset, ts, model::compression::none));
        }
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());

    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      compaction_info->compaction_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io,
      /*min_compaction_lag_ms=*/lag,
      kafka::offset::max(),
      /*max_object_size=*/512,
      /*commit_interval_bytes=*/512)
      .get();

    // Extent 1's superseded records must actually be gone — not merely
    // claimed clean while the old extent still serves them. Extent 2 is
    // untouched (too fresh to compact).
    verify_compacted_log(
      ntp,
      tidp,
      latest_kv_map,
      /*expected_records=*/(num_extents - 1) * records_per_extent,
      /*expected_batches=*/(num_extents - 1) * batches_per_extent);

    // The skipped extent held back every claim at or above it: only the
    // range below it was marked clean, and everything from the skipped
    // extent up stays dirty — including extent 3, which was rewritten but
    // may not be claimed across the gap.
    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    EXPECT_FALSE(compaction_info->offsets_response.dirty_ranges.covers(
      kafka::offset{0}, kafka::offset{2 * records_per_extent - 1}));
    EXPECT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
      kafka::offset{2 * records_per_extent},
      kafka::offset{4 * records_per_extent - 1}));

    // Once the lag expires, a second pass compacts the skipped extent and
    // the claims converge: the whole log is clean and the data unchanged.
    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      compaction_info->compaction_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io,
      /*min_compaction_lag_ms=*/0ms,
      kafka::offset::max(),
      /*max_object_size=*/512,
      /*commit_interval_bytes=*/512)
      .get();

    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    EXPECT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());
    verify_compacted_log(
      ntp,
      tidp,
      latest_kv_map,
      /*expected_records=*/(num_extents - 1) * records_per_extent,
      /*expected_batches=*/(num_extents - 1) * batches_per_extent);
}

// A boundary commit cuts coverage at an extent boundary and leaves the
// anchor for the next object. If every extent after that boundary is fully
// deduplicated away — here, a trailing extent of expired tombstones — the
// anchor is never consumed and nothing replaces those extents. Yet
// finish_iteration() recorded them as processed, so the final metadata
// claims their range clean and their tombstones removed while the old
// extent still physically serves them, and the range is never compacted
// again. The sink must claim the dead span with an (empty) object before
// coverage ends.
TEST_F(ReducerTestFixture, DeadTailExtentIsReplacedAtJobEnd) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    constexpr int records_per_batch = 5;
    constexpr int batches_per_extent = 4;
    constexpr int records_per_extent = records_per_batch * batches_per_extent;
    constexpr int num_extents = 4;

    // Extents 0-2: unique live keys. Extent 3 [60,79]: only tombstones.
    latest_kv_map_t latest_kv_map;
    for (int extent_idx = 0; extent_idx < num_extents; ++extent_idx) {
        const bool tombstones = extent_idx == num_extents - 1;
        chunked_circular_buffer<model::record_batch> batches;
        for (int b = 0; b < batches_per_extent; ++b) {
            auto base_offset = model::offset(
              extent_idx * records_per_extent + b * records_per_batch);
            auto kvs = tests::kv_t::sequence(
              /*start=*/base_offset(),
              records_per_batch,
              /*val_start=*/base_offset(),
              /*cardinality=*/10000,
              tombstones);
            for (const auto& kv : kvs) {
                latest_kv_map.insert_or_assign(kv.key, kv.val);
            }
            batches.push_back(
              tests::batch_from_kvs(
                kvs,
                base_offset,
                model::timestamp::now(),
                model::compression::none));
        }
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());

    // Record extent 3's range as cleaned-with-tombstones so that, with
    // tombstone_removal_upper_bound_ts=max, the job sees its tombstones as
    // removable and drops every one of its records.
    chunked_vector<l1::metastore::compaction_update::cleaned_range> cleaned;
    cleaned.push_back(
      l1::metastore::compaction_update::cleaned_range{
        .base_offset = kafka::offset{3 * records_per_extent},
        .last_offset = kafka::offset{4 * records_per_extent - 1},
        .has_tombstones = true});
    l1::metastore::compaction_map_t seed;
    seed.emplace(
      tidp,
      l1::metastore::compaction_update{
        .new_cleaned_ranges = std::move(cleaned),
        .cleaned_at = model::timestamp::now(),
        .expected_compaction_epoch = compaction_info->compaction_epoch});
    ASSERT_TRUE(_metastore.commit_compaction_metadata(seed).get());

    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_TRUE(
      compaction_info->offsets_response.removable_tombstone_ranges.covers(
        kafka::offset{3 * records_per_extent},
        kafka::offset{4 * records_per_extent - 1}));

    // Every live extent's output crosses the commit interval at its
    // boundary, so extent 2's boundary commit leaves the anchor at extent
    // 3's base; extent 3 then contributes nothing and the job ends.
    do_compact(
      tidp,
      ntp,
      std::move(compaction_info->offsets_response),
      compaction_info->compaction_epoch,
      compaction_info->start_offset,
      &_metastore,
      &_io,
      0ms,
      kafka::offset::max(),
      /*max_object_size=*/512,
      /*commit_interval_bytes=*/512)
      .get();

    // The log is fully clean: nothing dirty, no removable tombstones left
    // (the dead range's cleaned-with-tombstones entry was erased), and the
    // empty object's extent is a first-class replacement in the state.
    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    EXPECT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());
    EXPECT_TRUE(
      compaction_info->offsets_response.removable_tombstone_ranges.empty());

    // Extent 3's expired tombstones must actually be gone — not merely
    // claimed removed while the old extent still serves them.
    verify_compacted_log(
      ntp,
      tidp,
      latest_kv_map,
      /*expected_records=*/(num_extents - 1) * records_per_extent,
      /*expected_batches=*/(num_extents - 1) * batches_per_extent);
}

// A partial commit rejected by the metastore (stale compaction epoch, e.g.
// a competing job committed after this job read its snapshot) aborts the
// run: no extents are replaced and no compaction metadata is recorded, so
// the log stays fully dirty for the retry.
TEST_F(ReducerTestFixture, StaleEpochAbortsWithoutMetadata) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    constexpr int num_extents = 3;
    constexpr int batches_per_extent = 10;
    constexpr int records_per_batch = 5;
    constexpr int total_records = num_extents * batches_per_extent
                                  * records_per_batch;

    latest_kv_map_t latest_kv_map;
    auto batches = generate_batches(
      num_extents * batches_per_extent,
      /*cardinality=*/total_records,
      records_per_batch,
      /*starting_value=*/0,
      /*produce_tombstones=*/false,
      &latest_kv_map);
    for (int i = 0; i < num_extents; ++i) {
        chunked_circular_buffer<model::record_batch> extent_batches;
        for (int j = 0; j < batches_per_extent; ++j) {
            extent_batches.push_back(std::move(batches.front()));
            batches.pop_front();
        }
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(extent_batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    auto initial_epoch = compaction_info->compaction_epoch;

    // A competing job bumps the epoch after this job read its snapshot.
    l1::metastore::compaction_map_t bump;
    bump.emplace(
      tidp,
      l1::metastore::compaction_update{
        .expected_compaction_epoch = initial_epoch});
    auto bump_res = _metastore.commit_compaction_metadata(bump).get();
    ASSERT_TRUE(bump_res.has_value());

    // The first partial commit is rejected as stale and aborts the run.
    EXPECT_THROW(
      do_compact(
        tidp,
        ntp,
        std::move(compaction_info->offsets_response),
        initial_epoch,
        compaction_info->start_offset,
        &_metastore,
        &_io,
        0ms,
        kafka::offset::max(),
        /*max_object_size=*/512,
        /*commit_interval_bytes=*/512)
        .get(),
      std::runtime_error);

    // Only the competing bump moved the epoch; the rejected commit replaced
    // nothing and recorded no metadata, so the log is still fully dirty.
    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    EXPECT_EQ(compaction_info->compaction_epoch(), initial_epoch() + 1);
    EXPECT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
      kafka::offset{0}, kafka::offset{total_records - 1}));

    // Nothing was lost: every record is still readable.
    verify_compacted_log(
      ntp,
      tidp,
      latest_kv_map,
      total_records,
      num_extents * batches_per_extent);
}

// A job that fails after a partial commit has landed keeps the committed
// prefix durable but must not record compaction metadata: the extents the
// partial commit replaced stay replaced, while every cleaned range stays
// dirty so the retry recompacts them.
TEST_F(ReducerTestFixture, AbortAfterPartialCommitSkipsMetadata) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    constexpr int num_extents = 3;
    constexpr int batches_per_extent = 10;
    constexpr int records_per_batch = 5;
    constexpr int total_records = num_extents * batches_per_extent
                                  * records_per_batch;

    latest_kv_map_t latest_kv_map;
    auto batches = generate_batches(
      num_extents * batches_per_extent,
      /*cardinality=*/total_records,
      records_per_batch,
      /*starting_value=*/0,
      /*produce_tombstones=*/false,
      &latest_kv_map);
    for (int i = 0; i < num_extents; ++i) {
        chunked_circular_buffer<model::record_batch> extent_batches;
        for (int j = 0; j < batches_per_extent; ++j) {
            extent_batches.push_back(std::move(batches.front()));
            batches.pop_front();
        }
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(extent_batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    auto initial_epoch = compaction_info->compaction_epoch;

    // Throw partway through the second extent: the first extent's boundary
    // commit has landed by then, and the job dies before finishing.
    int batch_count = 0;
    EXPECT_THROW(
      do_compact_with_throwing_sink(
        tidp,
        ntp,
        std::move(compaction_info->offsets_response),
        initial_epoch,
        compaction_info->start_offset,
        &_metastore,
        &_io,
        /*should_roll=*/[] { return false; },
        /*should_throw=*/
        [&batch_count] { return ++batch_count == batches_per_extent + 5; },
        0ms,
        kafka::offset::max(),
        /*max_object_size=*/512,
        /*commit_interval_bytes=*/512)
        .get(),
      std::runtime_error);

    // Exactly one partial commit landed (one epoch bump), and no compaction
    // metadata was recorded, so the whole log is still dirty for the retry.
    compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());
    EXPECT_EQ(compaction_info->compaction_epoch(), initial_epoch() + 1);
    EXPECT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
      kafka::offset{0}, kafka::offset{total_records - 1}));

    // The committed prefix was durably replaced: the first extent's range is
    // now backed by more, smaller extents.
    auto extents = _metastore
                     .get_extent_metadata_forwards(
                       tidp,
                       kafka::offset{0},
                       kafka::offset::max(),
                       std::numeric_limits<size_t>::max(),
                       l1::metastore::include_object_metadata::no)
                     .get();
    ASSERT_TRUE(extents.has_value());
    EXPECT_GT(extents->extents.size(), static_cast<size_t>(num_extents));

    // Nothing was lost: every record is still readable.
    verify_compacted_log(
      ntp,
      tidp,
      latest_kv_map,
      total_records,
      num_extents * batches_per_extent);
}

// Tests the exceptional path in compaction_sink::finalize() where the inflight
// object has been rolled mid-extent (Case 1 from the finalize() comments).
// Setup: two contiguous extents [[0,9], [10,19]], 1 record per batch.
// A forced roll at batch 15 creates a new object with object_base_offset=15.
// The throw fires immediately after, before finish_iteration for [10,19].
// At finalize(false), _processed_extents = {[0,9]}, last_offset=9.
// Flushing the inflight object would give offsets [15,9] - an inverted range,
// which would trigger an assert. `finalize()` must discard it instead.
TEST_F(ReducerTestFixture, ExceptionalFinalizeAfterObjectRoll) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    int batches_per_extent = 10;
    auto gen = linear_int_kv_batch_generator();
    auto ts = model::timestamp::now();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false,
      .count = 1,
      .timestamp = ts,
      .all_records_have_same_timestamp = true};

    // Two extents, 1 record per batch, 10 batches each -> offsets [0,9] and
    // [10,19].
    for (int i = 0; i < 2; ++i) {
        auto batches = gen(spec, batches_per_extent);
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());

    // Roll at batch 15 (offset 15, i.e. the 6th batch of the second extent),
    // then throw immediately after.
    int batch_count = 0;
    int roll_at = batches_per_extent + 5;
    auto should_roll = [&batch_count, roll_at]() -> bool {
        return batch_count == roll_at;
    };
    auto should_throw = [&batch_count, roll_at]() -> bool {
        ++batch_count;
        return batch_count == roll_at + 1;
    };

    EXPECT_THROW(
      do_compact_with_throwing_sink(
        tidp,
        ntp,
        std::move(compaction_info->offsets_response),
        compaction_info->compaction_epoch,
        compaction_info->start_offset,
        &_metastore,
        &_io,
        std::move(should_roll),
        std::move(should_throw))
        .get(),
      std::runtime_error);

    // The log must still be readable after the exceptional compaction.
    auto reader = make_reader(ntp, tidp);
    auto output_batches = read_all(std::move(reader));
    ASSERT_FALSE(output_batches.empty());
}

// Tests the exceptional path in compaction_sink::finalize() where the inflight
// object contains partially-processed extent data beyond what
// _processed_extents tracks (Case 2 from the finalize() comments).
//
// Setup: two contiguous extents [[0,9], [10,19]], 1 record per batch.
// No roll occurs — the single object spans both extents. The throw fires at
// batch 15 (offset 15), before finish_iteration for [10,19].
// At finalize(false), _processed_extents = {[0,9]}, last_offset=9.
// Flushing would give an object with offsets [0,9], but the object actually
// contains data up to offset 15. finalize must discard it instead.
TEST_F(ReducerTestFixture, ExceptionalFinalizePartialExtent) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    int batches_per_extent = 10;
    auto gen = linear_int_kv_batch_generator();
    auto ts = model::timestamp::now();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false,
      .count = 1,
      .timestamp = ts,
      .all_records_have_same_timestamp = true};

    for (int i = 0; i < 2; ++i) {
        auto batches = gen(spec, batches_per_extent);
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();
    ASSERT_TRUE(compaction_info.has_value());

    // No roll. Throw at batch 15 (offset 15, the 6th batch of the second
    // extent). The inflight object contains data for [0,15] but
    // _processed_extents only covers [0,9].
    int batch_count = 0;
    int throw_at = batches_per_extent + 5;
    auto no_roll = []() -> bool { return false; };
    auto should_throw = [&batch_count, throw_at]() -> bool {
        ++batch_count;
        return batch_count == throw_at + 1;
    };

    EXPECT_THROW(
      do_compact_with_throwing_sink(
        tidp,
        ntp,
        std::move(compaction_info->offsets_response),
        compaction_info->compaction_epoch,
        compaction_info->start_offset,
        &_metastore,
        &_io,
        std::move(no_roll),
        std::move(should_throw))
        .get(),
      std::runtime_error);

    // Verify that each object's physical data matches its metadata. If the
    // inflight object were flushed instead of discarded, the new object
    // would contain batches beyond what the metadata records (e.g., metadata
    // says [0,9] but the object physically has data up to offset 15).
    auto extents_res = _metastore
                         .get_extent_metadata_forwards(
                           tidp,
                           kafka::offset{0},
                           kafka::offset::max(),
                           /*max_num_extents=*/100,
                           l1::metastore::include_object_metadata::yes)
                         .get();
    ASSERT_TRUE(extents_res.has_value());
    for (const auto& extent : extents_res->extents) {
        ASSERT_TRUE(extent.object_info.has_value());
        auto obj = _io.get_object(extent.object_info->oid);
        ASSERT_TRUE(obj.has_value());
        auto rdr = l1::object_reader::create(
          make_iobuf_input_stream(std::move(obj.value())));
        auto close_rdr = ss::defer([&rdr] { rdr->close().get(); });

        kafka::offset physical_last_offset{};
        while (true) {
            auto res = rdr->read_next().get();
            if (std::holds_alternative<model::record_batch>(res)) {
                auto& batch = std::get<model::record_batch>(res);
                physical_last_offset = model::offset_cast(batch.last_offset());
            }
            if (std::holds_alternative<l1::object_reader::eof>(res)) {
                break;
            }
        }

        EXPECT_EQ(physical_last_offset, extent.last_offset)
          << "Object " << extent.object_info->oid
          << " physical last offset does not match metadata last offset: "
          << physical_last_offset << " != " << extent.last_offset;
    }
}
