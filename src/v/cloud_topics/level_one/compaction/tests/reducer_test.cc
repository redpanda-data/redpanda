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
#include "cloud_topics/level_one/common/object_utils.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/sink.h"
#include "cloud_topics/level_one/compaction/source.h"
#include "cloud_topics/level_one/compaction/tests/in_memory_sink.h"
#include "cloud_topics/level_one/compaction/tests/throwing_compaction_sink.h"
#include "cloud_topics/level_one/compaction/worker_probe.h"
#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
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
#include "ssx/when_all.h"
#include "storage/tests/batch_generators.h"
#include "test_utils/async.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <limits>
#include <numeric>
#include <variant>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace {

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
  kafka::offset max_compactible_offset = kafka::offset::max()) {
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
      nullptr);
    auto sink = std::make_unique<l1::compaction_sink>(
      tidp,
      dirty_range_intervals,
      offsets_response.removable_tombstone_ranges,
      expected_compaction_epoch,
      start_offset,
      io,
      metastore,
      as,
      config::mock_binding<size_t>(128_MiB),
      16_MiB);
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
  kafka::offset max_compactible_offset = kafka::offset::max()) {
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
      nullptr);
    // Use a very large max_object_size to disable size-based rolls; the
    // throwing_compaction_sink's should_roll predicate controls rolling.
    auto inner_sink = std::make_unique<l1::compaction_sink>(
      tidp,
      dirty_range_intervals,
      offsets_response.removable_tombstone_ranges,
      expected_compaction_epoch,
      start_offset,
      io,
      metastore,
      as,
      config::mock_binding<size_t>(128_MiB),
      16_MiB);
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
