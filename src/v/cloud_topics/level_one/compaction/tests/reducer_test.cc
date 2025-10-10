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
#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/committing_policy.h"
#include "cloud_topics/level_one/compaction/sink.h"
#include "cloud_topics/level_one/compaction/source.h"
#include "cloud_topics/level_one/compaction/tests/in_memory_sink.h"
#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"
#include "compaction/tests/simple_reducer.h"
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

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <numeric>
#include <variant>

using namespace cloud_topics;

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
  l1::compaction_committer& committer,
  l1::metastore* metastore,
  l1::io* io) {
    ss::abort_source as;
    auto state = l1::compaction_job_state::running;
    auto map = compaction::simple_key_offset_map();
    auto dirty_range_intervals = offsets_response.dirty_ranges.to_vec();
    auto src = std::make_unique<l1::compaction_source>(
      ntp,
      tidp,
      dirty_range_intervals,
      offsets_response.removable_tombstone_ranges,
      std::move(offsets_response.extents),
      &map,
      metastore,
      io,
      as,
      state);
    auto sink = std::make_unique<l1::compaction_sink>(
      tidp,
      dirty_range_intervals,
      offsets_response.removable_tombstone_ranges,
      &map,
      io,
      &committer);
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    co_await std::move(reducer).run();
}

chunked_circular_buffer<model::record_batch>
read_batches_from_l1_stream(ss::input_stream<char> object_stream) {
    auto rdr = l1::object_reader::create(std::move(object_stream));
    auto close_rdr = ss::defer([&rdr] { rdr->close().get(); });

    chunked_circular_buffer<model::record_batch> output_batches;
    while (true) {
        l1::object_reader::result res = rdr->read_next().get();
        if (std::holds_alternative<model::record_batch>(res)) {
            auto b = std::move(std::get<model::record_batch>(res));
            if (b.compressed()) {
                b = model::decompress_batch(b).get();
            }
            output_batches.push_back(std::move(b));
        }
        if (std::holds_alternative<l1::object_reader::eof>(res)) {
            break;
        }
    }
    return output_batches;
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
    l1::compaction_committer::updates_t
    take_updates_from_committer(l1::compaction_committer& committer) {
        auto updates = std::exchange(committer._updates, {});
        return updates;
    }

    ss::future<> commit_updates(
      l1::compaction_committer& committer,
      l1::compaction_committer::updates_t updates) {
        co_await committer.commit_some(std::move(updates));
    }
};

class never_commit : public l1::committing_policy {
public:
    update_response on_update(const l1::object_output_t&) final {
        return update_response::wait;
    }

    bool should_commit() const final { return false; }
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

    auto state = l1::compaction_job_state::running;
    auto map = compaction::simple_key_offset_map();
    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(info_spec).get();

    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 1.0);
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
      start_offset, last_offset));

    auto committer = l1::compaction_committer(
      std::make_unique<never_commit>(), &_io, &_metastore);
    auto committer_stop = ss::defer([&committer] { committer.stop().get(); });
    auto dirty_range_intervals
      = compaction_info->offsets_response.dirty_ranges.to_vec();
    auto src = std::make_unique<l1::compaction_source>(
      ntp,
      tidp,
      dirty_range_intervals,
      compaction_info->offsets_response.removable_tombstone_ranges,
      std::move(compaction_info->offsets_response.extents),
      &map,
      &_metastore,
      &_io,
      as,
      state);
    auto sink = std::make_unique<l1::compaction_sink>(
      tidp,
      dirty_range_intervals,
      compaction_info->offsets_response.removable_tombstone_ranges,
      &map,
      &_io,
      &committer);
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    std::move(reducer).run().get();

    auto updates = take_updates_from_committer(committer);
    ASSERT_EQ(updates.size(), 1);

    auto& update = updates.front();
    auto& file_and_md_infos = update.staging_files_and_md_infos;
    ASSERT_EQ(file_and_md_infos.size(), 1);
    auto& file_and_md_info = file_and_md_infos.front();
    auto& ntp_md = file_and_md_info.ntp_md;
    ASSERT_EQ(ntp_md.tidp, tidp);
    ASSERT_EQ(ntp_md.base_offset, start_offset);
    ASSERT_EQ(ntp_md.last_offset, last_offset);
    ASSERT_GE(ntp_md.max_timestamp, ts);

    auto remove_file = ss::defer(
      [&file_and_md_info] { file_and_md_info.staging_file->remove().get(); });

    auto size = file_and_md_info.staging_file->size().get();
    ASSERT_GT(size, 0);

    auto oid = l1::create_object_id();
    ASSERT_TRUE(
      _io.put_object(oid, file_and_md_info.staging_file.get(), &as).get());
    auto object_stream
      = _io
          .read_object(
            l1::object_extent{.id = oid, .position = 0, .size = size}, &as)
          .get();
    ASSERT_TRUE(object_stream.has_value());

    auto output_batches = read_batches_from_l1_stream(
      std::move(object_stream.value()));

    ASSERT_EQ(output_batches.size(), num_batches);
    int output_num_records = std::accumulate(
      output_batches.begin(),
      output_batches.end(),
      int{0},
      [](int acc, model::record_batch& b) { return acc + b.record_count(); });
    ASSERT_EQ(output_num_records, num_batches);
    linear_int_kv_batch_generator::validate_post_compaction(
      std::move(output_batches));

    // TODO: once we hook up the committer to the metastore, it would be nice to
    // assert on metastore state as well.
}

TEST_F(ReducerTestFixture, TombstoneReducer) {
    ss::abort_source as;
    auto [ntp, tidp] = make_ntidp("test_topic");
    int num_batches = 10;
    int records_per_batch = 150;
    int cardinality = 100;
    latest_kv_map_t latest_kv_map;
    auto ts = model::timestamp::now();
    auto batches = generate_batches(
      num_batches, cardinality, records_per_batch, 0, true, &latest_kv_map);
    kafka::offset start_offset{0};
    kafka::offset last_offset{num_batches * records_per_batch - 1};
    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    auto committer = l1::compaction_committer(
      std::make_unique<never_commit>(), &_io, &_metastore);
    auto committer_stop = ss::defer([&committer] { committer.stop().get(); });

    auto info_spec = l1::metastore::compaction_info_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};

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
          committer,
          &_metastore,
          &_io)
          .get();
    }

    auto updates = take_updates_from_committer(committer);
    ASSERT_EQ(updates.size(), 1);

    {
        auto& update = updates.front();
        auto& file_and_md_infos = update.staging_files_and_md_infos;
        ASSERT_EQ(file_and_md_infos.size(), 1);
        auto& file_and_md_info = file_and_md_infos.front();
        auto& ntp_md = file_and_md_info.ntp_md;
        ASSERT_EQ(update.tidp, tidp);
        ASSERT_EQ(ntp_md.tidp, tidp);
        ASSERT_EQ(ntp_md.base_offset, start_offset);
        ASSERT_TRUE(!update.compact_update.new_cleaned_ranges.empty());
        ASSERT_EQ(
          update.compact_update.new_cleaned_ranges.front().base_offset,
          start_offset);
        ASSERT_EQ(
          update.compact_update.new_cleaned_ranges.front().last_offset,
          last_offset);
        ASSERT_TRUE(
          update.compact_update.new_cleaned_ranges.front().has_tombstones);
        ASSERT_EQ(ntp_md.last_offset, last_offset);
        ASSERT_GE(ntp_md.max_timestamp, ts);
        // Even though we removed tombstones, they were removed because of
        // deduplication rather than expiration.
        ASSERT_TRUE(update.compact_update.removed_tombstones_ranges.empty());

        auto size = file_and_md_info.staging_file->size().get();
        ASSERT_GT(size, 0);

        auto oid = l1::create_object_id();
        ASSERT_TRUE(
          _io.put_object(oid, file_and_md_info.staging_file.get(), &as).get());
        auto object_stream
          = _io
              .read_object(
                l1::object_extent{.id = oid, .position = 0, .size = size}, &as)
              .get();
        ASSERT_TRUE(object_stream.has_value());

        auto output_batches = read_batches_from_l1_stream(
          std::move(object_stream.value()));

        ASSERT_EQ(output_batches.size(), 1);
        int output_num_records = std::accumulate(
          output_batches.begin(),
          output_batches.end(),
          int{0},
          [](int acc, model::record_batch& b) {
              return acc + b.record_count();
          });
        ASSERT_EQ(output_num_records, cardinality);

        for (auto& batch : output_batches) {
            if (batch.compressed()) {
                batch = model::decompress_batch(batch).get();
            }
            batch.for_each_record([&latest_kv_map](model::record rec) {
                auto key = iobuf_to_string(rec.release_key());
                std::optional<ss::sstring> val;
                if (rec.has_value()) {
                    auto val = iobuf_to_string(rec.release_value());
                }
                EXPECT_TRUE(latest_kv_map.contains(key));
                EXPECT_EQ(val, latest_kv_map[key]);
            });
        }
    }

    commit_updates(committer, std::move(updates)).get();

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
          committer,
          &_metastore,
          &_io)
          .get();
    }

    updates = take_updates_from_committer(committer);
    ASSERT_EQ(updates.size(), 1);

    {
        auto& update = updates.front();
        auto& file_and_md_infos = update.staging_files_and_md_infos;
        ASSERT_EQ(file_and_md_infos.size(), 1);
        auto& file_and_md_info = file_and_md_infos.front();
        auto& ntp_md = file_and_md_info.ntp_md;
        ASSERT_EQ(update.tidp, tidp);
        ASSERT_EQ(ntp_md.tidp, tidp);
        ASSERT_EQ(ntp_md.base_offset, start_offset);
        ASSERT_TRUE(update.compact_update.new_cleaned_ranges.empty());
        ASSERT_EQ(ntp_md.last_offset, last_offset);
        ASSERT_EQ(ntp_md.max_timestamp, model::timestamp::missing());
        ASSERT_FALSE(update.compact_update.removed_tombstones_ranges.empty());
    }

    commit_updates(committer, std::move(updates)).get();

    {
        auto compaction_info = _metastore.get_compaction_info(info_spec).get();
        ASSERT_TRUE(compaction_info.has_value());
        ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.0);
        ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.empty());
        // All tombstones should have been removed.
        ASSERT_TRUE(
          compaction_info->offsets_response.removable_tombstone_ranges.empty());

        do_compact(
          tidp,
          ntp,
          std::move(compaction_info->offsets_response),
          committer,
          &_metastore,
          &_io)
          .get();
    }

    updates = take_updates_from_committer(committer);
    ASSERT_EQ(updates.size(), 0);
}
