/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

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
#include "model/batch_compression.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "storage/tests/batch_generators.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <numeric>
#include <variant>

using namespace cloud_topics;

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

class ReducerTestFixture : public l1::l1_reader_fixture {
public:
    l1::compaction_committer::updates_t
    take_updates_from_committer(l1::compaction_committer& committer) {
        auto updates = std::exchange(committer._updates, {});
        return updates;
    }
};

class never_commit : public l1::committing_policy {
public:
    update_response on_update(const l1::object_output_t&) final {
        return update_response::wait;
    }

    bool should_commit() const final { return false; }
};

TEST_F(ReducerTestFixture, Reducer) {
    l1::simple_metastore m;
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
    make_l1_objects(tidp_batches);

    ss::abort_source as;

    auto state = l1::compaction_job_state::running;
    auto map = compaction::simple_key_offset_map();
    auto sample_spec = l1::metastore::compaction_sample_spec{
      .tidp = tidp,
      .tombstone_removal_upper_bound_ts = model::timestamp::max()};
    auto compaction_info = _metastore.get_compaction_info(sample_spec).get();

    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 1.0);
    ASSERT_TRUE(compaction_info->offsets_response.dirty_ranges.covers(
      start_offset, last_offset));

    auto committer = l1::compaction_committer(
      std::make_unique<never_commit>(), &_metastore, &_io);
    auto committer_stop = ss::defer([&committer] { committer.stop().get(); });
    auto src = std::make_unique<l1::compaction_source>(
      ntp,
      tidp,
      compaction_info->offsets_response,
      &map,
      &_metastore,
      &_io,
      as,
      state);
    auto sink = std::make_unique<l1::compaction_sink>(&_io, &committer, tidp);
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    std::move(reducer).run().get();

    auto updates = take_updates_from_committer(committer);
    ASSERT_EQ(updates.size(), 1);

    auto& update = updates.front();
    ASSERT_EQ(update.ntp_md.tidp, tidp);
    ASSERT_EQ(update.ntp_md.base_offset, start_offset);
    ASSERT_EQ(update.ntp_md.last_offset, last_offset);
    ASSERT_GE(update.ntp_md.max_timestamp, ts);

    auto remove_file = ss::defer(
      [&update] { update.staging_file->remove().get(); });

    auto size = update.staging_file->size().get();
    ASSERT_GT(size, 0);

    auto oid = l1::create_object_id();
    ASSERT_TRUE(_io.put_object(oid, update.staging_file.get(), &as).get());
    auto object_stream
      = _io
          .read_object(
            l1::object_extent{.id = oid, .position = 0, .size = size}, &as)
          .get();
    ASSERT_TRUE(object_stream.has_value());

    auto rdr = l1::object_reader::create(std::move(object_stream.value()));
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
