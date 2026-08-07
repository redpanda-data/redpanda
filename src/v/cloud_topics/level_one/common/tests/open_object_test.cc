/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_io/admission_control_types.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_handle.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/common/open_object.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "storage/record_batch_utils.h"
#include "test_utils/scoped_config.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

using namespace cloud_topics::l1;

namespace {

kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}

model::offset_delta operator""_od(unsigned long long d) {
    return model::offset_delta{static_cast<int64_t>(d)};
}

model::record_batch
make_batch(kafka::offset base, kafka::offset last, model::timestamp ts = {}) {
    int count = static_cast<int>(last - base) + 1;
    std::vector<size_t> record_sizes(count, 100);
    return model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = kafka::offset_cast(base),
        .count = count,
        .record_sizes = record_sizes,
        .timestamp = ts,
        .all_records_have_same_timestamp = true,
      });
}

/// Builds a 2-batch L1 object, stores it in `fio`, and returns the object info
/// along with the generated object_id.
struct test_object {
    object_id oid;
    object_builder::object_info info;
    model::topic_id_partition tidp;
};

test_object make_and_store(fake_io& fio) {
    auto tid = model::topic_id(uuid_t::create());
    auto tidp = model::topic_id_partition{tid, model::partition_id{0}};
    auto oid = create_object_id();

    iobuf buf;
    auto builder = object_builder::create(
      make_iobuf_ref_output_stream(buf), {.indexing_interval = 1});
    auto _ = ss::defer([&builder] { builder->close().get(); });
    builder->start_partition(tidp).get();
    builder->add_batch(make_batch(0_o, 9_o)).get();
    builder->add_batch(make_batch(10_o, 19_o)).get();
    auto info = builder->finish().get();

    fio.put_object(oid, std::move(buf));
    return {.oid = oid, .info = std::move(info), .tidp = tidp};
}

} // namespace

// Build an L1 object with two batches (all batches indexed), store it in
// fake_io, then open it and seek to the second batch. Verify that the seek
// returns a non-zero file_position (the footer index was used) and no Kafka
// offset (native object, not an imported TS segment).
TEST(OpenObjectTest, SeekReturnsNonzeroPosition) {
    fake_io fio;
    auto [oid, info, tidp] = make_and_store(fio);

    ss::abort_source as;
    object_extent extent{
      .id = oid,
      .position = info.footer_offset,
      .size = info.size_bytes - info.footer_offset,
      .imported = std::nullopt,
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    // Seek to offset 10 — the start of the second batch.
    auto seek = handle->index().seek_offset_le(tidp, 10_o);
    ASSERT_TRUE(seek.has_value());
    EXPECT_GT(seek->file_position, size_t{0});
    EXPECT_FALSE(seek->delta.has_value());
}

// After seeking to the second batch, open a reader and verify that only batches
// at or after offset 10 are returned, and that at least one batch is returned.
TEST(OpenObjectTest, ReadReturnsBatchesAtOrAfterTarget) {
    fake_io fio;
    auto [oid, info, tidp] = make_and_store(fio);

    ss::abort_source as;
    object_extent extent{
      .id = oid,
      .position = info.footer_offset,
      .size = info.size_bytes - info.footer_offset,
      .imported = std::nullopt,
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_offset_le(tidp, 10_o);
    ASSERT_TRUE(seek.has_value());

    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    int batch_count = 0;
    while (true) {
        auto item = reader->read_next().get();
        if (std::holds_alternative<object_reader::eof>(item)) {
            break;
        }
        if (!std::holds_alternative<model::record_batch>(item)) {
            continue;
        }
        const auto& batch = std::get<model::record_batch>(item);
        EXPECT_GE(batch.base_offset(), kafka::offset_cast(10_o))
          << "batch starts before seek target";
        ++batch_count;
    }
    EXPECT_GT(batch_count, 0) << "no batches returned after seek";
}

// Seeking with a topic_id_partition not present in the object must return
// nullopt.
TEST(OpenObjectTest, SeekUnknownTidpReturnsNullopt) {
    fake_io fio;
    auto [oid, info, tidp] = make_and_store(fio);

    ss::abort_source as;
    object_extent extent{
      .id = oid,
      .position = info.footer_offset,
      .size = info.size_bytes - info.footer_offset,
      .imported = std::nullopt,
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto unknown_tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    EXPECT_FALSE(handle->index().seek_offset_le(unknown_tidp, 0_o).has_value());
    EXPECT_FALSE(handle->index()
                   .seek_timestamp_le(unknown_tidp, model::timestamp{0})
                   .has_value());
}

// Calling open_object with an object_id not present in fake_io must return
// cloud_missing_object.
TEST(OpenObjectTest, MissingObjectReturnsError) {
    fake_io fio;
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = 100,
      .imported = std::nullopt,
    };
    auto result = open_object(
                    fio, extent, &as, cloud_io::group_id::default_group, false)
                    .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), io::errc::cloud_missing_object);
}

// ── Group C: fake_io imported (TS segment) path ──────────────────────────────

namespace {

// Serialise a batch into the on-disk storage format used by TS segments:
// packed header followed by raw records.
iobuf batch_to_disk_iobuf(model::record_batch batch) {
    iobuf out;
    out.append(storage::batch_header_to_disk_iobuf(batch.header()));
    out.append(std::move(batch).release_data());
    return out;
}

// Build a TS segment (on-disk format) from a single batch.
iobuf make_ts_segment(model::record_batch batch) {
    return batch_to_disk_iobuf(std::move(batch));
}

// Build a batch of a given type starting at a given LOG offset.
model::record_batch
make_log_batch(model::offset base, int count, model::record_batch_type bt) {
    std::vector<size_t> record_sizes(static_cast<size_t>(count), 100);
    return model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = base,
        .count = count,
        .bt = bt,
        .record_sizes = record_sizes,
      });
}

// Build a transaction control batch (raft_data + control attr) at a given
// LOG offset.
model::record_batch make_control_batch(model::offset base, int count) {
    std::vector<size_t> record_sizes(static_cast<size_t>(count), 100);
    return model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = base,
        .count = count,
        .bt = model::record_batch_type::raft_data,
        .is_control = true,
        .record_sizes = record_sizes,
      });
}

// Build a transactional raft_data batch at a given LOG offset, with the given
// producer identity (so it can be matched against an aborted tx range).
model::record_batch make_txn_log_batch(
  model::offset base, int count, int64_t producer_id, int16_t producer_epoch) {
    std::vector<size_t> record_sizes(static_cast<size_t>(count), 100);
    return model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = base,
        .count = count,
        .bt = model::record_batch_type::raft_data,
        .producer_id = producer_id,
        .producer_epoch = producer_epoch,
        .is_transactional = true,
        .record_sizes = record_sizes,
      });
}

// Concatenate batches into a single TS segment (on-disk format).
iobuf make_ts_segment_multi(std::vector<model::record_batch> batches) {
    iobuf out;
    for (auto& b : batches) {
        out.append(batch_to_disk_iobuf(std::move(b)));
    }
    return out;
}

} // namespace

// Fixture for the imported (TS segment) tests. Forces a small chunk size so
// reads through the ts_object_handle exercise the chunked data source for real
// -- multi-chunk reads and batches spanning chunk boundaries -- rather than a
// single whole-suffix download. open_object takes the chunk size from config,
// so it is set here rather than baked into fake_io.
class OpenObjectTsTest : public ::testing::Test {
protected:
    OpenObjectTsTest() {
        _cfg.get("cloud_storage_disable_chunk_reads").set_value(false);
        _cfg.get("cloud_storage_cache_chunk_size").set_value(size_t{64});
    }

    scoped_config _cfg;
};

// With no injected .index, open_object builds an empty ts_segment_index (as
// file_io does when the segment's .index is absent): seek_offset_le falls back
// to {file_position=0, length=segment_size, delta=delta_base} -- a
// full-segment scan with the segment's authoritative base delta.
TEST_F(OpenObjectTsTest, SeekReturnsFullSegment) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-1-v1.log"};

    auto batch = make_batch(5_o, 14_o);
    auto segment = make_ts_segment(batch.copy());
    size_t segment_size = segment.size_bytes();

    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = segment_size,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 5_od},
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_offset_le(tidp, 5_o);
    ASSERT_TRUE(seek.has_value());
    EXPECT_EQ(seek->file_position, size_t{0});
    EXPECT_EQ(seek->length, segment_size);
    // No index entry: the seek reports the segment's authoritative base delta,
    // which the reader applies directly.
    ASSERT_TRUE(seek->delta.has_value());
    EXPECT_EQ(*seek->delta, 5_od);
}

// open_reader on an imported handle reads batches with delta-translated kafka
// offsets. Log offset 15..19 with delta=5 must yield kafka offset 10..14.
TEST_F(OpenObjectTsTest, ReadBatchesWithDeltaTranslation) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-2-v1.log"};

    // Log offsets 15..19 (kafka 10..14: base kafka 10, so derived delta=5)
    auto batch = make_batch(15_o, 19_o);
    auto segment = make_ts_segment(batch.copy());
    size_t sz = segment.size_bytes();
    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 5_od},
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_offset_le(tidp, 10_o);
    ASSERT_TRUE(seek.has_value());

    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    auto item = reader->read_next().get();
    ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
    const auto& got = std::get<model::record_batch>(item);
    EXPECT_EQ(got.base_offset(), kafka::offset_cast(10_o));
    EXPECT_EQ(got.last_offset(), kafka::offset_cast(14_o));
}

// With an injected .index, open_object seeks through the real ts_segment_index
// (not the full-segment fallback): seek_offset_le finds the byte position of
// the batch containing the target via offset_index::find_kaf_offset, derives
// the Kafka offset of the indexed entry, and the reader resumes at that
// position. Three raft_data batches, delta 5 (log = kafka + 5):
//   batch0 log [5,9]   kafka [0,4]   file_pos 0
//   batch1 log [10,14] kafka [5,9]   file_pos p1
//   batch2 log [15,19] kafka [10,14] file_pos p2
TEST_F(OpenObjectTsTest, IndexBackedSeekFindsBatchPosition) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-4-v1.log"};

    auto d0 = batch_to_disk_iobuf(
      make_log_batch(model::offset{5}, 5, model::record_batch_type::raft_data));
    auto d1 = batch_to_disk_iobuf(make_log_batch(
      model::offset{10}, 5, model::record_batch_type::raft_data));
    auto d2 = batch_to_disk_iobuf(make_log_batch(
      model::offset{15}, 5, model::record_batch_type::raft_data));
    const auto p1 = static_cast<int64_t>(d0.size_bytes());
    const auto p2 = p1 + static_cast<int64_t>(d1.size_bytes());
    iobuf segment;
    segment.append(std::move(d0));
    segment.append(std::move(d1));
    segment.append(std::move(d2));
    const size_t sz = segment.size_bytes();

    // Build the segment's offset_index (rp=log, kaf=kafka, one entry per batch)
    // and serialize it as file_io downloads it.
    cloud_storage::offset_index oi(
      model::offset{5},
      kafka::offset{0},
      0,
      cloud_storage::remote_segment_sampling_step_bytes,
      model::timestamp::missing());
    oi.add(model::offset{5}, kafka::offset{0}, 0, model::timestamp{1});
    oi.add(model::offset{10}, kafka::offset{5}, p1, model::timestamp{2});
    oi.add(model::offset{15}, kafka::offset{10}, p2, model::timestamp{3});

    fio.put_ts_segment(ts_path, std::move(segment), {}, oi.to_iobuf());

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 5_od},
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    // Seek to a kafka offset inside batch1 -> its (non-zero) byte position,
    // reporting the offset delta (log 10 - kafka 5 = 5) at that indexed entry.
    auto seek = handle->index().seek_offset_le(tidp, 5_o);
    ASSERT_TRUE(seek.has_value());
    EXPECT_EQ(seek->file_position, static_cast<size_t>(p1));
    ASSERT_TRUE(seek->delta.has_value());
    EXPECT_EQ(*seek->delta, 5_od);

    // Batch boundaries either side: first batch at 0, third batch at p2.
    auto seek0 = handle->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek0.has_value());
    EXPECT_EQ(seek0->file_position, size_t{0});
    auto seek2 = handle->index().seek_offset_le(tidp, 10_o);
    ASSERT_TRUE(seek2.has_value());
    EXPECT_EQ(seek2->file_position, static_cast<size_t>(p2));

    // Reading from the indexed position yields batch1 first (kafka [5,9]).
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    auto item = reader->read_next().get();
    ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
    const auto& got = std::get<model::record_batch>(item);
    EXPECT_EQ(got.base_offset(), kafka::offset_cast(5_o));
    EXPECT_EQ(got.last_offset(), kafka::offset_cast(9_o));
}

// A TS segment is a raw redpanda log: besides raft_data it can hold
// offset-translator batches (e.g. raft_configuration) and other non-data
// batches (e.g. tx_fence). The reader must surface ONLY raft_data, advancing
// the running delta for offset-translator batches by their offset span and
// dropping everything else without moving the delta. Layout (log offsets):
//   raft_configuration @ [0,1]  delta 0 -> 2   (translator: advances delta)
//   tx_fence           @ [2]    dropped, delta stays 2 (leaves a kafka gap)
//   raft_data          @ [3,5]  kafka [1,3]    (3 - delta 2 .. 5 - delta 2)
TEST_F(OpenObjectTsTest, ReadImportedExtentEmitsOnlyRaftData) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-3-v1.log"};

    std::vector<model::record_batch> batches;
    batches.push_back(make_log_batch(
      model::offset{0}, 2, model::record_batch_type::raft_configuration));
    batches.push_back(
      make_log_batch(model::offset{2}, 1, model::record_batch_type::tx_fence));
    batches.push_back(
      make_log_batch(model::offset{3}, 3, model::record_batch_type::raft_data));
    auto segment = make_ts_segment_multi(std::move(batches));
    size_t sz = segment.size_bytes();
    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 0_od},
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());

    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    std::vector<model::record_batch> got;
    while (true) {
        auto item = reader->read_next().get();
        if (std::holds_alternative<object_reader::eof>(item)) {
            break;
        }
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
        got.push_back(std::move(std::get<model::record_batch>(item)));
    }

    ASSERT_EQ(got.size(), 1u) << "only the raft_data batch should surface";
    EXPECT_EQ(got[0].header().type, model::record_batch_type::raft_data);
    EXPECT_EQ(got[0].base_offset(), kafka::offset_cast(1_o));
    EXPECT_EQ(got[0].last_offset(), kafka::offset_cast(3_o));
}

// Transaction control batches (commit/abort markers) are raft_data with the
// control attribute set. They must never be surfaced to Kafka clients (native
// CT L1 strips them at reconciliation). A control batch still consumes a Kafka
// offset, leaving a gap. Layout (log offsets, delta 0):
//   raft_data         @ [0,2]  -> kafka [0,2]
//   control           @ [3]    -> dropped (kafka gap at 3)
//   raft_data         @ [4,5]  -> kafka [4,5]
TEST_F(OpenObjectTsTest, ReadImportedExtentDropsControlBatches) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-4-v1.log"};

    std::vector<model::record_batch> batches;
    batches.push_back(
      make_log_batch(model::offset{0}, 3, model::record_batch_type::raft_data));
    batches.push_back(make_control_batch(model::offset{3}, 1));
    batches.push_back(
      make_log_batch(model::offset{4}, 2, model::record_batch_type::raft_data));
    auto segment = make_ts_segment_multi(std::move(batches));
    size_t sz = segment.size_bytes();
    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 0_od},
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    std::vector<model::record_batch> got;
    while (true) {
        auto item = reader->read_next().get();
        if (std::holds_alternative<object_reader::eof>(item)) {
            break;
        }
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
        got.push_back(std::move(std::get<model::record_batch>(item)));
    }

    ASSERT_EQ(got.size(), 2u) << "control batch must not surface";
    for (const auto& b : got) {
        EXPECT_FALSE(b.header().attrs.is_control());
    }
    EXPECT_EQ(got[0].base_offset(), kafka::offset_cast(0_o));
    EXPECT_EQ(got[0].last_offset(), kafka::offset_cast(2_o));
    EXPECT_EQ(got[1].base_offset(), kafka::offset_cast(4_o));
    EXPECT_EQ(got[1].last_offset(), kafka::offset_cast(5_o));
}

// Aborted-transaction data must be stripped so the imported region is
// committed-only (like native CT L1). Committed transactional data survives.
// Layout (log offsets, delta 0), all transactional:
//   pid 99 @ [0,2]  committed -> kafka [0,2]
//   pid 42 @ [3,4]  ABORTED   -> dropped (kafka gap at 3,4)
//   pid 99 @ [5,5]  committed -> kafka [5,5]
// Aborted range: {pid 42, [3,4]}.
TEST_F(OpenObjectTsTest, ReadImportedExtentStripsAbortedData) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-5-v1.log"};

    std::vector<model::record_batch> batches;
    batches.push_back(make_txn_log_batch(model::offset{0}, 3, 99, 0));
    batches.push_back(make_txn_log_batch(model::offset{3}, 2, 42, 0));
    batches.push_back(make_txn_log_batch(model::offset{5}, 1, 99, 0));
    auto segment = make_ts_segment_multi(std::move(batches));
    size_t sz = segment.size_bytes();

    aborted_transactions aborted;
    aborted.insert(
      model::tx_range{
        model::producer_identity{42, 0}, model::offset{3}, model::offset{4}});
    fio.put_ts_segment(ts_path, std::move(segment), std::move(aborted));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 0_od},
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    std::vector<model::record_batch> got;
    while (true) {
        auto item = reader->read_next().get();
        if (std::holds_alternative<object_reader::eof>(item)) {
            break;
        }
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
        got.push_back(std::move(std::get<model::record_batch>(item)));
    }

    ASSERT_EQ(got.size(), 2u) << "aborted batch must be stripped";
    EXPECT_EQ(got[0].base_offset(), kafka::offset_cast(0_o));
    EXPECT_EQ(got[0].last_offset(), kafka::offset_cast(2_o));
    EXPECT_EQ(got[1].base_offset(), kafka::offset_cast(5_o));
    EXPECT_EQ(got[1].last_offset(), kafka::offset_cast(5_o));
}

// An extent whose data is entirely aborted yields no batches.
TEST_F(OpenObjectTsTest, ReadImportedExtentAllAborted) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-6-v1.log"};

    auto segment = make_ts_segment(
      make_txn_log_batch(model::offset{0}, 5, 7, 0));
    size_t sz = segment.size_bytes();

    aborted_transactions aborted;
    aborted.insert(
      model::tx_range{
        model::producer_identity{7, 0}, model::offset{0}, model::offset{4}});
    fio.put_ts_segment(ts_path, std::move(segment), std::move(aborted));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 0_od},
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    auto item = reader->read_next().get();
    EXPECT_TRUE(std::holds_alternative<object_reader::eof>(item))
      << "all data aborted: reader must yield no batches";
}

// tx_state == absent means the .tx manifest is known empty (compacted or v3
// with metadata_size_hint == 0), so the read path must NOT consult any
// aborted-range source. Inject aborted ranges that WOULD strip a batch, set
// tx_state = absent, and assert the "aborted" batch still surfaces -- proving
// the decision was made without checking. (The same layout with the default
// unknown tx_state strips it, in ReadImportedExtentStripsAbortedData.)
TEST_F(OpenObjectTsTest, ReadImportedExtentTxAbsentSkipsAbortStripping) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-7-v1.log"};

    std::vector<model::record_batch> batches;
    batches.push_back(make_txn_log_batch(model::offset{0}, 3, 99, 0));
    batches.push_back(make_txn_log_batch(model::offset{3}, 2, 42, 0));
    batches.push_back(make_txn_log_batch(model::offset{5}, 1, 99, 0));
    auto segment = make_ts_segment_multi(std::move(batches));
    size_t sz = segment.size_bytes();

    // An aborted range that would strip batch[1] if it were consulted.
    aborted_transactions aborted;
    aborted.insert(
      model::tx_range{
        model::producer_identity{42, 0}, model::offset{3}, model::offset{4}});
    fio.put_ts_segment(ts_path, std::move(segment), std::move(aborted));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported
      = imported_ts_info{.ts_path = ts_path, .delta_base = 0_od, .tx_state = tx_manifest_state::absent},
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    std::vector<model::record_batch> got;
    while (true) {
        auto item = reader->read_next().get();
        if (std::holds_alternative<object_reader::eof>(item)) {
            break;
        }
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
        got.push_back(std::move(std::get<model::record_batch>(item)));
    }

    ASSERT_EQ(got.size(), 3u)
      << "tx_state=absent must skip abort-stripping: all batches surface";
    EXPECT_EQ(got[0].base_offset(), kafka::offset_cast(0_o));
    EXPECT_EQ(got[1].base_offset(), kafka::offset_cast(3_o));
    EXPECT_EQ(got[2].base_offset(), kafka::offset_cast(5_o));
}

// An imported extent whose ts_path was never injected has no data. open_object
// still builds a handle -- a missing .index is indistinguishable from an absent
// index and falls back to a full-segment scan, exactly as file_io does when a
// segment's .index is absent -- but reading the segment surfaces
// cloud_missing_object. Chunking is disabled here so the fetch failure is
// reported synchronously by open_reader rather than thrown mid-read.
TEST_F(OpenObjectTsTest, MissingTsSegmentReturnsErrorOnRead) {
    _cfg.get("cloud_storage_disable_chunk_reads").set_value(true);
    fake_io fio;
    ss::abort_source as;
    const ts_segment_path ts_path{"no-such-segment.log"};
    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = 100,
      .imported = imported_ts_info{.ts_path = ts_path},
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_FALSE(reader_result.has_value());
    EXPECT_EQ(reader_result.error(), io::errc::cloud_missing_object);
}

// ── Compaction: surviving batches sit at non-contiguous log offsets ──────────
//
// Compaction removes raft_data records from a segment, leaving the survivors at
// their original log offsets with gaps where records were dropped. The removed
// batches consumed Kafka offsets but are not offset-translator types, so the
// delta is unchanged across a hole. These three tests place the hole at the
// front, middle, and end of the segment. A compacted segment is modelled by
// simply omitting the removed batches while keeping the survivors at their real
// log offsets and setting delta_base to the segment's *declared* base delta
// (the pre-compaction base), which is what the source segment_meta records.

// Leading records removed: the first surviving batch sits past the declared
// base. The reader must seed its delta from the seek's delta_base, NOT infer it
// from the first surviving batch -- inferring would renumber the survivors down
// into the hole. Declared base log 10 / Kafka 0 (delta_base 10); log [10,12]
// compacted away. Surviving raft_data (log offsets):
//   [13,15] -> kafka [3,5]
//   [16,17] -> kafka [6,7]
TEST_F(OpenObjectTsTest, ReadImportedExtentCompactedFront) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-8-v1.log"};

    std::vector<model::record_batch> batches;
    batches.push_back(make_log_batch(
      model::offset{13}, 3, model::record_batch_type::raft_data));
    batches.push_back(make_log_batch(
      model::offset{16}, 2, model::record_batch_type::raft_data));
    auto segment = make_ts_segment_multi(std::move(batches));
    size_t sz = segment.size_bytes();
    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 10_od},
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    std::vector<model::record_batch> got;
    while (true) {
        auto item = reader->read_next().get();
        if (std::holds_alternative<object_reader::eof>(item)) {
            break;
        }
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
        got.push_back(std::move(std::get<model::record_batch>(item)));
    }

    ASSERT_EQ(got.size(), 2u);
    // Survivors keep their delta_base-translated offsets; they are NOT shifted
    // down to kafka 0 to fill the compacted hole.
    EXPECT_EQ(got[0].base_offset(), kafka::offset_cast(3_o));
    EXPECT_EQ(got[0].last_offset(), kafka::offset_cast(5_o));
    EXPECT_EQ(got[1].base_offset(), kafka::offset_cast(6_o));
    EXPECT_EQ(got[1].last_offset(), kafka::offset_cast(7_o));
}

// A batch removed from the middle leaves a Kafka gap; survivors on both sides
// translate by the same (unchanged) delta. Declared base log 4 / Kafka 0
// (delta_base 4):
//   [4,6]   -> kafka [0,2]
//   (log [7,9] compacted away -> Kafka gap 3..5)
//   [10,11] -> kafka [6,7]
TEST_F(OpenObjectTsTest, ReadImportedExtentCompactedMiddle) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-9-v1.log"};

    std::vector<model::record_batch> batches;
    batches.push_back(
      make_log_batch(model::offset{4}, 3, model::record_batch_type::raft_data));
    batches.push_back(make_log_batch(
      model::offset{10}, 2, model::record_batch_type::raft_data));
    auto segment = make_ts_segment_multi(std::move(batches));
    size_t sz = segment.size_bytes();
    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 4_od},
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    std::vector<model::record_batch> got;
    while (true) {
        auto item = reader->read_next().get();
        if (std::holds_alternative<object_reader::eof>(item)) {
            break;
        }
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
        got.push_back(std::move(std::get<model::record_batch>(item)));
    }

    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0].base_offset(), kafka::offset_cast(0_o));
    EXPECT_EQ(got[0].last_offset(), kafka::offset_cast(2_o));
    // Gap at kafka 3..5; the survivor after the hole is unshifted.
    EXPECT_EQ(got[1].base_offset(), kafka::offset_cast(6_o));
    EXPECT_EQ(got[1].last_offset(), kafka::offset_cast(7_o));
}

// Trailing records removed: the physical stream ends before the segment's
// declared last offset. The reader is not upper-bounded by the declared range;
// it emits the survivors and reports EOF at the end of the stream rather than
// expecting the missing tail. Declared base log 2 / Kafka 0 (delta_base 2):
//   [2,4] -> kafka [0,2]
//   [5,6] -> kafka [3,4]
//   (log [7,9] compacted away)
TEST_F(OpenObjectTsTest, ReadImportedExtentCompactedEnd) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-10-v1.log"};

    std::vector<model::record_batch> batches;
    batches.push_back(
      make_log_batch(model::offset{2}, 3, model::record_batch_type::raft_data));
    batches.push_back(
      make_log_batch(model::offset{5}, 2, model::record_batch_type::raft_data));
    auto segment = make_ts_segment_multi(std::move(batches));
    size_t sz = segment.size_bytes();
    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 2_od},
    };
    auto handle_result
      = open_object(fio, extent, &as, cloud_io::group_id::default_group, false)
          .get();
    ASSERT_TRUE(handle_result.has_value());
    auto& handle = *handle_result;

    auto seek = handle->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader_result = handle->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader_result.has_value());
    auto& reader = *reader_result;
    auto _r = ss::defer([&reader] { reader->close().get(); });

    std::vector<model::record_batch> got;
    while (true) {
        auto item = reader->read_next().get();
        if (std::holds_alternative<object_reader::eof>(item)) {
            break;
        }
        ASSERT_TRUE(std::holds_alternative<model::record_batch>(item));
        got.push_back(std::move(std::get<model::record_batch>(item)));
    }

    ASSERT_EQ(got.size(), 2u)
      << "reader must EOF at the stream end, not expect "
         "the compacted tail";
    EXPECT_EQ(got[0].base_offset(), kafka::offset_cast(0_o));
    EXPECT_EQ(got[0].last_offset(), kafka::offset_cast(2_o));
    EXPECT_EQ(got[1].base_offset(), kafka::offset_cast(3_o));
    EXPECT_EQ(got[1].last_offset(), kafka::offset_cast(4_o));
}

// ── Read chunking: open_object drives read_object per its (size, alignment,
// skip_cache) ────────────────────────────────────────────────────────────────
//
// Chunking lives in open_object (above the cache), not in read_object. fake_io
// records every read_object call so these tests assert the wiring per variant.
// The two chunk-size knobs are pinned to *different* values (cache 64,
// streaming 128) so a variant reading the wrong knob is caught.

namespace {

// A multi-batch raft_data TS segment large enough to span several 64- and
// 128-byte chunks.
iobuf make_multibatch_ts_segment() {
    std::vector<model::record_batch> batches;
    batches.push_back(
      make_log_batch(model::offset{0}, 2, model::record_batch_type::raft_data));
    batches.push_back(
      make_log_batch(model::offset{2}, 2, model::record_batch_type::raft_data));
    batches.push_back(
      make_log_batch(model::offset{4}, 2, model::record_batch_type::raft_data));
    return make_ts_segment_multi(std::move(batches));
}

// Drain a reader to EOF so every chunk it touches is fetched.
void drain_reader(object_reader& reader) {
    while (
      !std::holds_alternative<object_reader::eof>(reader.read_next().get())) {
    }
}

} // namespace

class OpenObjectChunkingTest : public ::testing::Test {
protected:
    OpenObjectChunkingTest() {
        _cfg.get("cloud_storage_disable_chunk_reads").set_value(false);
        _cfg.get("cloud_storage_cache_chunk_size").set_value(size_t{64});
        _cfg.get("cloud_topics_l1_streaming_read_chunk_size")
          .set_value(size_t{128});
    }
    scoped_config _cfg;
};

// ts/cache: chunk at the cache-chunk size (64), chunk-aligned, without
// bypassing the cache. Every read_object call is an imported, non-skip_cache
// read of <=64 bytes at a 64-aligned position, and the chunks cover the whole
// segment.
TEST_F(OpenObjectChunkingTest, TsCacheChunksAtCacheChunkSize) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-20-v1.log"};
    auto segment = make_multibatch_ts_segment();
    const size_t sz = segment.size_bytes();
    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 0_od},
    };
    auto handle = open_object(
                    fio,
                    extent,
                    &as,
                    cloud_io::group_id::default_group,
                    /*skip_cache=*/false)
                    .get();
    ASSERT_TRUE(handle.has_value());
    auto seek = (*handle)->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader = (*handle)->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader.has_value());
    auto _r = ss::defer([&reader] { (*reader)->close().get(); });
    drain_reader(**reader);

    const auto& calls = fio.read_object_calls();
    ASSERT_FALSE(calls.empty());
    size_t covered = 0;
    for (const auto& c : calls) {
        EXPECT_TRUE(c.imported);
        EXPECT_FALSE(c.skip_cache) << "ts/cache must not bypass the cache";
        EXPECT_EQ(c.position % 64, 0u)
          << "cache chunks are chunk-aligned to 64";
        EXPECT_LE(c.size, 64u) << "ts/cache reads the cache-chunk size";
        covered += c.size;
    }
    EXPECT_EQ(covered, sz) << "chunks cover the whole segment";
    EXPECT_EQ(calls.size(), (sz + 63) / 64);
}

// ts/nocache: chunk at the streaming size (128) and bypass the cache. Distinct
// size from ts/cache proves the variant reads its own knob.
TEST_F(OpenObjectChunkingTest, TsNocacheChunksAtStreamingSize) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-21-v1.log"};
    auto segment = make_multibatch_ts_segment();
    const size_t sz = segment.size_bytes();
    fio.put_ts_segment(ts_path, std::move(segment));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 0_od},
    };
    auto handle = open_object(
                    fio,
                    extent,
                    &as,
                    cloud_io::group_id::default_group,
                    /*skip_cache=*/true)
                    .get();
    ASSERT_TRUE(handle.has_value());
    auto seek = (*handle)->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader = (*handle)->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader.has_value());
    auto _r = ss::defer([&reader] { (*reader)->close().get(); });
    drain_reader(**reader);

    const auto& calls = fio.read_object_calls();
    ASSERT_FALSE(calls.empty());
    size_t covered = 0;
    for (const auto& c : calls) {
        EXPECT_TRUE(c.imported);
        EXPECT_TRUE(c.skip_cache) << "ts/nocache must bypass the cache";
        EXPECT_LE(c.size, 128u) << "ts/nocache reads the streaming size";
        covered += c.size;
    }
    EXPECT_EQ(covered, sz);
    EXPECT_EQ(calls.size(), (sz + 127) / 128);
}

// native/cache: not chunked. The footer read aside, the data is served by a
// single un-chunked read_object of the whole extent, without bypassing cache.
TEST_F(OpenObjectChunkingTest, NativeCacheNotChunked) {
    fake_io fio;
    auto [oid, info, tidp] = make_and_store(fio);
    ss::abort_source as;
    object_extent extent{
      .id = oid,
      .position = info.footer_offset,
      .size = info.size_bytes - info.footer_offset,
      .imported = std::nullopt,
    };
    auto handle = open_object(
                    fio,
                    extent,
                    &as,
                    cloud_io::group_id::default_group,
                    /*skip_cache=*/false)
                    .get();
    ASSERT_TRUE(handle.has_value());
    auto seek = (*handle)->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader = (*handle)->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader.has_value());
    auto _r = ss::defer([&reader] { (*reader)->close().get(); });
    drain_reader(**reader);

    // Data reads sit below the footer (the footer read is at footer_offset).
    size_t data_reads = 0;
    for (const auto& c : fio.read_object_calls()) {
        EXPECT_FALSE(c.imported);
        EXPECT_FALSE(c.skip_cache);
        if (c.position < info.footer_offset) {
            ++data_reads;
        }
    }
    EXPECT_EQ(data_reads, 1u)
      << "cached native read is a single un-chunked read";
}

// native/nocache: chunk at the streaming size (128) and bypass the cache.
TEST_F(OpenObjectChunkingTest, NativeNocacheChunksAtStreamingSize) {
    fake_io fio;
    auto [oid, info, tidp] = make_and_store(fio);
    ss::abort_source as;
    object_extent extent{
      .id = oid,
      .position = info.footer_offset,
      .size = info.size_bytes - info.footer_offset,
      .imported = std::nullopt,
    };
    auto handle = open_object(
                    fio,
                    extent,
                    &as,
                    cloud_io::group_id::default_group,
                    /*skip_cache=*/true)
                    .get();
    ASSERT_TRUE(handle.has_value());
    auto seek = (*handle)->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader = (*handle)->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader.has_value());
    auto _r = ss::defer([&reader] { (*reader)->close().get(); });
    drain_reader(**reader);

    size_t data_reads = 0;
    for (const auto& c : fio.read_object_calls()) {
        EXPECT_FALSE(c.imported);
        if (c.position < info.footer_offset) {
            EXPECT_TRUE(c.skip_cache) << "native/nocache must bypass the cache";
            EXPECT_LE(c.size, 128u)
              << "native/nocache reads the streaming size";
            ++data_reads;
        }
    }
    EXPECT_GT(data_reads, 1u)
      << "a multi-chunk extent is read in several chunks";
}

// A cancelled cache-bypassing read surfaces as an exception rather than a
// (truncated) clean end-of-stream: open_object wraps the read in a
// chunk_data_source, which checks the abort source on every chunk. The
// mechanism is unit-tested in chunk_data_source_test; this pins the wiring end
// to end so a cancelled maintenance read cannot be committed as if it had read
// the whole extent.
TEST_F(OpenObjectChunkingTest, NativeReadAbortSurfacesAsError) {
    fake_io fio;
    auto [oid, info, tidp] = make_and_store(fio);
    ss::abort_source as;
    object_extent extent{
      .id = oid,
      .position = info.footer_offset,
      .size = info.size_bytes - info.footer_offset,
      .imported = std::nullopt,
    };
    auto handle = open_object(
                    fio,
                    extent,
                    &as,
                    cloud_io::group_id::default_group,
                    /*skip_cache=*/true)
                    .get();
    ASSERT_TRUE(handle.has_value());
    auto seek = (*handle)->index().seek_offset_le(tidp, 0_o);
    ASSERT_TRUE(seek.has_value());
    auto reader = (*handle)->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader.has_value());
    auto _r = ss::defer([&reader] { (*reader)->close().get(); });

    // Read the first batch, then abort; draining the remainder (a multi-chunk
    // extent) must throw rather than terminate as a clean EOF.
    auto first = (*reader)->read_next().get();
    ASSERT_TRUE(std::holds_alternative<model::record_batch>(first));
    as.request_abort();
    EXPECT_THROW(
      {
          while (!std::holds_alternative<object_reader::eof>(
            (*reader)->read_next().get())) {
          }
      },
      ss::abort_requested_exception);
}

namespace {

// A 3-batch raft_data segment (delta 0) plus its offset_index, so an
// index-backed seek lands mid-segment. Returns the byte position of batch1
// (which is not a multiple of either chunk size, so chunk alignment is
// observable). Layout: batch0 log [0,1] kafka [0,1] at 0; batch1 log [2,3]
// kafka [2,3] at p1; batch2 log [4,5] kafka [4,5] at p2.
struct indexed_segment {
    iobuf segment;
    iobuf index;
    size_t p1;
};

indexed_segment make_indexed_segment() {
    auto d0 = batch_to_disk_iobuf(
      make_log_batch(model::offset{0}, 2, model::record_batch_type::raft_data));
    auto d1 = batch_to_disk_iobuf(
      make_log_batch(model::offset{2}, 2, model::record_batch_type::raft_data));
    auto d2 = batch_to_disk_iobuf(
      make_log_batch(model::offset{4}, 2, model::record_batch_type::raft_data));
    const auto p1 = d0.size_bytes();
    const auto p2 = p1 + d1.size_bytes();
    iobuf segment;
    segment.append(std::move(d0));
    segment.append(std::move(d1));
    segment.append(std::move(d2));

    cloud_storage::offset_index oi(
      model::offset{0},
      kafka::offset{0},
      0,
      cloud_storage::remote_segment_sampling_step_bytes,
      model::timestamp::missing());
    oi.add(model::offset{0}, kafka::offset{0}, 0, model::timestamp{1});
    oi.add(
      model::offset{2},
      kafka::offset{2},
      static_cast<int64_t>(p1),
      model::timestamp{2});
    oi.add(
      model::offset{4},
      kafka::offset{4},
      static_cast<int64_t>(p2),
      model::timestamp{3});
    return {std::move(segment), oi.to_iobuf(), p1};
}

} // namespace

// ts/cache alignment wiring: seeking to a mid-chunk byte position, the first
// read_object is the chunk aligned at or before it -- not the seek position
// itself -- so overlapping reads request identical (pos,size) chunks the cache
// can dedup.
TEST_F(OpenObjectChunkingTest, TsCacheAlignsFirstChunkToChunkBoundary) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-22-v1.log"};
    auto seg = make_indexed_segment();
    ASSERT_NE(seg.p1 % 64, 0u) << "batch1 must be mid-chunk for this to matter";
    const size_t sz = seg.segment.size_bytes();
    fio.put_ts_segment(
      ts_path, std::move(seg.segment), {}, std::move(seg.index));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 0_od},
    };
    auto handle = open_object(
                    fio,
                    extent,
                    &as,
                    cloud_io::group_id::default_group,
                    /*skip_cache=*/false)
                    .get();
    ASSERT_TRUE(handle.has_value());
    auto seek = (*handle)->index().seek_offset_le(tidp, 2_o);
    ASSERT_TRUE(seek.has_value());
    ASSERT_EQ(seek->file_position, seg.p1);
    auto reader = (*handle)->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader.has_value());
    auto _r = ss::defer([&reader] { (*reader)->close().get(); });
    drain_reader(**reader);

    const auto& calls = fio.read_object_calls();
    ASSERT_FALSE(calls.empty());
    EXPECT_EQ(calls.front().position, seg.p1 - (seg.p1 % 64))
      << "first cache chunk is aligned down to the 64-byte boundary";
    EXPECT_LT(calls.front().position, seg.p1);
}

// ts/nocache alignment wiring: with no cache to dedup against, the first
// read_object starts exactly at the seek position (request-relative), not the
// chunk boundary before it.
TEST_F(OpenObjectChunkingTest, TsNocacheStartsFirstChunkAtSeekPos) {
    fake_io fio;
    const ts_segment_path ts_path{"00000000000000000000-23-v1.log"};
    auto seg = make_indexed_segment();
    ASSERT_NE(seg.p1 % 128, 0u)
      << "batch1 must be mid-chunk for this to matter";
    const size_t sz = seg.segment.size_bytes();
    fio.put_ts_segment(
      ts_path, std::move(seg.segment), {}, std::move(seg.index));

    auto tidp = model::topic_id_partition{
      model::topic_id(uuid_t::create()), model::partition_id{0}};
    ss::abort_source as;
    object_extent extent{
      .id = create_object_id(),
      .position = 0,
      .size = sz,
      .imported = imported_ts_info{.ts_path = ts_path, .delta_base = 0_od},
    };
    auto handle = open_object(
                    fio,
                    extent,
                    &as,
                    cloud_io::group_id::default_group,
                    /*skip_cache=*/true)
                    .get();
    ASSERT_TRUE(handle.has_value());
    auto seek = (*handle)->index().seek_offset_le(tidp, 2_o);
    ASSERT_TRUE(seek.has_value());
    ASSERT_EQ(seek->file_position, seg.p1);
    auto reader = (*handle)->open_reader(*seek, &as).get();
    ASSERT_TRUE(reader.has_value());
    auto _r = ss::defer([&reader] { (*reader)->close().get(); });
    drain_reader(**reader);

    const auto& calls = fio.read_object_calls();
    ASSERT_FALSE(calls.empty());
    EXPECT_EQ(calls.front().position, seg.p1)
      << "first streaming chunk starts at the seek position, unaligned";
}
