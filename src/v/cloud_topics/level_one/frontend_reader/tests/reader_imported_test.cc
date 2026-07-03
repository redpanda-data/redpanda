/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "container/chunked_circular_buffer.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "storage/record_batch_utils.h"

#include <gtest/gtest.h>

using namespace cloud_topics::l1;

namespace {

kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}

model::offset_delta operator""_od(unsigned long long d) {
    return model::offset_delta{static_cast<int64_t>(d)};
}

model::record_batch make_data_batch(
  kafka::offset base, kafka::offset last, model::timestamp ts = {}) {
    int count = static_cast<int>(last - base) + 1;
    std::vector<size_t> record_sizes(count, 64);
    return model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = kafka::offset_cast(base),
        .count = count,
        .record_sizes = record_sizes,
        .timestamp = ts,
        .all_records_have_same_timestamp = true,
      });
}

model::record_batch make_config_batch(kafka::offset base) {
    return model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = kafka::offset_cast(base),
        .count = 1,
        .bt = model::record_batch_type::raft_configuration,
        .record_sizes = std::vector<size_t>{32},
      });
}

iobuf batch_to_ts_bytes(model::record_batch batch) {
    iobuf out;
    out.append(storage::batch_header_to_disk_iobuf(batch.header()));
    out.append(std::move(batch).release_data());
    return out;
}

} // namespace

class ReaderImportedTest : public l1_reader_fixture {};

// Five data batches at log offsets 5-9 with delta=5 are registered as an
// imported extent (kafka offsets 0-4). The reader must return all five batches
// with kafka base_offsets 0-4, demonstrating that delta translation is applied.
TEST_F(ReaderImportedTest, ReadImportedExtentReturnsKafkaOffsets) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    iobuf seg;
    for (int i = 0; i < 5; ++i) {
        // log offset 5+i, base kafka offset 0 → delta 5 → kafka offset i
        seg.append(batch_to_ts_bytes(
          make_data_batch(kafka::offset{5 + i}, kafka::offset{5 + i})));
    }

    register_imported_extent(
      tidp, std::move(seg), 0_o, 4_o, 5_od, l1::ts_segment_path{"t/seg-a.log"})
      .get();

    auto batches = read_all(make_reader(ntp, tidp, /*start_offset=*/0_o));

    ASSERT_EQ(batches.size(), 5u);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(
          batches[i].base_offset(), kafka::offset_cast(kafka::offset{i}))
          << "batch " << i;
    }
}

// Same imported extent (log offsets 5-9, delta=5, kafka 0-4). A reader
// starting at kafka offset 2 must return only the three batches at kafka
// offsets 2, 3, 4. Batches at offsets 0 and 1 are read from the segment but
// skipped by read_batches because they are before the start offset.
TEST_F(ReaderImportedTest, ReadImportedExtentRespectsStartOffset) {
    auto [ntp, tidp] = make_ntidp("test_topic2");

    iobuf seg;
    for (int i = 0; i < 5; ++i) {
        seg.append(batch_to_ts_bytes(
          make_data_batch(kafka::offset{5 + i}, kafka::offset{5 + i})));
    }

    register_imported_extent(
      tidp, std::move(seg), 0_o, 4_o, 5_od, l1::ts_segment_path{"t/seg-b.log"})
      .get();

    auto batches = read_all(make_reader(ntp, tidp, /*start_offset=*/2_o));

    ASSERT_EQ(batches.size(), 3u);
    EXPECT_EQ(batches[0].base_offset(), kafka::offset_cast(2_o));
    EXPECT_EQ(batches[1].base_offset(), kafka::offset_cast(3_o));
    EXPECT_EQ(batches[2].base_offset(), kafka::offset_cast(4_o));
}

// A raft_configuration batch at log offset 0 (delta starts at 0, 1 record →
// running delta becomes 1) followed by two data batches at log offsets 1 and 2
// (kafka offsets 0 and 1). The reader must skip the config batch and return
// exactly 2 data batches.
TEST_F(ReaderImportedTest, ReadImportedExtentSkipsNonDataBatches) {
    auto [ntp, tidp] = make_ntidp("test_topic3");

    iobuf seg;
    // raft_configuration at log offset 0; 1 record → running delta 0 → 1
    seg.append(batch_to_ts_bytes(make_config_batch(kafka::offset{0})));
    // data at log offset 1 → kafka offset 1 - 1 = 0
    seg.append(batch_to_ts_bytes(make_data_batch(1_o, 1_o)));
    // data at log offset 2 → kafka offset 2 - 1 = 1
    seg.append(batch_to_ts_bytes(make_data_batch(2_o, 2_o)));

    register_imported_extent(
      tidp, std::move(seg), 0_o, 1_o, 0_od, l1::ts_segment_path{"t/seg-c.log"})
      .get();

    auto batches = read_all(make_reader(ntp, tidp, /*start_offset=*/0_o));

    ASSERT_EQ(batches.size(), 2u);
    for (const auto& b : batches) {
        EXPECT_NE(b.header().type, model::record_batch_type::raft_configuration)
          << "config batch must not be returned";
    }
    EXPECT_EQ(batches[0].base_offset(), kafka::offset_cast(0_o));
    EXPECT_EQ(batches[1].base_offset(), kafka::offset_cast(1_o));
}

// A compacted segment whose leading records were removed. Compaction does not
// move a segment's base: the extent still declares base kafka offset 0, but the
// first surviving physical batch is at log offset 7 (kafka offset 2) -- log
// offsets 5,6 (kafka 0,1) were compacted away, leaving a two-offset front hole.
// delta is 5 throughout. The reader must report the survivors at their true
// kafka offsets 2,3,4; it must NOT renumber them down into the hole (0,1,2),
// which is what happens when the delta is inferred from the first batch's
// position rather than the segment's authoritative base delta.
TEST_F(ReaderImportedTest, ReadImportedExtentFrontHoleCompacted) {
    auto [ntp, tidp] = make_ntidp("test_topic_front_hole");

    iobuf seg;
    for (int log = 7; log <= 9; ++log) {
        seg.append(batch_to_ts_bytes(
          make_data_batch(kafka::offset{log}, kafka::offset{log})));
    }

    register_imported_extent(
      tidp, std::move(seg), 0_o, 4_o, 5_od, l1::ts_segment_path{"t/seg-fh.log"})
      .get();

    auto batches = read_all(make_reader(ntp, tidp, /*start_offset=*/0_o));

    ASSERT_EQ(batches.size(), 3u);
    EXPECT_EQ(batches[0].base_offset(), kafka::offset_cast(2_o));
    EXPECT_EQ(batches[1].base_offset(), kafka::offset_cast(3_o));
    EXPECT_EQ(batches[2].base_offset(), kafka::offset_cast(4_o));
}

// One imported extent covers kafka offsets 0-4 (log offsets 0-4, delta=0),
// followed by a native L1 object covering kafka offsets 5-9. A reader
// starting at offset 0 must return all 10 batches in offset order.
TEST_F(ReaderImportedTest, ReadImportedExtentThenNative) {
    auto [ntp, tidp] = make_ntidp("test_topic4");

    // Imported: 5 single-record batches, log offsets 0-4 = kafka offsets 0-4
    iobuf seg;
    for (int i = 0; i < 5; ++i) {
        seg.append(batch_to_ts_bytes(
          make_data_batch(kafka::offset{i}, kafka::offset{i})));
    }
    register_imported_extent(
      tidp, std::move(seg), 0_o, 4_o, 0_od, l1::ts_segment_path{"t/seg-d.log"})
      .get();

    // Native: 5 batches at kafka offsets 5-9
    chunked_circular_buffer<model::record_batch> native_batches;
    for (int i = 5; i < 10; ++i) {
        native_batches.push_back(
          make_data_batch(kafka::offset{i}, kafka::offset{i}));
    }

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(native_batches));
    make_l1_objects(std::move(tidp_batches)).get();

    auto batches = read_all(make_reader(ntp, tidp, /*start_offset=*/0_o));

    ASSERT_EQ(batches.size(), 10u);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(
          batches[i].base_offset(), kafka::offset_cast(kafka::offset{i}))
          << "batch " << i;
    }
}
