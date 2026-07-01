/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_storage/types.h"
#include "cluster/archival/migration_metastore.h"
#include "cluster/archival/migration_segment.h"
#include "model/fundamental.h"

#include <gtest/gtest.h>

using namespace archival;
using cloud_storage::segment_meta;
using cloud_storage::segment_name_format;
using tx_state = migration_metastore::tx_manifest_state;

namespace {

// A segment_meta with sane, translation-free defaults: log offsets [base, base
// + count - 1], delta 0 (kafka offset == log offset), v3, not compacted, no
// .tx. Individual fields are overridden per test to walk the format matrix.
segment_meta meta_with(
  segment_name_format fmt,
  bool compacted,
  uint64_t metadata_size_hint,
  model::offset_delta delta = model::offset_delta{0},
  model::offset base = model::offset{0},
  model::offset committed = model::offset{9}) {
    return segment_meta{
      .is_compacted = compacted,
      .size_bytes = 1024,
      .base_offset = base,
      .committed_offset = committed,
      .base_timestamp = model::timestamp{1000},
      .max_timestamp = model::timestamp{2000},
      .delta_offset = delta,
      .ntp_revision = model::initial_revision_id{1},
      .archiver_term = model::term_id{1},
      .segment_term = model::term_id{2},
      .delta_offset_end = delta,
      .sname_format = fmt,
      .metadata_size_hint = metadata_size_hint,
    };
}

} // namespace

// --- .tx-presence resolution across the format matrix -----------------------

// v3 with metadata_size_hint == 0: the .tx manifest is authoritatively empty,
// so the read path can skip the probe -> absent.
TEST(MigrationSegmentTest, TxStateV3NoHintIsAbsent) {
    auto seg = make_imported_segment(
      meta_with(segment_name_format::v3, /*compacted=*/false, /*hint=*/0),
      "p/seg-v3-empty.log");
    ASSERT_TRUE(seg.has_value());
    EXPECT_EQ(seg->tx_state, tx_state::absent);
}

// v3 with metadata_size_hint > 0: the .tx manifest is known present -> present
// (the read path downloads it and treats a miss as a hard error).
TEST(MigrationSegmentTest, TxStateV3WithHintIsPresent) {
    auto seg = make_imported_segment(
      meta_with(segment_name_format::v3, /*compacted=*/false, /*hint=*/512),
      "p/seg-v3-tx.log");
    ASSERT_TRUE(seg.has_value());
    EXPECT_EQ(seg->tx_state, tx_state::present);
}

// v1/v2 don't encode .tx presence -> unknown (the read path probes and
// tolerates a missing .tx). metadata_size_hint is meaningless pre-v3 and must
// not be consulted.
TEST(MigrationSegmentTest, TxStateV1IsUnknown) {
    auto seg = make_imported_segment(
      meta_with(segment_name_format::v1, /*compacted=*/false, /*hint=*/0),
      "p/seg-v1.log");
    ASSERT_TRUE(seg.has_value());
    EXPECT_EQ(seg->tx_state, tx_state::unknown);
}

TEST(MigrationSegmentTest, TxStateV2IsUnknown) {
    // Even a v2 segment carrying a (meaningless) non-zero hint stays unknown:
    // the hint is only authoritative for v3.
    auto seg = make_imported_segment(
      meta_with(segment_name_format::v2, /*compacted=*/false, /*hint=*/512),
      "p/seg-v2.log");
    ASSERT_TRUE(seg.has_value());
    EXPECT_EQ(seg->tx_state, tx_state::unknown);
}

// A compacted segment has no aborted batches by construction, regardless of
// format -> absent, even for v1/v2 (which otherwise would be unknown).
TEST(MigrationSegmentTest, TxStateCompactedIsAbsentEvenForV1) {
    auto seg = make_imported_segment(
      meta_with(segment_name_format::v1, /*compacted=*/true, /*hint=*/0),
      "p/seg-v1-compacted.log");
    ASSERT_TRUE(seg.has_value());
    EXPECT_EQ(seg->tx_state, tx_state::absent);
}

// --- delta_base sentinel guard ----------------------------------------------

// A normal delta is carried through verbatim.
TEST(MigrationSegmentTest, DeltaBaseCarriedThrough) {
    auto seg = make_imported_segment(
      meta_with(
        segment_name_format::v3,
        /*compacted=*/false,
        /*hint=*/0,
        /*delta=*/model::offset_delta{5},
        /*base=*/model::offset{15},
        /*committed=*/model::offset{19}),
      "p/seg.log");
    ASSERT_TRUE(seg.has_value());
    EXPECT_EQ(seg->delta_base, model::offset_delta{5});
    // base/last bounds use the same delta: kafka [15-5, 19-5] = [10, 14].
    EXPECT_EQ(seg->base_kafka_offset, kafka::offset{10});
    EXPECT_EQ(seg->last_kafka_offset, kafka::offset{14});
}

// The pre-offset-translation sentinel (offset_delta::min()) must be clamped to
// 0, matching what segment_meta's kafka-offset helpers do for the base/last
// bounds -- otherwise the reader would translate by INT64_MIN.
TEST(MigrationSegmentTest, DeltaBaseSentinelClampedToZero) {
    auto seg = make_imported_segment(
      meta_with(
        segment_name_format::v1,
        /*compacted=*/false,
        /*hint=*/0,
        /*delta=*/model::offset_delta::min(),
        /*base=*/model::offset{0},
        /*committed=*/model::offset{9}),
      "p/seg-ancient.log");
    ASSERT_TRUE(seg.has_value());
    EXPECT_EQ(seg->delta_base, model::offset_delta{0});
    // With the sentinel clamped, kafka offset == log offset: [0, 9].
    EXPECT_EQ(seg->base_kafka_offset, kafka::offset{0});
    EXPECT_EQ(seg->last_kafka_offset, kafka::offset{9});
}

// --- skip of Kafka-empty segments -------------------------------------------

// A segment whose data is entirely non-data batches (delta == span, so
// base_kafka_offset == next_kafka_offset) has no Kafka-addressable records and
// must be skipped (nullopt) rather than yielding an inverted extent.
TEST(MigrationSegmentTest, KafkaEmptySegmentIsSkipped) {
    // log [0,4], delta_offset 0 but delta_offset_end 5 => next_kafka_offset 0,
    // base_kafka_offset 0 -> inverted/empty.
    auto meta = meta_with(
      segment_name_format::v3,
      /*compacted=*/false,
      /*hint=*/0,
      /*delta=*/model::offset_delta{0},
      /*base=*/model::offset{0},
      /*committed=*/model::offset{4});
    meta.delta_offset_end = model::offset_delta{5};
    auto seg = make_imported_segment(meta, "p/seg-empty.log");
    EXPECT_FALSE(seg.has_value());
}

// The descriptor carries the path and the term/timestamp/size verbatim.
TEST(MigrationSegmentTest, CarriesPathAndScalarFields) {
    auto seg = make_imported_segment(
      meta_with(segment_name_format::v3, /*compacted=*/false, /*hint=*/0),
      "p/seg-fields.log");
    ASSERT_TRUE(seg.has_value());
    EXPECT_EQ(seg->ts_path, "p/seg-fields.log");
    EXPECT_EQ(seg->term, model::term_id{2});
    EXPECT_EQ(seg->max_timestamp, model::timestamp{2000});
    EXPECT_EQ(seg->size_bytes, 1024u);
}
