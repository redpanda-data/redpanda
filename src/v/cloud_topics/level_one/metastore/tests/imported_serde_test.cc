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
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "serde/rw/envelope.h"

#include <gtest/gtest.h>

using namespace cloud_topics::l1;

namespace {

/// Mirrors extent_row_value at version<0>, before the `imported` field was
/// added in version<1>.
struct extent_row_value_v0
  : public serde::envelope<
      extent_row_value_v0,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(last_offset, max_timestamp, filepos, len, oid);
    }
    kafka::offset last_offset{};
    model::timestamp max_timestamp{};
    size_t filepos{0};
    size_t len{0};
    object_id oid{};
};

/// Mirrors object_entry at version<1>, before the `imported` field was added
/// in version<2>.
struct object_entry_v1
  : public serde::
      envelope<object_entry_v1, serde::version<1>, serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(
          total_data_size,
          removed_data_size,
          footer_pos,
          object_size,
          last_updated,
          is_preregistration);
    }
    size_t total_data_size{0};
    size_t removed_data_size{0};
    size_t footer_pos{0};
    size_t object_size{0};
    model::timestamp last_updated{};
    bool is_preregistration{false};
};

} // namespace

// Encode a v0 extent_row_value (no `imported` field) and verify that decoding
// it as the current v1 struct leaves `imported` as nullopt.
TEST(ImportedSerdeTest, ExtentRowValueV0CompatV1) {
    extent_row_value_v0 v0;
    v0.last_offset = kafka::offset{42};
    v0.max_timestamp = model::timestamp{1000};
    v0.filepos = 100;
    v0.len = 200;
    v0.oid = create_object_id();

    iobuf buf = serde::to_iobuf(v0);
    auto v1 = serde::from_iobuf<extent_row_value>(std::move(buf));

    ASSERT_EQ(v1.last_offset, v0.last_offset);
    ASSERT_EQ(v1.max_timestamp, v0.max_timestamp);
    ASSERT_EQ(v1.filepos, v0.filepos);
    ASSERT_EQ(v1.len, v0.len);
    ASSERT_EQ(v1.oid, v0.oid);
    ASSERT_EQ(v1.imported_ts_info, std::nullopt);
}

// Verify that a v1 extent_row_value with `imported` populated round-trips
// through serde correctly.
TEST(ImportedSerdeTest, ExtentRowValueV1RoundTrip) {
    extent_row_value v1;
    v1.last_offset = kafka::offset{100};
    v1.max_timestamp = model::timestamp{2000};
    v1.filepos = 300;
    v1.len = 400;
    v1.oid = create_object_id();
    v1.imported_ts_info = imported_ts_segment_info{
      .segment_term = model::term_id{7},
      .delta_base = model::offset_delta{5},
      .tx_state = tx_manifest_state::present,
    };

    iobuf buf = serde::to_iobuf(v1);
    auto decoded = serde::from_iobuf<extent_row_value>(std::move(buf));

    ASSERT_EQ(decoded.last_offset, v1.last_offset);
    ASSERT_EQ(decoded.max_timestamp, v1.max_timestamp);
    ASSERT_EQ(decoded.filepos, v1.filepos);
    ASSERT_EQ(decoded.len, v1.len);
    ASSERT_EQ(decoded.oid, v1.oid);
    ASSERT_TRUE(decoded.imported_ts_info.has_value());
    ASSERT_EQ(decoded.imported_ts_info, v1.imported_ts_info);
}

// Encode a v1 object_entry (no `imported` field) and verify that decoding it
// as the current v2 struct leaves `imported` as nullopt.
TEST(ImportedSerdeTest, ObjectEntryV1CompatV2) {
    object_entry_v1 v1;
    v1.total_data_size = 1024;
    v1.removed_data_size = 512;
    v1.footer_pos = 900;
    v1.object_size = 1000;
    v1.last_updated = model::timestamp{3000};
    v1.is_preregistration = false;

    iobuf buf = serde::to_iobuf(v1);
    auto v2 = serde::from_iobuf<object_entry>(std::move(buf));

    ASSERT_EQ(v2.total_data_size, v1.total_data_size);
    ASSERT_EQ(v2.removed_data_size, v1.removed_data_size);
    ASSERT_EQ(v2.footer_pos, v1.footer_pos);
    ASSERT_EQ(v2.object_size, v1.object_size);
    ASSERT_EQ(v2.last_updated, v1.last_updated);
    ASSERT_EQ(v2.is_preregistration, v1.is_preregistration);
    ASSERT_EQ(v2.imported_ts_location, std::nullopt);
}

// Verify that a v2 object_entry with `imported` populated round-trips through
// serde correctly.
TEST(ImportedSerdeTest, ObjectEntryV2RoundTrip) {
    object_entry v2;
    v2.total_data_size = 2048;
    v2.removed_data_size = 0;
    v2.footer_pos = 1900;
    v2.object_size = 2000;
    v2.last_updated = model::timestamp{4000};
    v2.is_preregistration = false;
    v2.imported_ts_location = imported_ts_object_location{
      .ts_path = ts_segment_path{"00000000000000000000-1-v1.log"},
    };

    iobuf buf = serde::to_iobuf(v2);
    auto decoded = serde::from_iobuf<object_entry>(std::move(buf));

    ASSERT_EQ(decoded, v2);
}

// Verify that imported_ts_segment_info round-trips through serde correctly.
TEST(ImportedSerdeTest, ImportedTsSegmentInfoRoundTrip) {
    imported_ts_segment_info info{
      .segment_term = model::term_id{3},
      .delta_base = model::offset_delta{2},
      .tx_state = tx_manifest_state::absent,
    };

    iobuf buf = serde::to_iobuf(info);
    auto decoded = serde::from_iobuf<imported_ts_segment_info>(std::move(buf));
    ASSERT_EQ(decoded, info);
}
// Verify that imported_ts_object_location round-trips through serde correctly.
TEST(ImportedSerdeTest, ImportedTsObjectLocationRoundTrip) {
    imported_ts_object_location loc{
      .ts_path = ts_segment_path{"00000000000000000000-1-v1.log"},
    };

    iobuf buf = serde::to_iobuf(loc);
    auto decoded = serde::from_iobuf<imported_ts_object_location>(
      std::move(buf));
    ASSERT_EQ(decoded, loc);
}
