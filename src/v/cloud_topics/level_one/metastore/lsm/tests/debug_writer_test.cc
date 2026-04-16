/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/lsm/debug_writer.h"
#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "model/fundamental.h"
#include "serde/rw/rw.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

using namespace cloud_topics::l1;
namespace pm = proto::admin::metastore;

namespace {

constexpr std::string_view test_uuid_str
  = "12345678-1234-5678-1234-567812345678";
constexpr std::string_view test_obj_uuid_str
  = "abcdef01-2345-6789-abcd-ef0123456789";

model::topic_id_partition
make_tidp(std::string_view uuid_str = test_uuid_str, int pid = 0) {
    return model::topic_id_partition(
      model::topic_id(uuid_t::from_string(uuid_str)), model::partition_id(pid));
}

pm::row_key
make_metadata_key(std::string_view uuid = test_uuid_str, int32_t pid = 0) {
    pm::row_key key;
    pm::metadata_key mk;
    mk.set_topic_id(std::string(uuid));
    mk.set_partition_id(pid);
    key.set_metadata(std::move(mk));
    return key;
}

pm::row_key make_extent_key(
  std::string_view uuid = test_uuid_str, int32_t pid = 0, int64_t base = 100) {
    pm::row_key key;
    pm::extent_key ek;
    ek.set_topic_id(std::string(uuid));
    ek.set_partition_id(pid);
    ek.set_base_offset(base);
    key.set_extent(std::move(ek));
    return key;
}

pm::row_key make_term_key(
  std::string_view uuid = test_uuid_str, int32_t pid = 0, int64_t term = 5) {
    pm::row_key key;
    pm::term_key tk;
    tk.set_topic_id(std::string(uuid));
    tk.set_partition_id(pid);
    tk.set_term_id(term);
    key.set_term(std::move(tk));
    return key;
}

pm::row_key
make_compaction_key(std::string_view uuid = test_uuid_str, int32_t pid = 0) {
    pm::row_key key;
    pm::compaction_key ck;
    ck.set_topic_id(std::string(uuid));
    ck.set_partition_id(pid);
    key.set_compaction(std::move(ck));
    return key;
}

pm::row_key make_object_key(std::string_view uuid = test_obj_uuid_str) {
    pm::row_key key;
    pm::object_key ok;
    ok.set_object_id(std::string(uuid));
    key.set_object(std::move(ok));
    return key;
}

} // namespace

TEST(DebugWriterTest, EncodeMetadataKey) {
    pm::write_rows_request req;
    pm::write_row w;
    w.set_key(make_metadata_key());
    pm::row_value val;
    pm::metadata_value mv;
    mv.set_start_offset(10);
    mv.set_next_offset(20);
    mv.set_compaction_epoch(1);
    mv.set_size(1000);
    mv.set_num_extents(42);
    val.set_metadata(std::move(mv));
    w.set_value(std::move(val));
    req.get_writes().push_back(std::move(w));

    auto result = debug_writer::build_rows(req);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);

    auto& row = (*result)[0];
    auto expected_key = metadata_row_key::encode(make_tidp());
    EXPECT_EQ(row.key, expected_key);

    auto decoded = serde::from_iobuf<metadata_row_value>(row.value.copy());
    EXPECT_EQ(decoded.start_offset, kafka::offset{10});
    EXPECT_EQ(decoded.next_offset, kafka::offset{20});
    EXPECT_EQ(decoded.compaction_epoch, partition_state::compaction_epoch_t{1});
    EXPECT_EQ(decoded.size, 1000);
    EXPECT_EQ(decoded.num_extents, 42);
}

TEST(DebugWriterTest, EncodeExtentKey) {
    pm::write_rows_request req;
    pm::write_row w;
    w.set_key(make_extent_key());
    pm::row_value val;
    pm::extent_value ev;
    ev.set_last_offset(199);
    ev.set_max_timestamp(42000);
    ev.set_filepos(512);
    ev.set_len(1024);
    ev.set_object_id(std::string(test_obj_uuid_str));
    val.set_extent(std::move(ev));
    w.set_value(std::move(val));
    req.get_writes().push_back(std::move(w));

    auto result = debug_writer::build_rows(req);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);

    auto& row = (*result)[0];
    auto expected_key = extent_row_key::encode(make_tidp(), kafka::offset{100});
    EXPECT_EQ(row.key, expected_key);

    auto decoded = serde::from_iobuf<extent_row_value>(row.value.copy());
    EXPECT_EQ(decoded.last_offset, kafka::offset{199});
    EXPECT_EQ(decoded.max_timestamp, model::timestamp{42000});
    EXPECT_EQ(decoded.filepos, 512);
    EXPECT_EQ(decoded.len, 1024);
    EXPECT_EQ(decoded.oid, object_id{uuid_t::from_string(test_obj_uuid_str)});
}

TEST(DebugWriterTest, EncodeTermKey) {
    pm::write_rows_request req;
    pm::write_row w;
    w.set_key(make_term_key());
    pm::row_value val;
    pm::term_value tv;
    tv.set_term_start_offset(50);
    val.set_term(std::move(tv));
    w.set_value(std::move(val));
    req.get_writes().push_back(std::move(w));

    auto result = debug_writer::build_rows(req);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);

    auto& row = (*result)[0];
    auto expected_key = term_row_key::encode(make_tidp(), model::term_id{5});
    EXPECT_EQ(row.key, expected_key);

    auto decoded = serde::from_iobuf<term_row_value>(row.value.copy());
    EXPECT_EQ(decoded.term_start_offset, kafka::offset{50});
}

TEST(DebugWriterTest, EncodeCompactionKey) {
    pm::write_rows_request req;
    pm::write_row w;
    w.set_key(make_compaction_key());
    pm::row_value val;
    pm::compaction_value cv;
    pm::offset_range range;
    range.set_base_offset(0);
    range.set_last_offset(99);
    cv.get_cleaned_ranges().push_back(std::move(range));
    pm::cleaned_range_with_tombstones tr;
    tr.set_base_offset(50);
    tr.set_last_offset(99);
    tr.set_cleaned_with_tombstones_at(12345);
    cv.get_cleaned_ranges_with_tombstones().push_back(std::move(tr));
    val.set_compaction(std::move(cv));
    w.set_value(std::move(val));
    req.get_writes().push_back(std::move(w));

    auto result = debug_writer::build_rows(req);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);

    auto& row = (*result)[0];
    auto expected_key = compaction_row_key::encode(make_tidp());
    EXPECT_EQ(row.key, expected_key);

    auto decoded = serde::from_iobuf<compaction_row_value>(row.value.copy());
    EXPECT_FALSE(decoded.state.cleaned_ranges.empty());
    EXPECT_EQ(decoded.state.cleaned_ranges_with_tombstones.size(), 1);
    auto& crt = *decoded.state.cleaned_ranges_with_tombstones.begin();
    EXPECT_EQ(crt.base_offset, kafka::offset{50});
    EXPECT_EQ(crt.last_offset, kafka::offset{99});
    EXPECT_EQ(crt.cleaned_with_tombstones_at, model::timestamp{12345});
}

TEST(DebugWriterTest, EncodeObjectKey) {
    pm::write_rows_request req;
    pm::write_row w;
    w.set_key(make_object_key());
    pm::row_value val;
    pm::object_value ov;
    ov.set_total_data_size(5000);
    ov.set_removed_data_size(100);
    ov.set_footer_pos(4800);
    ov.set_object_size(5100);
    ov.set_last_updated(99999);
    ov.set_is_preregistration(true);
    val.set_object(std::move(ov));
    w.set_value(std::move(val));
    req.get_writes().push_back(std::move(w));

    auto result = debug_writer::build_rows(req);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);

    auto& row = (*result)[0];
    auto expected_key = object_row_key::encode(
      object_id{uuid_t::from_string(test_obj_uuid_str)});
    EXPECT_EQ(row.key, expected_key);

    auto decoded = serde::from_iobuf<object_row_value>(row.value.copy());
    EXPECT_EQ(decoded.object.total_data_size, 5000);
    EXPECT_EQ(decoded.object.removed_data_size, 100);
    EXPECT_EQ(decoded.object.footer_pos, 4800);
    EXPECT_EQ(decoded.object.object_size, 5100);
    EXPECT_EQ(decoded.object.last_updated, model::timestamp{99999});
    EXPECT_TRUE(decoded.object.is_preregistration);
}

TEST(DebugWriterTest, TombstoneProducesEmptyValue) {
    pm::write_rows_request req;
    req.get_deletes().push_back(make_metadata_key());

    auto result = debug_writer::build_rows(req);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);

    auto& row = (*result)[0];
    auto expected_key = metadata_row_key::encode(make_tidp());
    EXPECT_EQ(row.key, expected_key);
    EXPECT_EQ(row.value.size_bytes(), 0);
}

TEST(DebugWriterTest, InvalidUuidReturnsError) {
    pm::write_rows_request req;
    pm::write_row w;
    w.set_key(make_metadata_key("not-a-valid-uuid"));
    pm::row_value val;
    pm::metadata_value mv;
    mv.set_start_offset(0);
    mv.set_next_offset(0);
    val.set_metadata(std::move(mv));
    w.set_value(std::move(val));
    req.get_writes().push_back(std::move(w));

    auto result = debug_writer::build_rows(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().e, debug_writer::errc::invalid_uuid);
}

TEST(DebugWriterTest, MixedWritesAndDeletes) {
    pm::write_rows_request req;

    // One write
    pm::write_row w;
    w.set_key(make_metadata_key());
    pm::row_value val;
    pm::metadata_value mv;
    mv.set_start_offset(0);
    mv.set_next_offset(10);
    val.set_metadata(std::move(mv));
    w.set_value(std::move(val));
    req.get_writes().push_back(std::move(w));

    // Two deletes
    req.get_deletes().push_back(make_extent_key());
    req.get_deletes().push_back(make_object_key());

    auto result = debug_writer::build_rows(req);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 3);

    // First row is the write
    EXPECT_GT((*result)[0].value.size_bytes(), 0);
    // Second and third are tombstones
    EXPECT_EQ((*result)[1].value.size_bytes(), 0);
    EXPECT_EQ((*result)[2].value.size_bytes(), 0);
}
