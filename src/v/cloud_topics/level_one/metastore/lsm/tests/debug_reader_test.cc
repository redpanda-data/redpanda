/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/lsm/debug_reader.h"
#include "cloud_topics/level_one/metastore/lsm/debug_serde.h"
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

pm::row_value make_metadata_value() {
    pm::row_value val;
    pm::metadata_value mv;
    mv.set_start_offset(10);
    mv.set_next_offset(20);
    mv.set_compaction_epoch(1);
    mv.set_size(1000);
    mv.set_num_extents(42);
    val.set_metadata(std::move(mv));
    return val;
}

pm::row_value make_extent_value() {
    pm::row_value val;
    pm::extent_value ev;
    ev.set_last_offset(199);
    ev.set_max_timestamp(42000);
    ev.set_filepos(512);
    ev.set_len(1024);
    ev.set_object_id(std::string(test_obj_uuid_str));
    val.set_extent(std::move(ev));
    return val;
}

pm::row_value make_term_value() {
    pm::row_value val;
    pm::term_value tv;
    tv.set_term_start_offset(50);
    val.set_term(std::move(tv));
    return val;
}

pm::row_value make_compaction_value() {
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
    return val;
}

pm::row_value make_object_value() {
    pm::row_value val;
    pm::object_value ov;
    ov.set_total_data_size(5000);
    ov.set_removed_data_size(100);
    ov.set_footer_pos(4800);
    ov.set_object_size(5100);
    ov.set_last_updated(99999);
    ov.set_is_preregistration(true);
    val.set_object(std::move(ov));
    return val;
}

write_batch_row encode_row(const pm::row_key& key, const pm::row_value& val) {
    auto key_res = debug_encode_key(key);
    EXPECT_TRUE(key_res.has_value());
    auto val_res = debug_encode_value(val);
    EXPECT_TRUE(val_res.has_value());
    return write_batch_row{
      .key = std::move(*key_res),
      .value = std::move(*val_res),
    };
}

} // namespace

TEST(DebugReaderTest, DecodeMetadataKey) {
    auto raw_key = metadata_row_key::encode(make_tidp());
    auto result = debug_reader::decode_key(raw_key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->type, row_type::metadata);
    ASSERT_TRUE(result->key.has_metadata());
    EXPECT_EQ(result->key.get_metadata().get_topic_id(), test_uuid_str);
    EXPECT_EQ(result->key.get_metadata().get_partition_id(), 0);
}

TEST(DebugReaderTest, DecodeExtentKey) {
    auto raw_key = extent_row_key::encode(make_tidp(), kafka::offset{100});
    auto result = debug_reader::decode_key(raw_key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->type, row_type::extent);
    ASSERT_TRUE(result->key.has_extent());
    EXPECT_EQ(result->key.get_extent().get_topic_id(), test_uuid_str);
    EXPECT_EQ(result->key.get_extent().get_partition_id(), 0);
    EXPECT_EQ(result->key.get_extent().get_base_offset(), 100);
}

TEST(DebugReaderTest, DecodeTermKey) {
    auto raw_key = term_row_key::encode(make_tidp(), model::term_id{5});
    auto result = debug_reader::decode_key(raw_key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->type, row_type::term_start);
    ASSERT_TRUE(result->key.has_term());
    EXPECT_EQ(result->key.get_term().get_topic_id(), test_uuid_str);
    EXPECT_EQ(result->key.get_term().get_partition_id(), 0);
    EXPECT_EQ(result->key.get_term().get_term_id(), 5);
}

TEST(DebugReaderTest, DecodeCompactionKey) {
    auto raw_key = compaction_row_key::encode(make_tidp());
    auto result = debug_reader::decode_key(raw_key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->type, row_type::compaction);
    ASSERT_TRUE(result->key.has_compaction());
    EXPECT_EQ(result->key.get_compaction().get_topic_id(), test_uuid_str);
    EXPECT_EQ(result->key.get_compaction().get_partition_id(), 0);
}

TEST(DebugReaderTest, DecodeObjectKey) {
    auto raw_key = object_row_key::encode(
      object_id{uuid_t::from_string(test_obj_uuid_str)});
    auto result = debug_reader::decode_key(raw_key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->type, row_type::object);
    ASSERT_TRUE(result->key.has_object());
    EXPECT_EQ(result->key.get_object().get_object_id(), test_obj_uuid_str);
}

TEST(DebugReaderTest, RoundTripMetadataValue) {
    auto val = make_metadata_value();
    auto encoded = debug_encode_value(val);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = debug_reader::decode_value(
      row_type::metadata, std::move(*encoded));
    ASSERT_TRUE(decoded.has_value());
    ASSERT_TRUE(decoded->has_metadata());
    EXPECT_EQ(decoded->get_metadata().get_start_offset(), 10);
    EXPECT_EQ(decoded->get_metadata().get_next_offset(), 20);
    EXPECT_EQ(decoded->get_metadata().get_compaction_epoch(), 1);
    EXPECT_EQ(decoded->get_metadata().get_size(), 1000);
    EXPECT_EQ(decoded->get_metadata().get_num_extents(), 42);
}

TEST(DebugReaderTest, RoundTripExtentValue) {
    auto val = make_extent_value();
    auto encoded = debug_encode_value(val);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = debug_reader::decode_value(
      row_type::extent, std::move(*encoded));
    ASSERT_TRUE(decoded.has_value());
    ASSERT_TRUE(decoded->has_extent());
    EXPECT_EQ(decoded->get_extent().get_last_offset(), 199);
    EXPECT_EQ(decoded->get_extent().get_max_timestamp(), 42000);
    EXPECT_EQ(decoded->get_extent().get_filepos(), 512);
    EXPECT_EQ(decoded->get_extent().get_len(), 1024);
    EXPECT_EQ(decoded->get_extent().get_object_id(), test_obj_uuid_str);
}

TEST(DebugReaderTest, RoundTripTermValue) {
    auto val = make_term_value();
    auto encoded = debug_encode_value(val);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = debug_reader::decode_value(
      row_type::term_start, std::move(*encoded));
    ASSERT_TRUE(decoded.has_value());
    ASSERT_TRUE(decoded->has_term());
    EXPECT_EQ(decoded->get_term().get_term_start_offset(), 50);
}

TEST(DebugReaderTest, RoundTripCompactionValue) {
    auto val = make_compaction_value();
    auto encoded = debug_encode_value(val);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = debug_reader::decode_value(
      row_type::compaction, std::move(*encoded));
    ASSERT_TRUE(decoded.has_value());
    ASSERT_TRUE(decoded->has_compaction());
    ASSERT_EQ(decoded->get_compaction().get_cleaned_ranges().size(), 1);
    EXPECT_EQ(
      decoded->get_compaction().get_cleaned_ranges()[0].get_base_offset(), 0);
    EXPECT_EQ(
      decoded->get_compaction().get_cleaned_ranges()[0].get_last_offset(), 99);
    ASSERT_EQ(
      decoded->get_compaction().get_cleaned_ranges_with_tombstones().size(), 1);
    EXPECT_EQ(
      decoded->get_compaction()
        .get_cleaned_ranges_with_tombstones()[0]
        .get_base_offset(),
      50);
    EXPECT_EQ(
      decoded->get_compaction()
        .get_cleaned_ranges_with_tombstones()[0]
        .get_cleaned_with_tombstones_at(),
      12345);
}

TEST(DebugReaderTest, RoundTripObjectValue) {
    auto val = make_object_value();
    auto encoded = debug_encode_value(val);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = debug_reader::decode_value(
      row_type::object, std::move(*encoded));
    ASSERT_TRUE(decoded.has_value());
    ASSERT_TRUE(decoded->has_object());
    EXPECT_EQ(decoded->get_object().get_total_data_size(), 5000);
    EXPECT_EQ(decoded->get_object().get_removed_data_size(), 100);
    EXPECT_EQ(decoded->get_object().get_footer_pos(), 4800);
    EXPECT_EQ(decoded->get_object().get_object_size(), 5100);
    EXPECT_EQ(decoded->get_object().get_last_updated(), 99999);
    EXPECT_TRUE(decoded->get_object().get_is_preregistration());
}

TEST(DebugReaderTest, UnknownRowTypeReturnsError) {
    // Construct a key with an invalid row_type prefix "ff".
    ss::sstring bad_key = "ff00000000000000000000000000000000000000000000";
    auto result = debug_reader::decode_key(bad_key);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().e, debug_serde_errc::unknown_row_type);
}

TEST(DebugReaderTest, RoundTripTermKeyViaEncode) {
    auto proto_key = make_term_key();
    auto raw = debug_encode_key(proto_key);
    ASSERT_TRUE(raw.has_value());
    auto decoded = debug_reader::decode_key(*raw);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->type, row_type::term_start);
    ASSERT_TRUE(decoded->key.has_term());
    EXPECT_EQ(decoded->key.get_term().get_topic_id(), test_uuid_str);
    EXPECT_EQ(decoded->key.get_term().get_term_id(), 5);
}

TEST(DebugReaderTest, RoundTripCompactionKeyViaEncode) {
    auto proto_key = make_compaction_key();
    auto raw = debug_encode_key(proto_key);
    ASSERT_TRUE(raw.has_value());
    auto decoded = debug_reader::decode_key(*raw);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->type, row_type::compaction);
    ASSERT_TRUE(decoded->key.has_compaction());
    EXPECT_EQ(decoded->key.get_compaction().get_topic_id(), test_uuid_str);
    EXPECT_EQ(decoded->key.get_compaction().get_partition_id(), 0);
}

TEST(DebugReaderTest, BuildResponseMultipleRows) {
    chunked_vector<write_batch_row> rows;
    rows.push_back(encode_row(make_metadata_key(), make_metadata_value()));
    rows.push_back(encode_row(make_extent_key(), make_extent_value()));
    rows.push_back(encode_row(make_object_key(), make_object_value()));

    auto result = debug_reader::build_response(rows, std::nullopt);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->get_rows().size(), 3);
    EXPECT_TRUE(result->get_next_key().empty());

    EXPECT_TRUE(result->get_rows()[0].get_key().has_metadata());
    EXPECT_TRUE(result->get_rows()[1].get_key().has_extent());
    EXPECT_TRUE(result->get_rows()[2].get_key().has_object());
}

TEST(DebugReaderTest, BuildResponseWithNextKey) {
    chunked_vector<write_batch_row> rows;
    rows.push_back(encode_row(make_metadata_key(), make_metadata_value()));

    auto result = debug_reader::build_response(rows, "some_next_key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->get_rows().size(), 1);
    EXPECT_EQ(result->get_next_key(), "some_next_key");
}
