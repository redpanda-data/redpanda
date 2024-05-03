// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "random/generators.h"
#include "storage/mvlog/entry.h"
#include "storage/mvlog/entry_stream_utils.h"

#include <gtest/gtest.h>

using namespace storage::experimental::mvlog;

TEST(EntryHeaderToFromIObuf, TestBasic) {
    entry_header orig_h{0, 0, entry_type::record_batch};
    auto buf = entry_header_to_iobuf(orig_h);
    ASSERT_EQ(buf.size_bytes(), packed_entry_header_size);
    auto new_h = entry_header_from_iobuf(std::move(buf));
    ASSERT_EQ(new_h, orig_h);
}

TEST(EntryHeaderToFromIOBuf, TestMax) {
    entry_header orig_h{
      std::numeric_limits<uint32_t>::max(),
      std::numeric_limits<int32_t>::max(),
      entry_type{std::numeric_limits<int8_t>::max()}};
    auto buf = entry_header_to_iobuf(orig_h);
    ASSERT_EQ(buf.size_bytes(), packed_entry_header_size);
    auto new_h = entry_header_from_iobuf(std::move(buf));
    ASSERT_EQ(new_h, orig_h);
}

TEST(EntryHeaderToFromIOBuf, TestMin) {
    entry_header orig_h{
      std::numeric_limits<uint32_t>::min(),
      std::numeric_limits<int32_t>::min(),
      entry_type{std::numeric_limits<int8_t>::min()}};
    auto buf = entry_header_to_iobuf(orig_h);
    ASSERT_EQ(buf.size_bytes(), packed_entry_header_size);
    auto new_h = entry_header_from_iobuf(std::move(buf));
    ASSERT_EQ(new_h, orig_h);
}

TEST(EntryHeaderToFromIOBuf, TestRandom) {
    for (int i = 0; i < 10; i++) {
        entry_header orig_h{
          random_generators::get_int<uint32_t>(),
          random_generators::get_int<int32_t>(),
          entry_type{random_generators::get_int<int8_t>()}};
        auto buf = entry_header_to_iobuf(orig_h);
        ASSERT_EQ(buf.size_bytes(), packed_entry_header_size);
        auto new_h = entry_header_from_iobuf(std::move(buf));
        ASSERT_EQ(new_h, orig_h);
    }
}

TEST(EntryHeaderToFromIObuf, TestDeserializeTooSmall) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    entry_header orig_h{0, 0, entry_type::record_batch};
    auto buf = entry_header_to_iobuf(orig_h);
    ASSERT_EQ(buf.size_bytes(), packed_entry_header_size);

    // For these lower level utilities, callers are expected to pass in
    // appropriately sized buffers.
    buf.pop_back();
    ASSERT_DEATH(
      entry_header_from_iobuf(std::move(buf)), "Expected buf of size 9");
}

TEST(EntryHeaderToFromIObuf, TestUnknownEnum) {
    iobuf fake_header_buf;
    serde::write(fake_header_buf, uint32_t{0});
    serde::write(fake_header_buf, int32_t{0});
    serde::write(fake_header_buf, std::numeric_limits<int8_t>::max());
    ASSERT_EQ(fake_header_buf.size_bytes(), packed_entry_header_size);

    auto header = entry_header_from_iobuf(std::move(fake_header_buf));
    ASSERT_EQ(
      static_cast<int8_t>(header.type), std::numeric_limits<int8_t>::max());
}
