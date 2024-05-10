// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iostream.h"
#include "bytes/random.h"
#include "random/generators.h"
#include "storage/mvlog/entry.h"
#include "storage/mvlog/entry_stream.h"
#include "storage/mvlog/entry_stream_utils.h"

#include <gtest/gtest.h>

using namespace storage::experimental::mvlog;

namespace {
entry_header make_checksummed_header(size_t body_size, entry_type type) {
    auto sz = static_cast<int32_t>(body_size);
    return entry_header(entry_header_crc(sz, type), sz, type);
}
} // namespace

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

TEST(EntryStream, TestShortHeader) {
    iobuf hdr_buf;
    entry_header hdr{0, 0, entry_type::record_batch};
    hdr_buf.append(entry_header_to_iobuf(hdr));
    ASSERT_GT(hdr_buf.size_bytes(), 1);
    for (int i = 1; i < hdr_buf.size_bytes(); i++) {
        auto copy_buf = hdr_buf.copy();
        copy_buf.trim_back(i);
        auto short_stream = make_iobuf_input_stream(std::move(copy_buf));
        entry_stream entries(std::move(short_stream));
        auto entry = entries.next().get();
        ASSERT_TRUE(entry.has_error());
        ASSERT_EQ(entry.error(), errc::short_read);
    }
}

TEST(EntryStream, TestBadEntryHeaderChecksum) {
    iobuf hdr_buf;
    entry_header hdr{0, 0, entry_type::record_batch};
    hdr_buf.append(entry_header_to_iobuf(hdr));
    ASSERT_GT(hdr_buf.size_bytes(), 0);
    auto& last_frag = *hdr_buf.rbegin();
    ASSERT_GT(last_frag.size(), 0);
    last_frag.get_write()[last_frag.size() - 1] += 1;

    auto corrupted_stream = make_iobuf_input_stream(std::move(hdr_buf));
    entry_stream entries(std::move(corrupted_stream));
    auto entry = entries.next().get();
    ASSERT_TRUE(entry.has_error());
    ASSERT_EQ(errc::checksum_mismatch, entry.error());
}

TEST(EntryStream, TestShortBody) {
    constexpr auto expected_body_size = 10;
    iobuf hdr_buf;
    auto hdr = make_checksummed_header(
      expected_body_size, entry_type::record_batch);
    hdr_buf.append(entry_header_to_iobuf(hdr));
    for (int i = 0; i < expected_body_size; i++) {
        auto copy_buf = hdr_buf.copy();
        copy_buf.append(random_generators::make_iobuf(i));
        auto short_stream = make_iobuf_input_stream(std::move(copy_buf));
        entry_stream entries(std::move(short_stream));
        auto entry = entries.next().get();
        ASSERT_TRUE(entry.has_error());
        ASSERT_EQ(entry.error(), errc::short_read);
    }
}

TEST(EntryStream, TestRandom) {
    iobuf entries_buf;
    constexpr auto max_body_size = 100;
    constexpr auto num_entries = 10;
    std::vector<size_t> body_sizes;
    body_sizes.reserve(num_entries);
    for (int i = 0; i < num_entries; i++) {
        auto body_size = random_generators::get_int(max_body_size);
        body_sizes.emplace_back(body_size);

        iobuf hdr_buf;
        auto hdr = make_checksummed_header(body_size, entry_type::record_batch);
        entries_buf.append(entry_header_to_iobuf(hdr));
        entries_buf.append(random_generators::make_iobuf(body_size));
    }
    auto istream = make_iobuf_input_stream(std::move(entries_buf));
    entry_stream entries(std::move(istream));
    for (int i = 0; i < num_entries; i++) {
        auto entry_res = entries.next().get();
        ASSERT_FALSE(entry_res.has_error()) << entry_res.error();
        ASSERT_TRUE(entry_res.value().has_value()) << "Expected an entry";

        // Validate the header.
        const auto& entry = entry_res.value().value();
        ASSERT_EQ(body_sizes[i], entry.hdr.body_size);
        ASSERT_EQ(entry_type::record_batch, entry.hdr.type);
        ASSERT_EQ(
          entry_header_crc(entry.hdr.body_size, entry_type::record_batch),
          entry.hdr.header_crc);

        // Validate the body.
        ASSERT_EQ(body_sizes[i], entry.body.size_bytes());
    }
}
