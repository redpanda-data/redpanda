// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2026 Redpanda Data, Inc.

#include "base/seastarx.h"
#include "bytes/ioarray.h"
#include "bytes/iobuf.h"
#include "hashing/crc32c.h"
#include "lsm/block/filter.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/keys.h"
#include "lsm/io/persistence.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace {

// Serves a fixed in-memory buffer as a random-access file, so the lazy
// filter_reader can read its regions without a real cache/cloud backend.
class in_memory_reader final : public lsm::io::random_access_file_reader {
public:
    explicit in_memory_reader(iobuf data)
      : _data(std::move(data)) {}

    ss::future<ioarray> read(size_t offset, size_t n) override {
        n = std::min(n, _data.size_bytes() - offset);
        co_return ioarray::copy_from(_data.share(offset, n));
    }
    ss::future<> close() override { return ss::now(); }
    fmt::iterator format_to(fmt::iterator it) const override {
        return fmt::format_to(it, "in_memory_reader");
    }

    // Flip every bit of the byte at `offset`, simulating on-disk corruption
    // that occurs after the filter has been opened.
    void corrupt_byte(size_t offset) {
        std::string buf(_data.size_bytes(), '\0');
        auto* p = buf.data();
        iobuf::iterator_consumer(_data.cbegin(), _data.cend())
          .consume(buf.size(), [&p](const char* src, size_t sz) {
              std::memcpy(p, src, sz);
              p += sz;
              return ss::stop_iteration::no;
          });
        buf[offset] = static_cast<char>(~static_cast<uint8_t>(buf[offset]));
        iobuf out;
        out.append(buf.data(), buf.size());
        _data = std::move(out);
    }

private:
    iobuf _data;
};

using key_vector = std::vector<std::string_view>;
using keys_by_block = std::map<uint64_t, key_vector>;

// A filter_reader plus the file backing it, kept together so the file outlives
// the reader (which holds a non-owning pointer to it).
struct filter_under_test {
    std::unique_ptr<in_memory_reader> file;
    lsm::block::filter_reader reader;

    ss::future<bool>
    key_may_match(uint64_t block_offset, lsm::internal::key_view key) {
        return reader.key_may_match(block_offset, key);
    }
};

ss::future<filter_under_test> make_filter(const keys_by_block& keys) {
    lsm::block::filter_builder builder({});
    for (const auto& [block, keys_in_block] : keys) {
        builder.start_block(block);
        for (const auto& key : keys_in_block) {
            builder.add_key(
              lsm::internal::key::encode({.key = lsm::user_key_view(key)}));
        }
    }
    iobuf content = builder.finish();
    auto size = content.size_bytes();
    // Append the block trailer the SST builder writes: an (uncompressed)
    // compression-type byte followed by a CRC over the content plus that byte.
    // filter_reader::open validates this and derives the per-region CRCs.
    content.append(std::to_array<uint8_t>({0})); // compression_type::none
    crc::crc32c crc;
    crc_extend_iobuf(crc, content);
    content.append(
      std::bit_cast<std::array<uint8_t, sizeof(crc.value())>>(crc.value()));
    auto file = std::make_unique<in_memory_reader>(std::move(content));
    auto reader = co_await lsm::block::filter_reader::open(
      file.get(), /*content_offset=*/0, size);
    co_return filter_under_test{std::move(file), std::move(reader)};
}

using lsm::internal::operator""_key;

} // namespace

class FilterTest : public seastar_test {};

TEST_F(FilterTest, Empty) {
    auto filter = make_filter({}).get();
    EXPECT_TRUE(filter.key_may_match(0, "foo"_key).get());
    EXPECT_TRUE(filter.key_may_match(100000, "foo"_key).get());
}

TEST_F(FilterTest, Bloom) {
    auto reader = make_filter({
                                {0, {"hello", "world"}},
                              })
                    .get();
    EXPECT_TRUE(reader.key_may_match(0, "hello"_key).get());
    EXPECT_TRUE(reader.key_may_match(0, "world"_key).get());
    EXPECT_FALSE(reader.key_may_match(0, "x"_key).get());
    EXPECT_FALSE(reader.key_may_match(0, "foo"_key).get());
}

TEST_F(FilterTest, SingleBlock) {
    auto reader = make_filter({
                                {100, {"foo", "bar", "box"}},
                                {200, {"box"}},
                                {300, {"hello"}},
                              })
                    .get();
    EXPECT_TRUE(reader.key_may_match(100, "foo"_key).get());
    EXPECT_TRUE(reader.key_may_match(100, "bar"_key).get());
    EXPECT_TRUE(reader.key_may_match(100, "box"_key).get());
    EXPECT_TRUE(reader.key_may_match(100, "hello"_key).get());
    EXPECT_TRUE(reader.key_may_match(100, "foo"_key).get());
    EXPECT_FALSE(reader.key_may_match(100, "missing"_key).get());
    EXPECT_FALSE(reader.key_may_match(100, "other"_key).get());
}

TEST_F(FilterTest, MultipleBlocks) {
    auto reader = make_filter({
                                {0, {"foo"}},
                                {2000, {"bar"}},
                                {3100, {"box"}},
                                {9000, {"box", "hello"}},
                              })
                    .get();

    // Check first filter
    EXPECT_TRUE(reader.key_may_match(0, "foo"_key).get());
    EXPECT_TRUE(reader.key_may_match(2000, "bar"_key).get());
    EXPECT_FALSE(reader.key_may_match(0, "box"_key).get());
    EXPECT_FALSE(reader.key_may_match(0, "hello"_key).get());
    EXPECT_FALSE(reader.key_may_match(0, "world"_key).get());

    // Check second filter
    EXPECT_TRUE(reader.key_may_match(3100, "box"_key).get());
    EXPECT_FALSE(reader.key_may_match(3100, "foo"_key).get());
    EXPECT_FALSE(reader.key_may_match(3100, "bar"_key).get());
    EXPECT_FALSE(reader.key_may_match(3100, "hello"_key).get());
    EXPECT_FALSE(reader.key_may_match(3100, "world"_key).get());

    // Check third filter (empty)
    EXPECT_FALSE(reader.key_may_match(4100, "foo"_key).get());
    EXPECT_FALSE(reader.key_may_match(4100, "bar"_key).get());
    EXPECT_FALSE(reader.key_may_match(4100, "box"_key).get());
    EXPECT_FALSE(reader.key_may_match(4100, "hello"_key).get());

    // Check last filter
    EXPECT_TRUE(reader.key_may_match(9000, "box"_key).get());
    EXPECT_TRUE(reader.key_may_match(9000, "hello"_key).get());
    EXPECT_FALSE(reader.key_may_match(9000, "foo"_key).get());
    EXPECT_FALSE(reader.key_may_match(9000, "bar"_key).get());
}

// A bit that rots in a region's bloom data after open must be caught on the
// lazy read via the per-region CRC, not silently returned as a false negative.
TEST_F(FilterTest, DetectsRegionCorruptionAfterOpen) {
    auto filter = make_filter({
                                {0, {"hello", "world"}},
                              })
                    .get();
    // A clean read succeeds and the open-time whole-block CRC validated.
    EXPECT_TRUE(filter.key_may_match(0, "hello"_key).get());

    // Corrupt the first byte of region 0's bloom data on "disk" after open.
    filter.file->corrupt_byte(0);

    EXPECT_THROW(
      filter.key_may_match(0, "hello"_key).get(), lsm::corruption_exception);
}
