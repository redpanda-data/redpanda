// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "base/seastarx.h"
#include "bytes/ioarray.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "lsm/block/contents.h"
#include "lsm/block/handle.h"
#include "lsm/block/reader.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/keys.h"
#include "lsm/io/disk_persistence.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/io/persistence.h"
#include "lsm/sst/builder.h"
#include "lsm/sst/footer.h"
#include "lsm/sst/reader.h"
#include "utils/uuid.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>

using lsm::internal::operator""_key;
using persistence_factory
  = std::function<ss::future<std::unique_ptr<lsm::io::data_persistence>>()>;

namespace {

// Serves a fixed in-memory buffer as a random-access file, so a deliberately
// corrupted copy of an SST can be opened.
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

private:
    iobuf _data;
};

} // namespace

class SSTTest : public ::testing::TestWithParam<persistence_factory> {
    void SetUp() override { _persistence = GetParam()().get(); }
    void TearDown() override {
        if (_persistence) {
            _persistence->close().get();
        }
    }

protected:
    std::unique_ptr<lsm::io::data_persistence> _persistence;
};

TEST_P(SSTTest, CanCreateAndReadFile) {
    size_t file_size = 0;
    {
        auto file = _persistence->open_sequential_writer({}).get();
        lsm::sst::builder builder(std::move(file), {});
        builder.add("abc"_key, iobuf::from("value1")).get();
        builder.add("abcd"_key, iobuf::from("value2")).get();
        builder.add("abc123"_key, iobuf::from("value3")).get();
        builder.finish().get();
        builder.close().get();
        file_size = builder.file_size();
    }
    auto file = _persistence->open_random_access_reader({}, file_size).get();
    auto reader = lsm::sst::reader::open(
                    std::move(*file),
                    lsm::internal::file_id{0},
                    file_size,
                    ss::make_lw_shared<lsm::sst::block_cache>(
                      1_MiB, ss::make_lw_shared<lsm::probe>()))
                    .get();
    auto iter = reader.create_iterator();
    iter->seek_to_first().get();
    ASSERT_TRUE(iter->valid());
    EXPECT_EQ(iter->key(), "abc"_key);
    iter->next().get();
    ASSERT_TRUE(iter->valid());
    EXPECT_EQ(iter->key(), "abcd"_key);
    iter->next().get();
    ASSERT_TRUE(iter->valid());
    EXPECT_EQ(iter->key(), "abc123"_key);
    iter->next().get();
    ASSERT_FALSE(iter->valid());
    reader.close().get();
}

// Corrupting a byte in the filter block must be caught by the streaming CRC
// validation in reader::open, since the lazy filter reader no longer holds (and
// re-checks) the whole filter in memory. Proves integrity is preserved.
TEST(SSTCorruption, FilterCrcMismatchCaughtOnOpen) {
    auto persistence = lsm::io::make_memory_data_persistence();
    auto persistence_cleanup = ss::defer([&] { persistence->close().get(); });

    size_t file_size = 0;
    {
        auto file = persistence->open_sequential_writer({}).get();
        lsm::sst::builder builder(std::move(file), {});
        builder.add("abc"_key, iobuf::from("value1")).get();
        builder.add("abcd"_key, iobuf::from("value2")).get();
        builder.add("abc123"_key, iobuf::from("value3")).get();
        builder.finish().get();
        builder.close().get();
        file_size = builder.file_size();
    }

    auto rdr_opt = persistence->open_random_access_reader({}, file_size).get();
    ASSERT_TRUE(bool(rdr_opt));
    auto& rdr = *rdr_opt;
    auto rdr_cleanup = ss::defer([&] { rdr->close().get(); });

    // Locate the filter block: footer -> metaindex -> "filter.*" handle.
    auto footer_bytes = rdr
                          ->read(
                            file_size - lsm::sst::footer::encoded_length,
                            lsm::sst::footer::encoded_length)
                          .get();
    auto footer = lsm::sst::footer::from_iobuf(footer_bytes.as_iobuf());
    auto meta
      = lsm::block::contents::read(rdr.get(), footer.metaindex_handle).get();
    lsm::block::reader metaindex(std::move(meta));
    auto it = metaindex.create_iterator();
    auto filter_key = lsm::internal::key::encode(
      {.key = lsm::user_key_view("filter.RedpandaBloomV0")});
    it->seek(filter_key).get();
    ASSERT_TRUE(it->valid());
    auto filter_handle = lsm::block::handle::from_iobuf(it->value());

    // Read the whole file, flip one byte inside the filter block, and reopen.
    auto whole = rdr->read(0, file_size).get();
    iobuf_parser p(whole.as_iobuf());
    auto bytes = p.read_string_unsafe(p.bytes_left());
    ASSERT_LT(filter_handle.offset, bytes.size());
    bytes[filter_handle.offset] = static_cast<char>(
      bytes[filter_handle.offset] ^ 0xFF);

    auto corrupt = std::make_unique<in_memory_reader>(iobuf::from(bytes));
    EXPECT_THROW(
      lsm::sst::reader::open(
        std::move(corrupt),
        lsm::internal::file_id{0},
        file_size,
        ss::make_lw_shared<lsm::sst::block_cache>(
          1_MiB, ss::make_lw_shared<lsm::probe>()))
        .get(),
      lsm::corruption_exception);
}

INSTANTIATE_TEST_SUITE_P(
  SSTTestSuite,
  SSTTest,
  testing::Values(
    [] { return ss::as_ready_future(lsm::io::make_memory_data_persistence()); },
    [] {
        std::filesystem::path tmpdir = std::getenv("TEST_TMPDIR");
        // Ensure each testcase has it's own directory.
        auto subdir = ss::sstring(uuid_t::create());
        return lsm::io::open_disk_data_persistence(
          tmpdir / std::string_view(subdir));
    }),
  [](const testing::TestParamInfo<persistence_factory>& info) {
      switch (info.index) {
      case 0:
          return "memory";
      case 1:
          return "disk";
      default:
          return "unknown";
      }
  });
