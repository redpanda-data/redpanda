// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "base/seastarx.h"
#include "lsm/core/internal/keys.h"
#include "lsm/io/disk_persistence.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/sst/builder.h"
#include "lsm/sst/reader.h"
#include "utils/uuid.h"

#include <seastar/core/file.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

using lsm::internal::operator""_key;
using persistence_factory
  = std::function<ss::future<std::unique_ptr<lsm::io::data_persistence>>()>;

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
    auto file = _persistence->open_random_access_reader({}).get();
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
