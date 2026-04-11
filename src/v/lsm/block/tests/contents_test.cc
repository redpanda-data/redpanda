// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "base/seastarx.h"
#include "lsm/block/contents.h"
#include "lsm/io/memory_persistence.h"
#include "test_utils/test.h"

#include <seastar/core/file.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <limits>

TEST_CORO(Contents, StringView) {
    auto persistence = lsm::io::make_memory_data_persistence();
    iobuf b;
    {
        for (char c : {'a', 'b', 'c'}) {
            b.append(iobuf::from(std::string(128_KiB, c)));
        }
        auto file = co_await persistence->open_sequential_writer({});
        co_await file->append(b.share());
        co_await file->close();
    }
    auto file = co_await persistence->open_random_access_reader({});
    ASSERT_TRUE_CORO(bool(file));
    for (auto offset : std::to_array<size_t>({0, 1, 2, 3, 4, 5, 10, 64_KiB})) {
        auto buf = b.share(offset, b.size_bytes() - offset);
        auto contents = co_await lsm::block::contents::read(
          file->get(),
          lsm::block::handle{.offset = offset, .size = buf.size_bytes()});
        std::vector<std::pair<size_t, size_t>> testcases{
          // clang-format off
          {0, 1},
          {128_KiB, 1},
          {256_KiB, 1},
          {256_KiB - 1, 1},
          {128_KiB - 1, 1},
          {256_KiB - 1, 2},
          {128_KiB - 1, 2},
          {256_KiB - 2, 4},
          {128_KiB - 2, 4},
          {1, 128_KiB},
          {2, 128_KiB},
          {2, 128_KiB-1},
          // clang-format on
        };
        for (size_t i = 0; i < testcases.size(); ++i) {
            const auto& [pos, len] = testcases[i];
            ss::sstring expected;
            for (const auto& frag : buf.share(pos, len)) {
                expected.append(frag.get(), frag.size());
            }
            auto actual = contents->read_string(pos, len);
            EXPECT_EQ(expected, actual)
              << "offset: " << offset << ", testcase: " << i << ", pos: " << pos
              << ", len: " << len << ", expected: " << expected;
            EXPECT_EQ(expected, ss::sstring(actual))
              << "offset: " << offset << ", testcase: " << i << ", pos: " << pos
              << ", len: " << len << ", expected: " << expected;
        }
    }
    co_await (*file)->close();
    co_await persistence->close();
}

TEST_CORO(Contents, IobufShare) {
    auto persistence = lsm::io::make_memory_data_persistence();
    iobuf b;
    {
        for (char c : {'a', 'b', 'c'}) {
            b.append(iobuf::from(std::string(128_KiB, c)));
        }
        auto file = co_await persistence->open_sequential_writer({});
        co_await file->append(b.share());
        co_await file->close();
    }
    auto file = co_await persistence->open_random_access_reader({});
    ASSERT_TRUE_CORO(bool(file));
    for (auto offset : std::to_array<size_t>({0, 1, 2, 3, 4, 5, 10, 64_KiB})) {
        auto buf = b.share(offset, b.size_bytes() - offset);
        auto contents = co_await lsm::block::contents::read(
          file->get(),
          lsm::block::handle{.offset = offset, .size = buf.size_bytes()});
        std::vector<std::pair<size_t, size_t>> testcases{
          // clang-format off
          {0, 1},
          {1, 2},
          {0, buf.size_bytes()},
          {buf.size_bytes(), 0},
          {buf.size_bytes() - 1, 1},
          {0, buf.size_bytes() - 1},
          {1, buf.size_bytes() - 1},
          {1, buf.size_bytes() - 2},
          {128_KiB, 128_KiB},
          {128_KiB - 1, 128_KiB + 1},
          {128_KiB + 1, 128_KiB - 1},
          {128_KiB - 1, 128_KiB},
          {128_KiB + 1, 128_KiB},
          // clang-format on
        };
        for (size_t i = 0; i < testcases.size(); ++i) {
            const auto& [pos, len] = testcases[i];
            auto expected = buf.share(pos, len);
            auto actual = contents->share(pos, len);
            EXPECT_EQ(expected, actual)
              << "offset: " << offset << ", testcase: " << i << ", pos: " << pos
              << ", len: " << len << ", expected: " << expected
              << ", actual: " << actual;
        }
    }
    co_await (*file)->close();
    co_await persistence->close();
}

TEST(Contents, ReadFixed32) {
    iobuf b;
    std::vector<uint32_t> values{
      0,
      1,
      4,
      1209381,
      1232348239,
      std::numeric_limits<uint32_t>::max(),
      std::numeric_limits<uint32_t>::max() / 2};
    for (auto v : values) {
        b.append(std::bit_cast<std::array<uint8_t, sizeof(uint32_t)>>(v));
    }
    auto contents = lsm::block::contents::copy_from(b);
    for (size_t i = 0; i < values.size(); ++i) {
        auto expected = values[i];
        auto actual = contents->read_fixed32(i * sizeof(uint32_t));
        EXPECT_EQ(expected, actual);
    }
}
