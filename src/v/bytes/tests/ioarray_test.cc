/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/seastarx.h"
#include "bytes/ioarray.h"

#include <seastar/core/file.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <random>

// NOLINTBEGIN(*magic-numbers*)

namespace {

// Build a string where byte i = (base + i) & 0xFF.
std::string make_pattern(size_t size, size_t base = 0) {
    std::string s(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        s[i] = static_cast<char>((base + i) & 0xFF);
    }
    return s;
}

// Fill an ioarray with the default pattern (byte i = i & 0xFF).
ioarray make_filled(size_t size) {
    return ioarray::copy_from(iobuf::from(make_pattern(size)));
}

// Check that arr's contents equal the pattern starting at base.
void verify_pattern(const ioarray& arr, size_t base) {
    auto expected = make_pattern(arr.size(), base);
    EXPECT_TRUE(std::ranges::equal(arr.as_range(), expected));
}

} // namespace

TEST(IOArray, StringView) {
    iobuf b;
    for (char c : {'a', 'b', 'c'}) {
        b.append_str(std::string(128_KiB, c));
    }
    for (auto offset : std::to_array<size_t>({0, 1, 2, 3, 4, 5, 10, 64_KiB})) {
        auto buf = b.share(offset, b.size_bytes() - offset);
        auto contents = ioarray::copy_from(b).share(
          offset, b.size_bytes() - offset);
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
            auto actual = contents.read_string(pos, len);
            EXPECT_EQ(expected, actual)
              << "offset: " << offset << ", testcase: " << i << ", pos: " << pos
              << ", len: " << len << ", expected: " << expected;
            EXPECT_EQ(expected, ss::sstring(actual))
              << "offset: " << offset << ", testcase: " << i << ", pos: " << pos
              << ", len: " << len << ", expected: " << expected;
        }
    }
}

TEST(IOArray, ShareToIOBuf) {
    iobuf b;
    for (char c : {'a', 'b', 'c'}) {
        b.append_str(std::string(128_KiB, c));
    }
    for (auto offset : std::to_array<size_t>({0, 1, 2, 3, 4, 5, 10, 64_KiB})) {
        auto buf = b.share(offset, b.size_bytes() - offset);
        auto contents = ioarray::copy_from(buf);
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
            auto actual = contents.share(pos, len).as_iobuf();
            EXPECT_EQ(expected, actual)
              << "offset: " << offset << ", testcase: " << i << ", pos: " << pos
              << ", len: " << len << ", expected: " << expected
              << ", actual: " << actual;
        }
    }
}

TEST(IOArray, ScatterGatherIO) {
    iobuf b;
    for (char c : {'a', 'b', 'c'}) {
        b.append_str(std::string(128_KiB, c));
    }
    for (auto offset : std::to_array<size_t>({0, 1, 2, 3, 4, 5, 10, 64_KiB})) {
        auto buf = b.share(offset, b.size_bytes() - offset);
        auto contents = ioarray::copy_from(buf);
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
        for (auto [pos, len] : testcases) {
            auto expected = buf.share(pos, len);
            auto shared = contents.share(pos, len);
            auto iov = shared.as_iovec();
            iobuf actual;
            for (auto v : iov) {
                actual.append(static_cast<char*>(v.iov_base), v.iov_len);
            }
            EXPECT_EQ(expected, actual)
              << "offset: " << offset << ", pos: " << pos << ", len: " << len
              << ", expected: " << expected << ", actual: " << actual;
        }
    }
}

TEST(IOArray, Range) {
    auto b = ioarray::copy_from(iobuf::from("0123456789abcdefg"));
    std::string s;
    std::ranges::copy(b.as_range(), std::back_inserter(s));
    EXPECT_EQ("0123456789abcdefg", s);
}

TEST(IOArray, TrimBack) {
    auto b = ioarray::copy_from(iobuf::from("0123456789abcdefg"));
    b.trim_back(3);
    std::string s;
    std::ranges::copy(b.as_range(), std::back_inserter(s));
    EXPECT_EQ("0123456789abcd", s);

    std::string large_string(150_KiB, 'a');
    b = ioarray::copy_from(iobuf::from(large_string));
    b.trim_back(50_KiB);
    large_string = large_string.substr(0, 100_KiB);
    s.clear();
    std::ranges::copy(b.as_range(), std::back_inserter(s));
    EXPECT_EQ(large_string.size(), s.size());
}

TEST(IOArray, IndexingWithOffset) {
    iobuf b;
    for (size_t i = 0; i < 300_KiB; ++i) {
        char c = static_cast<char>(i);
        b.append(&c, 1);
    }

    for (auto offset : std::to_array<size_t>(
           {0, 1, 10, 100, 128_KiB - 1, 128_KiB, 128_KiB + 1, 200_KiB})) {
        auto contents = ioarray::copy_from(b).share(
          offset, b.size_bytes() - offset);

        for (size_t i = 0; i < std::min<size_t>(1000, contents.size()); ++i) {
            char expected = static_cast<char>(offset + i);
            char actual = contents[i];
            EXPECT_EQ(expected, actual) << "offset: " << offset << ", i: " << i;
        }

        if (contents.size() > 0) {
            contents[0] = 'X';
            EXPECT_EQ('X', contents[0]) << "offset: " << offset;
        }

        if (contents.size() > 128_KiB) {
            contents[128_KiB - 1] = 'Y';
            EXPECT_EQ('Y', contents[128_KiB - 1]) << "offset: " << offset;

            contents[128_KiB] = 'Z';
            EXPECT_EQ('Z', contents[128_KiB]) << "offset: " << offset;
        }
    }
}

TEST(IOArray, TrimBackWithOffset) {
    iobuf b;
    std::string pattern;
    for (size_t i = 0; i < 300_KiB; ++i) {
        char c = 'a' + (i % 26);
        b.append(&c, 1);
        pattern.push_back(c);
    }

    for (auto offset :
         std::to_array<size_t>({1, 10, 128_KiB - 1, 128_KiB, 128_KiB + 1})) {
        auto contents = ioarray::copy_from(b).share(
          offset, b.size_bytes() - offset);
        size_t original_size = contents.size();

        for (auto trim : std::to_array<size_t>({1, 10, 100, 1000})) {
            if (trim > original_size) continue;

            auto test_copy = ioarray::copy_from(b).share(
              offset, b.size_bytes() - offset);
            test_copy.trim_back(trim);

            EXPECT_EQ(original_size - trim, test_copy.size())
              << "offset: " << offset << ", trim: " << trim;

            std::string result;
            std::ranges::copy(test_copy.as_range(), std::back_inserter(result));
            std::string expected = pattern.substr(offset, original_size - trim);
            EXPECT_EQ(expected, result)
              << "offset: " << offset << ", trim: " << trim;
        }
    }
}

TEST(IOArray, IOVecWithOffset) {
    iobuf b;
    std::string pattern;
    for (size_t i = 0; i < 300_KiB; ++i) {
        char c = 'a' + (i % 26);
        b.append(&c, 1);
        pattern.push_back(c);
    }

    for (auto offset : std::to_array<size_t>(
           {0, 1, 10, 100, 128_KiB - 1, 128_KiB, 128_KiB + 1, 200_KiB})) {
        auto contents = ioarray::copy_from(b).share(
          offset, b.size_bytes() - offset);
        auto iov = contents.as_iovec();

        std::string result;
        size_t total_len = 0;
        for (const auto& v : iov) {
            result.append(static_cast<char*>(v.iov_base), v.iov_len);
            total_len += v.iov_len;
        }

        EXPECT_EQ(contents.size(), total_len) << "offset: " << offset;

        std::string expected = pattern.substr(offset);
        EXPECT_EQ(expected, result) << "offset: " << offset;
    }
}

TEST(IOArray, EdgeCases) {
    ioarray empty;
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ(0, empty.size());

    auto single = ioarray::copy_from(iobuf::from("X"));
    EXPECT_FALSE(single.empty());
    EXPECT_EQ(1, single.size());
    EXPECT_EQ('X', single[0]);

    auto empty_share = single.share(0, 0);
    EXPECT_TRUE(empty_share.empty());

    auto will_be_empty = ioarray::copy_from(iobuf::from("abc"));
    will_be_empty.trim_back(3);
    EXPECT_TRUE(will_be_empty.empty());
    EXPECT_EQ(0, will_be_empty.size());
}

TEST(IOArray, AlignedAllocation) {
    auto contents = ioarray::aligned(4096, 256_KiB);
    EXPECT_EQ(256_KiB, contents.size());

    for (size_t i = 0; i < contents.size(); i += 1024) {
        contents[i] = static_cast<char>(i & 0xFF);
    }

    for (size_t i = 0; i < contents.size(); i += 1024) {
        EXPECT_EQ(static_cast<char>(i & 0xFF), contents[i]);
    }
}

TEST(IOArray, StringViewComparison) {
    auto arr = ioarray::copy_from(iobuf::from("hello world"));

    auto view = arr.read_string(0, 5);
    EXPECT_EQ(view, std::string_view("hello"));
    EXPECT_NE(view, std::string_view("world"));

    auto view1 = arr.read_string(0, 5);
    auto view2 = arr.read_string(6, 5);
    EXPECT_LT(view1, std::string_view("world"));
    EXPECT_GT(view2, std::string_view("hello"));

    iobuf large_buf;
    large_buf.append_str(std::string(128_KiB - 2, 'a'));
    large_buf.append_str("xyz");
    large_buf.append_str(std::string(100, 'b'));

    auto large_arr = ioarray::copy_from(large_buf);
    auto boundary_view = large_arr.read_string(128_KiB - 2, 4);
    EXPECT_EQ(boundary_view, std::string_view("xyzb"));
    EXPECT_LT(boundary_view, std::string_view("z"));
    EXPECT_LT(std::string_view("b"), boundary_view);
    EXPECT_GT(boundary_view, std::string_view("a"));
}

TEST(IOArray, MoveSemantics) {
    auto b = ioarray::copy_from(iobuf::from("test data"));
    size_t original_size = b.size();

    auto moved = std::move(b);
    EXPECT_EQ(original_size, moved.size());

    auto b2 = ioarray::copy_from(iobuf::from("other data"));
    b2 = std::move(moved);
    EXPECT_EQ(original_size, b2.size());

    std::string result;
    std::ranges::copy(b2.as_range(), std::back_inserter(result));
    EXPECT_EQ("test data", result);
}

TEST(IOArray, ReadFixed32) {
    iobuf b;
    for (size_t i = 0; i < 200_KiB; ++i) {
        char c = static_cast<char>(i & 0xFF);
        b.append(&c, 1);
    }
    auto contents = ioarray::copy_from(b);

    EXPECT_EQ(0x03020100, contents.read_fixed32(0));
    EXPECT_EQ(0x04030201, contents.read_fixed32(1));

    EXPECT_EQ(0x03020100, contents.read_fixed32(128_KiB));
    EXPECT_EQ(0x04030201, contents.read_fixed32(128_KiB + 1));
    EXPECT_EQ(0x020100ff, contents.read_fixed32(128_KiB - 1));

    contents = ioarray::copy_from(b).share(100, b.size_bytes() - 100);
    EXPECT_EQ(0x67666564, contents.read_fixed32(0));
    EXPECT_EQ(0x68676665, contents.read_fixed32(1));

    auto contents2 = ioarray::copy_from(b).share(
      128_KiB, b.size_bytes() - 128_KiB);
    EXPECT_EQ(0x03020100, contents2.read_fixed32(0));
    EXPECT_EQ(0x04030201, contents2.read_fixed32(1));
}

TEST(IOArray, FromSizedBuffers) {
    // Test empty input
    {
        std::vector<ss::temporary_buffer<char>> bufs;
        auto arr = ioarray::from_sized_buffers(bufs);
        EXPECT_TRUE(arr.empty());
        EXPECT_EQ(0, arr.size());
    }

    // Test single buffer (can be any size since it's the last one)
    {
        std::vector<ss::temporary_buffer<char>> bufs;
        bufs.emplace_back(1024);
        for (size_t i = 0; i < 1024; ++i) {
            bufs[0].get_write()[i] = 'A' + (i % 26);
        }

        auto arr = ioarray::from_sized_buffers(bufs);
        EXPECT_EQ(1024, arr.size());
        for (size_t i = 0; i < 1024; ++i) {
            EXPECT_EQ('A' + (i % 26), arr[i]);
        }
    }

    // Test multiple buffers with correct sizes
    {
        std::vector<ss::temporary_buffer<char>> bufs;
        // Two full chunks
        bufs.emplace_back(128_KiB);
        bufs.emplace_back(128_KiB);
        // Last chunk can be smaller
        bufs.emplace_back(64_KiB);

        // Fill with pattern
        for (size_t buf_idx = 0; buf_idx < bufs.size(); ++buf_idx) {
            for (size_t i = 0; i < bufs[buf_idx].size(); ++i) {
                bufs[buf_idx].get_write()[i] = static_cast<char>(
                  (buf_idx * 128_KiB + i) & 0xFF);
            }
        }

        auto arr = ioarray::from_sized_buffers(bufs);
        EXPECT_EQ(128_KiB + 128_KiB + 64_KiB, arr.size());

        // Verify data is accessible
        for (size_t i = 0; i < arr.size(); ++i) {
            EXPECT_EQ(static_cast<char>(i & 0xFF), arr[i])
              << "Mismatch at index " << i;
        }
    }

    // Test that buffers are shared (not copied)
    {
        std::vector<ss::temporary_buffer<char>> bufs;
        bufs.emplace_back(128_KiB);
        bufs.emplace_back(100);

        // Fill with initial pattern
        std::memset(bufs[0].get_write(), 'X', 128_KiB);
        std::memset(bufs[1].get_write(), 'Y', 100);

        auto arr = ioarray::from_sized_buffers(bufs);

        // Modify original buffer
        bufs[0].get_write()[0] = 'Z';

        // Verify array sees the change (proving sharing)
        EXPECT_EQ('Z', arr[0]);
        EXPECT_EQ('X', arr[1]);
        EXPECT_EQ('Y', arr[128_KiB]);
    }

    // Test conversion to iobuf
    {
        std::vector<ss::temporary_buffer<char>> bufs;
        bufs.emplace_back(128_KiB);
        bufs.emplace_back(50_KiB);

        for (size_t buf_idx = 0; buf_idx < bufs.size(); ++buf_idx) {
            for (size_t i = 0; i < bufs[buf_idx].size(); ++i) {
                bufs[buf_idx].get_write()[i] = static_cast<char>(
                  (buf_idx * 128_KiB + i) & 0xFF);
            }
        }

        auto arr = ioarray::from_sized_buffers(bufs);

        EXPECT_EQ(128_KiB + 50_KiB, arr.size());

        // Verify contents
        char expected = 0;
        for (char c : arr.as_range()) {
            EXPECT_EQ(expected++, c);
        }
    }

    // Test invalid case: middle buffer not max_chunk_size
    {
        std::vector<ss::temporary_buffer<char>> bufs;
        bufs.emplace_back(128_KiB);
        bufs.emplace_back(64_KiB); // Wrong size!
        bufs.emplace_back(100);

        EXPECT_THROW(
          { auto arr = ioarray::from_sized_buffers(bufs); },
          std::invalid_argument);
    }
}

// -- concat tests --

TEST(IOArray, ConcatEmpty) {
    // Both empty
    auto r1 = ioarray::concat(ioarray{}, ioarray{});
    EXPECT_TRUE(r1.empty());

    // Empty + non-empty
    auto r2 = ioarray::concat(ioarray{}, make_filled(1000));
    EXPECT_EQ(1000, r2.size());
    verify_pattern(r2, 0);

    // Non-empty + empty
    auto r3 = ioarray::concat(make_filled(128_KiB), ioarray{});
    EXPECT_EQ(128_KiB, r3.size());
    verify_pattern(r3, 0);
}

TEST(IOArray, ConcatFastPath) {
    // Fast path: a ends on a chunk boundary, b has offset 0.
    struct testcase {
        size_t a_size;
        size_t b_size;
    };
    for (auto [a_size, b_size] : std::to_array<testcase>({
           {.a_size = 128_KiB, .b_size = 50_KiB},
           {.a_size = 128_KiB, .b_size = 128_KiB},
           {.a_size = 3 * 128_KiB, .b_size = 2 * 128_KiB + 64_KiB},
         })) {
        auto result = ioarray::concat(
          make_filled(a_size),
          ioarray::copy_from(iobuf::from(make_pattern(b_size, a_size))));
        verify_pattern(result, 0);
    }

    // Verify read_fixed32 across the chunk boundary
    auto result = ioarray::concat(
      make_filled(128_KiB),
      ioarray::copy_from(iobuf::from(make_pattern(128_KiB, 128_KiB))));
    auto byte = [](size_t i) -> uint32_t { return i & 0xFF; };
    uint32_t expected = byte(128_KiB - 2) | (byte(128_KiB - 1) << 8u)
                        | (byte(128_KiB) << 16u) | (byte(128_KiB + 1) << 24u);
    EXPECT_EQ(expected, result.read_fixed32(128_KiB - 2));
}

TEST(IOArray, ConcatFastPathWithNonZeroOffset) {
    // Fast path with a._offset != 0: offset+size = 128_KiB (chunk boundary).
    auto full = make_filled(256_KiB);
    auto a = full.share(100, 128_KiB - 100);
    auto b = ioarray::copy_from(iobuf::from(make_pattern(50_KiB, 128_KiB)));
    auto result = ioarray::concat(std::move(a), std::move(b));

    auto expected = make_pattern(128_KiB - 100, 100)
                    + make_pattern(50_KiB, 128_KiB);
    EXPECT_TRUE(std::ranges::equal(result.as_range(), expected));

    // read_fixed32 across the a/b boundary
    size_t boundary = 128_KiB - 100;
    auto byte = [](size_t i) -> uint32_t { return i & 0xFF; };
    uint32_t exp32 = byte(100 + boundary - 2) | (byte(100 + boundary - 1) << 8u)
                     | (byte(128_KiB) << 16u) | (byte(128_KiB + 1) << 24u);
    EXPECT_EQ(exp32, result.read_fixed32(boundary - 2));
}

TEST(IOArray, ConcatSlowPath) {
    // Small strings
    {
        auto result = ioarray::concat(
          ioarray::copy_from(iobuf::from("hello")),
          ioarray::copy_from(iobuf::from("world")));
        EXPECT_TRUE(
          std::ranges::equal(result.as_range(), std::string("helloworld")));
    }
    // Both misaligned (offsets + non-chunk-multiple size)
    {
        auto full_a = make_filled(300_KiB);
        auto full_b = make_filled(200_KiB);
        auto result = ioarray::concat(
          full_a.share(10, 100_KiB), full_b.share(20, 80_KiB));

        auto expected = make_pattern(100_KiB, 10) + make_pattern(80_KiB, 20);
        EXPECT_TRUE(std::ranges::equal(result.as_range(), expected));
    }
}

TEST(IOArray, ConcatRandomized) {
    std::mt19937 rng(42);
    std::uniform_int_distribution<size_t> size_dist(0, 512_KiB);
    std::uniform_int_distribution<size_t> off_dist(0, 1000);

    for (int iter = 0; iter < 100; ++iter) {
        size_t a_size = size_dist(rng);
        size_t b_size = size_dist(rng);
        bool use_shares = iter >= 50;

        size_t a_off = 0, b_off = 0;
        size_t a_len = a_size, b_len = b_size;
        if (use_shares && a_size > 1) {
            a_off = std::min(off_dist(rng), a_size - 1);
            a_len = a_size - a_off;
        }
        if (use_shares && b_size > 1) {
            b_off = std::min(off_dist(rng), b_size - 1);
            b_len = b_size - b_off;
        }

        auto src_a = make_filled(a_size);
        auto a = use_shares ? src_a.share(a_off, a_len) : std::move(src_a);
        auto src_b = make_filled(b_size);
        auto b = use_shares ? src_b.share(b_off, b_len) : std::move(src_b);

        auto result = ioarray::concat(std::move(a), std::move(b));
        auto expected = make_pattern(a_len, a_off) + make_pattern(b_len, b_off);
        ASSERT_TRUE(std::ranges::equal(result.as_range(), expected))
          << "iter=" << iter;
    }
}

// NOLINTEND(*magic-numbers*)
