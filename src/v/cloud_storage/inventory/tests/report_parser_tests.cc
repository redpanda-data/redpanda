/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/seastarx.h"
#include "cloud_storage/inventory/report_parser.h"
#include "cloud_storage/inventory/tests/common.h"
#include "test_utils/randoms.h"
#include "utils/base64.h"

#include <seastar/core/iostream.hh>
#include <seastar/util/file.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace cloud_storage::inventory;

namespace {

std::vector<ss::sstring> collect(report_parser& p) {
    std::vector<ss::sstring> collected;
    p.consume([&collected](auto&& rows) {
         collected.insert(
           collected.end(),
           std::make_move_iterator(rows.begin()),
           std::make_move_iterator(rows.end()));
         return ss::make_ready_future();
     })
      .get();
    EXPECT_TRUE(p.eof());
    p.stop().get();
    return collected;
}

template<typename Content>
report_parser make_parser(
  Content content,
  is_gzip_compressed compress,
  size_t chunk_size = details::io_allocation_size::max_chunk_size) {
    return report_parser{
      make_report_stream(content, compress), compress, chunk_size};
}
} // namespace

TEST(Parser, ParseReport) {
    auto input = std::vector<ss::sstring>{"foo", "bar", "x"};
    for (const auto compression :
         {is_gzip_compressed::no, is_gzip_compressed::yes}) {
        auto p = make_parser(input, compression);
        EXPECT_EQ(collect(p), input);
    }
}

TEST(Parser, ParseEmptyString) {
    std::string input{};
    for (const auto compression :
         {is_gzip_compressed::no, is_gzip_compressed::yes}) {
        auto p = make_parser(input, compression);
        EXPECT_EQ(collect(p), std::vector<ss::sstring>{});
    }
}

TEST(Parser, ParseTrailingNewLine) {
    std::string input{"foobar\n"};
    std::vector<ss::sstring> expected{"foobar"};
    for (const auto compression :
         {is_gzip_compressed::no, is_gzip_compressed::yes}) {
        auto p = make_parser(input, compression);
        EXPECT_EQ(collect(p), expected);
    }
}

TEST(Parser, ParseLargePayloadWithSmallChunkSize) {
    std::vector<ss::sstring> input;
    input.reserve(1024);
    for (auto i = 0; i < 1024; ++i) {
        input.push_back(tests::random_named_string<ss::sstring>(1024));
    }

    for (const auto chunk_size : {16, 1024, 64 * 1024}) {
        auto p = make_parser(input, is_gzip_compressed::yes, chunk_size);
        EXPECT_EQ(collect(p), input);
    }
}

TEST(Parser, ParseGzippedFile) {
    // This is the base64 encoding of a gzipped file containing the strings 1, 2
    // and 3, one per line.
    constexpr auto gzipped_data
      = "H4sICLuDbmYAA3Rlc3QAM+Qy4jLmAgDYVF93BgAAAA==";
    auto p = report_parser{
      make_report_stream(base64_to_string(gzipped_data)),
      is_gzip_compressed::yes};
    EXPECT_EQ(collect(p), (std::vector<ss::sstring>{"1", "2", "3"}));
}

TEST(Parser, BatchSizeControlledByChunkSize) {
    size_t chunk_size{4096};

    std::vector<ss::sstring> input;
    input.reserve(1024);

    for (auto i = 0; i < 1024; ++i) {
        // make each row slightly overshoot the chunk size, so the parser has to
        // keep data around between calls to next()
        input.push_back(
          tests::random_named_string<ss::sstring>(chunk_size + 1));
    }

    auto parser = make_parser(input, is_gzip_compressed::yes, chunk_size);
    report_parser::rows_t rows;
    do {
        rows = parser.next().get();
        const auto sizes = rows | std::views::transform(&ss::sstring::size);
        // The stream compressor can return upto two chunks. The parser could
        // have cached slightly less than one chunk between calls. The total
        // chars returned should be below this limit.
        ASSERT_LE(
          std::accumulate(sizes.begin(), sizes.end(), 0ul), chunk_size * 3 + 3);
    } while (!rows.empty());

    ASSERT_TRUE(parser.eof());
    parser.stop().get();
}
