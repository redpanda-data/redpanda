// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf_parser.h"

#include <gtest/gtest-matchers.h>
#include <gtest/gtest.h>

namespace {
[[maybe_unused]] const std::string hello = "hello";
[[maybe_unused]] const std::string world = "world";
[[maybe_unused]] const std::string hello_world = "helloworld";
[[maybe_unused]] const std::string multi_byte = "۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩";

// smoke test
TEST(iobuf_parser_tests, smoke) {
    auto buf = iobuf::from(hello);
    buf.append_str(world);
    auto parser = iobuf_parser{std::move(buf)};

    auto output = parser.read_string_safe(hello.size() + world.size());

    EXPECT_EQ(output, hello_world);
    EXPECT_EQ(parser.bytes_left(), 0);
};

// check that read_string_safe can truncate output
TEST(iobuf_parser_tests, test_shorter_string) {
    auto buf = iobuf::from(hello);
    buf.append_str(world);
    auto parser = iobuf_parser{std::move(buf)};

    // check that its okay reading over fragment boundary
    auto output = parser.read_string_safe(hello.size() + 1);

    EXPECT_EQ(output, "hellow");
    EXPECT_EQ(parser.bytes_left(), (hello_world.size() - hello.size() - 1));
};

// test a utf8 string with an incomplete final character
TEST(iobuf_parser_tests, test_multibyte_string) {
    auto buf = iobuf::from(multi_byte);
    auto parser = iobuf_parser{std::move(buf)};

    // check that its okay reading over fragment boundary
    auto output = parser.read_string_safe(multi_byte.size() - 1);

    EXPECT_EQ(output.size(), multi_byte.size() - 2);
    EXPECT_EQ(parser.bytes_left(), 2);
};

// test that a utf8 string can be partially read and continued from where it
// left off
TEST(iobuf_parser_tests, test_continued_read) {
    auto buf = iobuf::from(multi_byte);
    auto parser = iobuf_parser{std::move(buf)};

    const auto clean_cut_point = 4;
    const auto ragged_cut_point = clean_cut_point + 1;

    auto first_read = multi_byte.substr(0, clean_cut_point);
    auto second_read = multi_byte.substr(clean_cut_point, multi_byte.size());

    // check that its okay reading over fragment boundary
    auto output = parser.read_string_safe(ragged_cut_point);

    EXPECT_EQ(output.size(), clean_cut_point);
    EXPECT_EQ(output, first_read);
    EXPECT_EQ(parser.bytes_left(), (multi_byte.size() - clean_cut_point));

    output = parser.read_string_safe(parser.bytes_left());
    EXPECT_EQ(output, second_read);
    EXPECT_EQ(parser.bytes_left(), 0);
};

} // namespace
