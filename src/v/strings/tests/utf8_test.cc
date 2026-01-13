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

#include "bytes/iobuf.h"
#include "strings/utf8.h"

#include <boost/test/unit_test.hpp>

#include <array>
#include <string>

BOOST_AUTO_TEST_CASE(is_valid_utf8_test) {
    auto ts = [](char v) { return std::string{v}; };

    auto test_cases = std::vector<std::pair<std::string, bool>>{
      {"世", true},
      {"hello", true},
      {ts(0x80), false},
      {ts(0xFF), false},
      {ts(0xC2), false},
      {ts(0xE0), false},
      {ts(0xFF) + "hello", false},
    };

    for (auto& [param, expected] : test_cases) {
        BOOST_CHECK_EQUAL(
          utf::is_valid_utf8(param.cbegin(), param.cend(), param.size()),
          expected);
    }
}

BOOST_AUTO_TEST_CASE(is_valid_utf8_read_limit) {
    auto ts = [](char v) { return std::string{v}; };

    using test_params = std::pair<std::string, size_t>;
    auto test_cases = std::vector<std::pair<test_params, bool>>{
      {{"世", 1}, true},
      {{ts(0x80), 1}, false},
      {{ts(0xFF), 1}, false},
      {{"hello" + ts(0xFF), 5}, true},
      {{"hello" + ts(0xFF), 6}, false},
    };

    for (auto& [params, expected] : test_cases) {
        auto& [val, max_read_bytes] = params;
        BOOST_CHECK_EQUAL(
          utf::is_valid_utf8(val.cbegin(), val.cend(), max_read_bytes),
          expected);
    }
}

BOOST_AUTO_TEST_CASE(utf32_code_point_to_utf8) {
    auto test_cases
      = std::vector<std::pair<utf::utf32_code_point, std::array<char, 4>>>{
        {{U'a'}, {'a', 0, 0, 0}},
      };
    for (auto& [param, expected] : test_cases) {
        BOOST_CHECK(param.utf8_encoding() == expected);
    }
}

BOOST_AUTO_TEST_CASE(utf32_code_point_encoding_length) {
    auto test_cases = std::vector<std::pair<utf::utf32_code_point, int>>{
      {{U'a'}, 1},
      {{0x007F + 1}, 2},
      {{0x07FF + 1}, 3},
      {{0xFFFF + 1}, 4},
    };

    for (auto& [param, expected] : test_cases) {
        BOOST_CHECK_EQUAL(param.utf8_encoding_length(), expected);
    }
}

BOOST_AUTO_TEST_CASE(utf32_code_point_try_increment) {
    auto test_cases = std::vector<
      std::pair<utf::utf32_code_point, std::optional<utf::utf32_code_point>>>{
      {{U'a'}, {{U'b'}}},
      {{U'é'}, {{U'ê'}}},
      {{U'世'}, {{U'丗'}}},
      {{0xD800}, {{0xE000}}}, // surrogate range
      {{0xDFFF}, {{0xE000}}}, // ^^
      {{0x007F}, {}}, // can't increment without increasing utf8 encoding length
      {{0x07FF}, {}}, // ^^
      {{0xFFFF}, {}}, // ^^
      {{0x10FFFF}, {}}, // max utf32 encoding
    };

    for (auto& [param, expected] : test_cases) {
        BOOST_CHECK(param.try_increment() == expected);
    }
}

BOOST_AUTO_TEST_CASE(find_incomplete_code_point_test) {
    std::string a = std::string{"hello世"};
    iobuf b = iobuf::from(a);
    b.trim_back(1);
    auto begin = iobuf::reverse_byte_iterator(b.crbegin(), b.crend());
    auto end = iobuf::reverse_byte_iterator(b.crend(), b.crend());
    auto r = utf::find_incomplete_code_point(begin, end);
    BOOST_CHECK_EQUAL(r, 2);

    b.trim_back(r);
    BOOST_CHECK_EQUAL(b.linearize_to_string(), "hello");

    begin = iobuf::reverse_byte_iterator(b.crbegin(), b.crend());
    end = iobuf::reverse_byte_iterator(b.crend(), b.crend());
    r = utf::find_incomplete_code_point(begin, end);
    BOOST_CHECK_EQUAL(r, 0);
}

BOOST_AUTO_TEST_CASE(utf32_reverse_iterator_test) {
    auto test_cases
      = std::vector<std::pair<std::string, std::vector<utf::code_point_t>>>{
        {"", {}},
        {"a", {U'a'}},
        {"hello世", {U'h', U'e', U'l', U'l', U'o', U'世'}},
      };
    for (auto& [param, expected] : test_cases) {
        iobuf b = iobuf::from(param);
        auto begin = iobuf::reverse_byte_iterator(b.crbegin(), b.crend());
        auto end = iobuf::reverse_byte_iterator(b.crend(), b.crend());
        auto rcurr = utf::utf32_reverse_iterator(begin, end);
        auto rend = utf::utf32_reverse_iterator(end, end);
        std::vector<utf::code_point_t> rev_res{};
        for (; rcurr != rend; ++rcurr) {
            rev_res.push_back(rcurr->code_point);
        }
        std::vector<utf::code_point_t> res{rev_res.crbegin(), rev_res.crend()};
        BOOST_CHECK_EQUAL(expected, res);
    }
}
