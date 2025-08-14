/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 *
 * This file includes code from RapidJSON (https://rapidjson.org/)
 *
 * Copyright (C) 2015 THL A29 Limited, a Tencent company, and Milo Yip.
 *
 * Licensed under the MIT License (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License
 * at http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#include "bytes/iobuf_parser.h"
#include "serde/json/detail/string.h"

#include <gtest/gtest.h>

#include <array>
#include <string_view>

using namespace serde::json::detail;

namespace {

std::string iobuf_as_string(iobuf b) {
    iobuf_parser p(std::move(b));
    return p.read_string(p.bytes_left());
}

} // namespace

struct test_case {
    std::string_view input;
    string_parser::result expected_err;
    size_t expected_pos;
    std::string_view expected_output;
};

std::ostream& operator<<(std::ostream& os, const test_case& tc) {
    // Don't print the input string as it's generally not valid UTF-8
    // and it breaks test reports.
    os << ", expected_err: " << static_cast<int>(tc.expected_err)
       << ", expected_pos: " << tc.expected_pos;

    if (!tc.expected_output.empty()) {
        return os << ", expected_output: " << tc.expected_output;
    }

    return os;
}

class string_parse_test : public testing::TestWithParam<test_case> {};

constexpr auto test_strings = std::to_array<test_case>({
  // Empty input.
  {"", string_parser::result::need_more_data, 0},
  // Invalid starting character.
  {"a", string_parser::result::invalid_json_string, 1},
  // Valid starting character.
  {"\"", string_parser::result::need_more_data, 1},
  // Valid but incomplete string.
  {"\"aaa", string_parser::result::need_more_data, 4},
  // Valid string.
  {"\"aaa\"", string_parser::result::done, 5, "aaa"},
  // Valid string with trailer.
  {"\"aaa\"aaa", string_parser::result::done, 5, "aaa"},
  // Valid string with escaped control character.
  {R"("a\ta")", string_parser::result::done, 6, "a\ta"},
  // Invalid string because of unescaped control character.
  {"\"a\ta\"", string_parser::result::invalid_json_string, 3},

  // Below are some of the more interesting strings from jsonchecker test
  // suite.
  {"\"1~!@#$%^&*()_+-={':[,]}|;.</>?\"",
   string_parser::result::done,
   32,
   "1~!@#$%^&*()_+-={':[,]}|;.</>?"},
  {R"("{\"object with 1 member\":[\"array with 1 element\"]}")",
   string_parser::result::done,
   55,
   R"({"object with 1 member":["array with 1 element"]})"},
  {
    R"("\u0123\u4567\u89AB\uCDEF\uabcd\uef4A")",
    string_parser::result::done,
    38,
    "ģ䕧覫췯ꯍ",
  },
  {
    R"("\/\\\"\uCAFE\uBABE\uAB98\uFCDE\ubcda\uef4A\b\f\n\r\t`1~!@#$%^&*()_+-=[]{}|;:',./<>?")",
    string_parser::result::done,
    85,
    "/\\\"\xEC\xAB\xBE\xEB\xAA\xBE\xEA\xAE\x98\xEF\xB3\x9E\xEB\xB3\x9A\xEE"
    "\xBD"
    "\x8A\b\f\n\r\t`1~!@#$%^&*()_+-=[]{}|;:',./<>?",
  },

  ////////////////////////////////////////////////////////////
  // Unicode escape sequences from RapidJSON test suite.

  // G clef sign U+1D11E (𝄞)
  {
    R"("\uD834\uDD1E")",
    string_parser::result::done,
    14,
    "\xF0\x9D\x84\x9E",
  },

  // Single low surrogate pair in string is invalid.
  {
    R"("\udc4d")",
    string_parser::result::invalid_json_string,
    7,
  },

  // Invalid surrogate pair.
  {
    R"("\uD800X")",
    string_parser::result::invalid_json_string,
    8,
  },
  {
    R"("\uD800\uFFFF")",
    string_parser::result::invalid_json_string,
    13,
  },

  //////////////////////////////////////////////////////////////
  // Custom test cases.

  // Short unicode escape sequence.
  {
    R"("\uCAF")",
    string_parser::result::invalid_json_string,
    7,
    "",
  },

  //////////////////////////////////////////////////////////////
  // UTF-8 validation test cases.

  // Valid UTF-8 sequences
  // 2-byte UTF-8: Euro sign (U+20AC)
  {
    "\"€\"",
    string_parser::result::done,
    5,
    "€",
  },
  // 3-byte UTF-8: Check mark (U+2713)
  {
    "\"✓\"",
    string_parser::result::done,
    5,
    "✓",
  },
  // 4-byte UTF-8: Musical symbol (U+1D11E)
  {
    "\"\xF0\x9D\x84\x9E\"",
    string_parser::result::done,
    6,
    "\xF0\x9D\x84\x9E",
  },
  // Mixed ASCII and UTF-8
  {
    "\"Hello 世界\"",
    string_parser::result::done,
    14,
    "Hello 世界",
  },

  // Invalid UTF-8 start bytes
  // Invalid start byte 0x80 (continuation byte used as start)
  {
    "\"\x80\"",
    string_parser::result::invalid_json_string,
    2,
  },
  // Invalid start byte 0xBF (continuation byte used as start)
  {
    "\"\xBF\"",
    string_parser::result::invalid_json_string,
    2,
  },
  // Invalid start byte 0xFE
  {
    "\"\xFE\"",
    string_parser::result::invalid_json_string,
    2,
  },
  // Invalid start byte 0xFF
  {
    "\"\xFF\"",
    string_parser::result::invalid_json_string,
    2,
  },

  // Truncated UTF-8 sequences
  // Truncated 2-byte sequence (missing closing quote to simulate end of buffer)
  {
    "\"\xC2",
    string_parser::result::need_more_data,
    2,
  },
  // Truncated 3-byte sequence (missing closing quote to simulate end of buffer)
  {
    "\"\xE0\xA0",
    string_parser::result::need_more_data,
    3,
  },
  // Truncated 4-byte sequence (missing closing quote to simulate end of buffer)
  {
    "\"\xF0\x90\x80",
    string_parser::result::need_more_data,
    4,
  },

  // Invalid continuation bytes
  // 2-byte sequence with invalid continuation
  {
    "\"\xC2\x20\"",
    string_parser::result::invalid_json_string,
    3,
  },
  // 3-byte sequence with invalid first continuation
  {
    "\"\xE0\x20\x80\"",
    string_parser::result::invalid_json_string,
    3,
  },
  // 3-byte sequence with invalid second continuation
  {
    "\"\xE0\xA0\x20\"",
    string_parser::result::invalid_json_string,
    4,
  },
  // 4-byte sequence with invalid continuation
  {
    "\"\xF0\x20\x80\x80\"",
    string_parser::result::invalid_json_string,
    3,
  },

  // Overlong encodings
  // Overlong 2-byte encoding of U+0000
  {
    "\"\xC0\x80\"",
    string_parser::result::invalid_json_string,
    3,
  },
  // Overlong 3-byte encoding of U+0000
  {
    "\"\xE0\x80\x80\"",
    string_parser::result::invalid_json_string,
    4,
  },
  // Overlong 3-byte encoding of U+007F
  {
    "\"\xE0\x81\xBF\"",
    string_parser::result::invalid_json_string,
    4,
  },
  // Overlong 4-byte encoding of U+0000
  {
    "\"\xF0\x80\x80\x80\"",
    string_parser::result::invalid_json_string,
    5,
  },

  // UTF-16 surrogates in raw UTF-8 (invalid)
  // High surrogate U+D800
  {
    "\"\xED\xA0\x80\"",
    string_parser::result::invalid_json_string,
    4,
  },
  // Low surrogate U+DC00
  {
    "\"\xED\xB0\x80\"",
    string_parser::result::invalid_json_string,
    4,
  },

  // Out of valid Unicode range
  // U+110000 (too large)
  {
    "\"\xF4\x90\x80\x80\"",
    string_parser::result::invalid_json_string,
    5,
  },
});

TEST_P(string_parse_test, test_string) {
    auto [input, expected_err, expected_pos, expected_output] = GetParam();
    string_parser parser;
    string_parser::result err;
    ss::temporary_buffer<char> buf(input.data(), input.size());
    size_t pos = parser.advance(buf, err);
    EXPECT_EQ(pos, expected_pos);
    EXPECT_EQ(err, expected_err);
    if (expected_err == string_parser::result::done) {
        EXPECT_EQ(iobuf_as_string(std::move(parser).value()), expected_output);
    } else {
        EXPECT_THROW(std::move(parser).value(), std::runtime_error);
    }
}

TEST_P(string_parse_test, test_piecewise) {
    auto [input, expected_err, expected_pos, expected_output] = GetParam();

    if (expected_err != string_parser::result::done) {
        // Skip cases that end in error because we don't know how to split them
        // in interesting but practical ways.
        return;
    }

    // Try splitting the input at every possible position.
    for (size_t i = 0; i < input.size() - 1; ++i) {
        // Feed the first i characters of the string to the parser.
        ss::temporary_buffer<char> buf(input.data(), i);
        string_parser parser;
        string_parser::result err;
        parser.advance(buf, err);
        // EXPECT_EQ(pos, i);

        if (err == string_parser::result::done) {
            // If the parser is done, we can't split it anymore. Exit early.
        } else {
            EXPECT_EQ(err, string_parser::result::need_more_data);

            // Feed remaining characters to the parser.
            buf = ss::temporary_buffer<char>(
              input.data() + i, input.size() - i);
            parser.advance(buf, err);
            // EXPECT_EQ(pos, input.size() - i);
            EXPECT_EQ(err, string_parser::result::done);
        }

        // Check that the parser is not reusable.
        EXPECT_THROW(parser.advance(buf, err), std::runtime_error);

        // Check that the value is correct.
        EXPECT_EQ(iobuf_as_string(std::move(parser).value()), expected_output)
          << "When split as: " << input.substr(0, i) << " | "
          << input.substr(i);
    }
}

TEST(piecewise_string_test, test_string) {
    std::string_view json_string = R"("hello world\nhello universe")";
    for (size_t i = 0; i < json_string.size() - 1; ++i) {
        // Feed the first i characters of the string to the parser.
        ss::temporary_buffer<char> buf(json_string.data(), i);
        string_parser parser;
        string_parser::result err;
        size_t pos = parser.advance(buf, err);
        EXPECT_EQ(pos, i);
        EXPECT_EQ(err, string_parser::result::need_more_data);

        // Feed remaining characters to the parser.
        buf = ss::temporary_buffer<char>(
          json_string.data() + i, json_string.size() - i);
        pos = parser.advance(buf, err);
        EXPECT_EQ(pos, json_string.size() - i);
        EXPECT_EQ(err, string_parser::result::done);

        // Check that the parser is not reusable.
        EXPECT_THROW(parser.advance(buf, err), std::runtime_error);

        // Check that the value is correct.
        EXPECT_EQ(
          iobuf_as_string(std::move(parser).value()),
          "hello world\nhello universe")
          << "When split as: " << json_string.substr(0, i) << " | "
          << json_string.substr(i);
    }
}

INSTANTIATE_TEST_SUITE_P(
  string_parse_suite, string_parse_test, testing::ValuesIn(test_strings));
