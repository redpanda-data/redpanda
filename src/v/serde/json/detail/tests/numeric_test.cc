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

#include "random/generators.h"
#include "serde/json/detail/numeric.h"

#include <seastar/util/variant_utils.hh>

#include <gtest/gtest.h>

#include <array>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string_view>
#include <variant>

using namespace serde::json::detail;

struct test_case {
    /// Shortcut for valid inputs. Not a constructor because we want to
    /// keep the ability to use fields directly.
    constexpr static test_case valid(
      std::string_view input,
      std::variant<std::monostate, int64_t, double> output) {
        return {
          .input = input,
          .expected_err = numeric_parser::result::done,
          .expected_pos = input.size(),
          .expected_output = output,
        };
    }

    std::string_view input;
    numeric_parser::result expected_err;
    size_t expected_pos;
    std::variant<std::monostate, int64_t, double> expected_output;
};

std::ostream& operator<<(std::ostream& os, const test_case& tc) {
    os << "input: " << tc.input
       << ", expected_err: " << static_cast<int>(tc.expected_err)
       << ", expected_pos: " << tc.expected_pos;

    ss::visit(tc.expected_output, [&os](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, int64_t>) {
            os << ", expected_output: " << v;
        } else if constexpr (std::is_same_v<T, double>) {
            os << ", expected_output: " << v;
        }
    });

    return os;
}

class numeric_parse_test : public testing::TestWithParam<test_case> {};

// TODO: some positions are off-by-one. Think a bit about what we want
//   to do with them and ensure they are consistent with the rest
//   of the parser.
constexpr auto test_numerics = std::to_array<test_case>(
  {// Empty input.
   {"", numeric_parser::result::need_more_data, 0},
   // Invalid starting character.
   {"a", numeric_parser::result::invalid_json_string, 0},
   {"+0", numeric_parser::result::invalid_json_string, 0},
   {"0.0", numeric_parser::result::done, 3, 0.0},
   test_case::valid("0", 0),
   test_case::valid("0.0", 0.0),
   test_case::valid("123", 123),
   test_case::valid("-123", -123),
   test_case::valid("-0", 0),
   {"00,", numeric_parser::result::invalid_json_string, 1},
   {".", numeric_parser::result::invalid_json_string, 0},
   {"0.", numeric_parser::result::need_more_data, 2},
   {"-0.", numeric_parser::result::need_more_data, 3},
   {"0.a", numeric_parser::result::invalid_json_string, 2},
   {"-0.a", numeric_parser::result::invalid_json_string, 3},
   test_case::valid("0.0", 0.0),
   test_case::valid("-0.0", -0.0),
   test_case::valid("0.123", 0.123),
   test_case::valid("-0.123", -0.123),
   test_case::valid("123.123", 123.123),
   test_case::valid("-123.123", -123.123),

   // Large values
   test_case::valid(
     "9223372036854775807",
     std::numeric_limits<int64_t>::max()), // max int64_t
   test_case::valid(
     "-9223372036854775808",
     std::numeric_limits<int64_t>::min()), // min int64_t

   // Overflow cases, should be converted to double.
   test_case::valid("9223372036854775808", 9223372036854775808.0),
   test_case::valid("-9223372036854775809", -9223372036854775809.0),

   /////////////////////////////////////////////////////////////////////////////
   // Test cases from RapidJSON (test/unittest/readertest.cpp)

   test_case::valid("0e100", 0.0), // For checking issue #1249
   test_case::valid("1.0", 1.0),
   test_case::valid("-1.0", -1.0),
   test_case::valid("1.5", 1.5),
   test_case::valid("-1.5", -1.5),
   test_case::valid("3.1416", 3.1416),
   test_case::valid("1E10", 1E10),
   test_case::valid("1e10", 1e10),
   test_case::valid("1E+10", 1E+10),
   test_case::valid("1E-10", 1E-10),
   test_case::valid("-1E10", -1E10),
   test_case::valid("-1e10", -1e10),
   test_case::valid("-1E+10", -1E+10),
   test_case::valid("-1E-10", -1E-10),
   test_case::valid("1.234E+10", 1.234E+10),
   test_case::valid("1.234E-10", 1.234E-10),
   test_case::valid("1.79769e+308", 1.79769e+308),
   test_case::valid("2.22507e-308", 2.22507e-308),
   test_case::valid("-1.79769e+308", -1.79769e+308),
   test_case::valid("-2.22507e-308", -2.22507e-308),
   test_case::valid(
     "4.9406564584124654e-324", 4.9406564584124654e-324), // minimum denormal
   test_case::valid(
     "2.2250738585072009e-308",
     2.2250738585072009e-308), // Max subnormal double
   test_case::valid(
     "2.2250738585072014e-308",
     2.2250738585072014e-308), // Min normal positive double
   test_case::valid(
     "1.7976931348623157e+308", 1.7976931348623157e+308), // Max double
   test_case::valid("1e-10000", 0.0),                     // must underflow
   test_case::valid(
     "18446744073709551616",
     18446744073709551616.0), // 2^64 (max of uint64_t + 1, force to use
                              // double)
   test_case::valid(
     "-9223372036854775809",
     -9223372036854775809.0), // -2^63 - 1(min of int64_t + 1, force to use
                              // double)

   test_case::valid(
     "0.9868011474609375",
     0.9868011474609375), // https://github.com/Tencent/rapidjson/issues/120
   test_case::valid("123e34", 123e34), // Fast Path Cases In Disguise
   test_case::valid("45913141877270640000.0", 45913141877270640000.0),
   test_case::valid(
     "2.2250738585072011e-308",
     2.2250738585072011e-308), // http://www.exploringbinary.com/php-hangs-on-numeric-value-2-2250738585072011e-308/
   test_case::valid(
     "1e-00011111111111",
     0.0), // https://github.com/Tencent/rapidjson/issues/313
   test_case::valid("-1e-00011111111111", -0.0),
   test_case::valid("1e-214748363", 0.0), // Maximum supported negative exponent
   test_case::valid("1e-214748364", 0.0),
   test_case::valid("1e-21474836311", 0.0),
   test_case::valid("1.00000000001e-2147483638", 0.0),
   test_case::valid(
     "0.017976931348623157e+310",
     1.7976931348623157e+308), // Max double in another form
   test_case::valid(
     "128.74836467836484838364836483643636483648e-336",
     0.0), // https://github.com/Tencent/rapidjson/issues/1251
   test_case::valid(
     "0.184467440737095516159",
     0.184467440737095516159), // decimal part is 10 * (2^64 - 1) + 9

   // Since
   // abs((2^-1022 - 2^-1074) - 2.2250738585072012e-308)
   // = 3.109754131239141401123495768877590405345064751974375599... x 10^-324
   // abs((2^-1022) - 2.2250738585072012e-308)
   // = 1.830902327173324040642192159804623318305533274168872044... x 10 ^ -324
   // So 2.2250738585072012e-308 should round to 2^-1022
   // = 2.2250738585072014e-308
   test_case::valid(
     "2.2250738585072012e-308",
     2.2250738585072014e-308), // http://www.exploringbinary.com/java-hangs-when-converting-2-2250738585072012e-308/

   // More closer to normal/subnormal boundary
   // boundary = 2^-1022 - 2^-1075
   // = 2.225073858507201136057409796709131975934819546351645648... x 10^-308
   test_case::valid(
     "2.22507385850720113605740979670913197593481954635164564e-308",
     2.2250738585072009e-308),
   test_case::valid(
     "2.22507385850720113605740979670913197593481954635164565e-308",
     2.2250738585072014e-308),

   // 1.0 is in (1.0 - 2^-54, 1.0 + 2^-53)
   // 1.0 - 2^-54 = 0.999999999999999944488848768742172978818416595458984375
   test_case::valid(
     "0.999999999999999944488848768742172978818416595458984375",
     1.0), // round to even
   test_case::valid(
     "0.999999999999999944488848768742172978818416595458984374",
     0.99999999999999989), // previous double
   test_case::valid(
     "0.999999999999999944488848768742172978818416595458984376",
     1.0), // next double
           // 1.0 + 2^-53
           // = 1.00000000000000011102230246251565404236316680908203125
   test_case::valid(
     "1.00000000000000011102230246251565404236316680908203125",
     1.0), // round to even
   test_case::valid(
     "1.00000000000000011102230246251565404236316680908203124",
     1.0), // previous double
   test_case::valid(
     "1.00000000000000011102230246251565404236316680908203126",
     1.00000000000000022), // next double

   // Numbers from
   // https://github.com/floitsch/double-conversion/blob/master/test/cctest/test-strtod.cc

   test_case::valid("72057594037927928.0", 72057594037927928.0),
   test_case::valid("72057594037927936.0", 72057594037927936.0),
   test_case::valid("72057594037927932.0", 72057594037927936.0),
   test_case::valid("7205759403792793199999e-5", 72057594037927928.0),
   test_case::valid("7205759403792793200001e-5", 72057594037927936.0),

   test_case::valid("9223372036854774784.0", 9223372036854774784.0),
   test_case::valid("9223372036854775808.0", 9223372036854775808.0),
   test_case::valid("9223372036854775296.0", 9223372036854775808.0),
   test_case::valid("922337203685477529599999e-5", 9223372036854774784.0),
   test_case::valid("922337203685477529600001e-5", 9223372036854775808.0),

   test_case::valid(
     "10141204801825834086073718800384", 10141204801825834086073718800384.0),
   test_case::valid(
     "10141204801825835211973625643008", 10141204801825835211973625643008.0),
   test_case::valid(
     "10141204801825834649023672221696", 10141204801825835211973625643008.0),
   test_case::valid(
     "1014120480182583464902367222169599999e-5",
     10141204801825834086073718800384.0),
   test_case::valid(
     "1014120480182583464902367222169600001e-5",
     10141204801825835211973625643008.0),

   test_case::valid(
     "5708990770823838890407843763683279797179383808",
     5708990770823838890407843763683279797179383808.0),
   test_case::valid(
     "5708990770823839524233143877797980545530986496",
     5708990770823839524233143877797980545530986496.0),
   test_case::valid(
     "5708990770823839207320493820740630171355185152",
     5708990770823839524233143877797980545530986496.0),
   test_case::valid(
     "5708990770823839207320493820740630171355185151999e-3",
     5708990770823838890407843763683279797179383808.0),
   test_case::valid(
     "5708990770823839207320493820740630171355185152001e-3",
     5708990770823839524233143877797980545530986496.0),

   // Cover trimming
   test_case::valid(
     "2."
     "225073858507201136057409796709131975934819546351645648023426109724822222"
     "02"
     "107694551652952390813508"
     "791414915891303962110687008643869459464552765720740782062174337998814106"
     "32"
     "67329253552286881372149012"
     "981122451451889849057222307285255133155755015914397476397983411801999323"
     "96"
     "25482890171070818506906306"
     "666559949382757725720157630626906633326475653000092458883164330377797918"
     "69"
     "61204949739037782970490505"
     "108060994073026293712895895000358379996720725430436028407889577179615094"
     "55"
     "16748243471030702609144621"
     "572289880258182545180325707018860872113128079512233426288368622321503775"
     "66"
     "66225039825343359745688844"
     "239002654981983854879482922068947216898310996983658468140228542433306603"
     "39"
     "85088644580400103493397042"
     "756718644338377048603786162277173854562306587467901408672332763671875123"
     "45"
     "67890123456789012345678901"
     "e-308",
     2.2250738585072014e-308),

   // https://github.com/Tencent/rapidjson/issues/340
   test_case::valid("7.450580596923828e-9", 7.450580596923828e-9),

   // https://github.com/Tencent/rapidjson/issues/1249
   test_case::valid("0e100", 0.0),

   // https://github.com/Tencent/rapidjson/issues/1251
   test_case::valid("128.74836467836484838364836483643636483648e-336", 0.0),

   // https://github.com/Tencent/rapidjson/issues/1256
   test_case::valid(
     "6223372036854775296.1701512723685473547372536854755293372036854685477"
     "529752233737201701512337200972013723685473123372036872036854236854737"
     "247372368372367752975258547752975254729752547372368737201701512354737"
     "83723677529752585477247372368372368547354737253685475529752",
     6223372036854775808.0),

   // Bad inputs.
   //   {"1e309", numeric_parser::result::invalid_json_string, 5},
   // Miss fraction part in number.
   {"1.,", numeric_parser::result::invalid_json_string, 2},
   {"1.a", numeric_parser::result::invalid_json_string, 2},

   // Miss exponent in number.
   {"1e,", numeric_parser::result::invalid_json_string, 2},
   {"1e_", numeric_parser::result::invalid_json_string, 2},

   // https://github.com/Tencent/rapidjson/issues/849
   {"1.8e308,", numeric_parser::result::invalid_json_string, 7},
   {"5e308,", numeric_parser::result::invalid_json_string, 5},
   {"1e309", numeric_parser::result::invalid_json_string, 5},
   {"1.0e310", numeric_parser::result::invalid_json_string, 7},
   {"1.00e310,", numeric_parser::result::invalid_json_string, 8},
   {"-1.8e308,", numeric_parser::result::invalid_json_string, 8},
   {"-1e309,", numeric_parser::result::invalid_json_string, 6},

   // https://github.com/Tencent/rapidjson/issues/1253
   {"2e308,", numeric_parser::result::invalid_json_string, 5},

   // https://github.com/Tencent/rapidjson/issues/1259
   {"88474320368547737236837236775298547354737253685475547552933720368546854775"
    "297525"
    "29337203685468547770151233720097201372368547312337203687203685423685123372"
    "036872"
    "03685473724737236837236775297525854775297525472975254737236873720170151235"
    "473783"
    "7236737247372368772473723683723456789012E66,",
    numeric_parser::result::invalid_json_string,
    283},

   {"1."
    "7976931348623159077293051907890247336179769789423065727343008115773267580"
    "55009"
    "6313270847732240753602112011387987139335765878976881441662249284743063947"
    "4124377"
    "7678934248654852763022196012460941194530829520850057688381506823424628814"
    "7391311"
    "0540827237163350510684586298239947245938479716304835356329624224137216e+"
    "308,",
    numeric_parser::result::invalid_json_string,
    315}});

void test_parse_numeric_whole(const test_case& tc) {
    SCOPED_TRACE("test_parse_numeric_piecewise");

    auto [input, expected_err, expected_pos, expected_output] = tc;
    numeric_parser parser;
    numeric_parser::result err;

    // All inputs have a trailing character so that the parser knows
    // that all digits have been consumed. This is valid assumptions
    // as numbers standalone are not valid JSON.
    ss::temporary_buffer<char> buf(input.size() + 1);
    std::copy(input.begin(), input.end(), buf.get_write());

    if (expected_err == numeric_parser::result::done) {
        buf.get_write()[buf.size() - 1] = ','; // Add a trailing character.
    } else {
        buf.trim(input.size());
    }

    size_t pos = parser.advance(buf, err);
    ASSERT_EQ(err, expected_err);
    EXPECT_EQ(pos, expected_pos);

    if (expected_err == numeric_parser::result::done) {
        EXPECT_TRUE(!std::holds_alternative<std::monostate>(expected_output));

        ss::visit(expected_output, [&parser](auto&& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, int64_t>) {
                EXPECT_EQ(std::move(parser).value_int64(), v);
            } else if constexpr (std::is_same_v<T, double>) {
                EXPECT_DOUBLE_EQ(std::move(parser).value_double(), v);
            }
        });
    } else {
        EXPECT_TRUE(std::holds_alternative<std::monostate>(expected_output));

        // Check that value_* methods throw if the parser errored. Since we
        // can't move the parser twice we deterministically pseudo-randomize
        // the method to call.
        if (input.size() % 2 == 0) {
            EXPECT_THROW(std::move(parser).value_int64(), std::runtime_error);
        } else {
            EXPECT_THROW(std::move(parser).value_double(), std::runtime_error);
        }
    }
}

void test_parse_numeric_piecewise(const test_case& tc) {
    SCOPED_TRACE("test_parse_numeric_piecewise");

    auto [input, expected_err, expected_pos, expected_output] = tc;

    if (expected_err != numeric_parser::result::done) {
        // Skip cases that end in error because we don't know how to split them
        // in interesting but practical ways.
        return;
    }

    // Try splitting the input at every possible position.
    for (size_t i = 0; i < input.size() - 1; ++i) {
        // Feed the first i characters of the string to the parser.
        ss::temporary_buffer<char> buf(input.data(), i);
        numeric_parser parser;
        numeric_parser::result err;
        parser.advance(buf, err);
        // EXPECT_EQ(pos, i);

        if (err == numeric_parser::result::done) {
            // If the parser is done, we can't split it anymore. Exit early.
        } else {
            EXPECT_EQ(err, numeric_parser::result::need_more_data);

            // Feed remaining characters to the parser.
            buf = ss::temporary_buffer<char>(
              input.data() + i, input.size() - i);
            parser.advance(buf, err);
            // EXPECT_EQ(pos, input.size() - i);
            EXPECT_EQ(err, numeric_parser::result::need_more_data);

            //
            auto buf2 = ss::temporary_buffer<char>(",", 1);
            parser.advance(buf2, err);
            EXPECT_EQ(err, numeric_parser::result::done);
        }

        // Check that the parser is not reusable.
        EXPECT_THROW(parser.advance(buf, err), std::runtime_error);

        // Check that the value is correct.
        EXPECT_TRUE(!std::holds_alternative<std::monostate>(expected_output));

        ss::visit(expected_output, [&parser, &input, i](auto&& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, int64_t>) {
                ASSERT_EQ(std::move(parser).value_int64(), v)
                  << "When split as: " << input.substr(0, i) << " | "
                  << input.substr(i);
            } else if constexpr (std::is_same_v<T, double>) {
                ASSERT_DOUBLE_EQ(std::move(parser).value_double(), v)
                  << "When split as: " << input.substr(0, i) << " | "
                  << input.substr(i);
            }
        });
    }
}

TEST_P(numeric_parse_test, test_numeric) {
    test_parse_numeric_whole(GetParam());
}

TEST_P(numeric_parse_test, test_piecewise) {
    test_parse_numeric_piecewise(GetParam());
}

INSTANTIATE_TEST_SUITE_P(
  numeric_parse_suite, numeric_parse_test, testing::ValuesIn(test_numerics));

TEST(numeric_parse, test_numeric_1E308) {
    // From RapidJSON test suite: '1' followed by 308 '0'

    std::string input = "1";
    input.append(308, '0');

    test_parse_numeric_whole(test_case::valid(input, 1e308));
    test_parse_numeric_piecewise(test_case::valid(input, 1e308));
}

TEST(numeric_parse, test_numeric_1E309) {
    // From RapidJSON test suite:
    // Number too big to be stored in double.
    // '1' followed by 309 '0'

    std::string input = "1";
    input.append(309, '0');
    input.append(1, ',');

    test_parse_numeric_whole(
      test_case{input, numeric_parser::result::invalid_json_string, 310});
}
