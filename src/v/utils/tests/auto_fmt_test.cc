// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/auto_fmt.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(auto_fmt_0) {
    struct foo : auto_fmt<foo, ','> {};
    std::stringstream ss;
    ss << foo{};
    BOOST_REQUIRE_EQUAL(ss.str(), "");
}

BOOST_AUTO_TEST_CASE(auto_fmt_1) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
    };
    std::stringstream ss;
    ss << foo{.f1 = 1};
    BOOST_REQUIRE_EQUAL(ss.str(), "1");
}

BOOST_AUTO_TEST_CASE(auto_fmt_2) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
    };
    std::stringstream ss;
    ss << foo{.f1 = 1, .f2 = 2};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2");
}

BOOST_AUTO_TEST_CASE(auto_fmt_3) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
    };
    std::stringstream ss;
    ss << foo{.f1 = 1, .f2 = 2, .f3 = 3};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3");
}

BOOST_AUTO_TEST_CASE(auto_fmt_4) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
    };
    std::stringstream ss;
    ss << foo{.f1 = 1, .f2 = 2, .f3 = 3, .f4 = 4};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4");
}

BOOST_AUTO_TEST_CASE(auto_fmt_5) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
        int f5;
    };
    std::stringstream ss;
    ss << foo{.f1 = 1, .f2 = 2, .f3 = 3, .f4 = 4, .f5 = 5};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4,5");
}

BOOST_AUTO_TEST_CASE(auto_fmt_6) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
        int f5;
        int f6;
    };
    std::stringstream ss;
    ss << foo{.f1 = 1, .f2 = 2, .f3 = 3, .f4 = 4, .f5 = 5, .f6 = 6};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4,5,6");
}

BOOST_AUTO_TEST_CASE(auto_fmt_7) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
        int f5;
        int f6;
        int f7;
    };
    std::stringstream ss;
    ss << foo{.f1 = 1, .f2 = 2, .f3 = 3, .f4 = 4, .f5 = 5, .f6 = 6, .f7 = 7};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4,5,6,7");
}

BOOST_AUTO_TEST_CASE(auto_fmt_8) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
        int f5;
        int f6;
        int f7;
        int f8;
    };
    std::stringstream ss;
    ss << foo{
      .f1 = 1, .f2 = 2, .f3 = 3, .f4 = 4, .f5 = 5, .f6 = 6, .f7 = 7, .f8 = 8};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4,5,6,7,8");
}

BOOST_AUTO_TEST_CASE(auto_fmt_9) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
        int f5;
        int f6;
        int f7;
        int f8;
        int f9;
    };
    std::stringstream ss;
    ss << foo{
      .f1 = 1,
      .f2 = 2,
      .f3 = 3,
      .f4 = 4,
      .f5 = 5,
      .f6 = 6,
      .f7 = 7,
      .f8 = 8,
      .f9 = 9};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4,5,6,7,8,9");
}

BOOST_AUTO_TEST_CASE(auto_fmt_10) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
        int f5;
        int f6;
        int f7;
        int f8;
        int f9;
        int f10;
    };
    std::stringstream ss;
    ss << foo{
      .f1 = 1,
      .f2 = 2,
      .f3 = 3,
      .f4 = 4,
      .f5 = 5,
      .f6 = 6,
      .f7 = 7,
      .f8 = 8,
      .f9 = 9,
      .f10 = 10};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4,5,6,7,8,9,10");
}

BOOST_AUTO_TEST_CASE(auto_fmt_11) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
        int f5;
        int f6;
        int f7;
        int f8;
        int f9;
        int f10;
        int f11;
    };
    std::stringstream ss;
    ss << foo{
      .f1 = 1,
      .f2 = 2,
      .f3 = 3,
      .f4 = 4,
      .f5 = 5,
      .f6 = 6,
      .f7 = 7,
      .f8 = 8,
      .f9 = 9,
      .f10 = 10,
      .f11 = 11};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4,5,6,7,8,9,10,11");
}

BOOST_AUTO_TEST_CASE(auto_fmt_12) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
        int f5;
        int f6;
        int f7;
        int f8;
        int f9;
        int f10;
        int f11;
        int f12;
    };
    std::stringstream ss;
    ss << foo{
      .f1 = 1,
      .f2 = 2,
      .f3 = 3,
      .f4 = 4,
      .f5 = 5,
      .f6 = 6,
      .f7 = 7,
      .f8 = 8,
      .f9 = 9,
      .f10 = 10,
      .f11 = 11,
      .f12 = 12};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4,5,6,7,8,9,10,11,12");
}

BOOST_AUTO_TEST_CASE(auto_fmt_13) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
        int f5;
        int f6;
        int f7;
        int f8;
        int f9;
        int f10;
        int f11;
        int f12;
        int f13;
    };
    std::stringstream ss;
    ss << foo{
      .f1 = 1,
      .f2 = 2,
      .f3 = 3,
      .f4 = 4,
      .f5 = 5,
      .f6 = 6,
      .f7 = 7,
      .f8 = 8,
      .f9 = 9,
      .f10 = 10,
      .f11 = 11,
      .f12 = 12,
      .f13 = 13};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4,5,6,7,8,9,10,11,12,13");
}

BOOST_AUTO_TEST_CASE(auto_fmt_14) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
        int f5;
        int f6;
        int f7;
        int f8;
        int f9;
        int f10;
        int f11;
        int f12;
        int f13;
        int f14;
    };
    std::stringstream ss;
    ss << foo{
      .f1 = 1,
      .f2 = 2,
      .f3 = 3,
      .f4 = 4,
      .f5 = 5,
      .f6 = 6,
      .f7 = 7,
      .f8 = 8,
      .f9 = 9,
      .f10 = 10,
      .f11 = 11,
      .f12 = 12,
      .f13 = 13,
      .f14 = 14};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4,5,6,7,8,9,10,11,12,13,14");
}

BOOST_AUTO_TEST_CASE(auto_fmt_15) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
        int f5;
        int f6;
        int f7;
        int f8;
        int f9;
        int f10;
        int f11;
        int f12;
        int f13;
        int f14;
        int f15;
    };
    std::stringstream ss;
    ss << foo{
      .f1 = 1,
      .f2 = 2,
      .f3 = 3,
      .f4 = 4,
      .f5 = 5,
      .f6 = 6,
      .f7 = 7,
      .f8 = 8,
      .f9 = 9,
      .f10 = 10,
      .f11 = 11,
      .f12 = 12,
      .f13 = 13,
      .f14 = 14,
      .f15 = 15};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15");
}

BOOST_AUTO_TEST_CASE(auto_fmt_16) {
    struct foo : auto_fmt<foo, ','> {
        int f1;
        int f2;
        int f3;
        int f4;
        int f5;
        int f6;
        int f7;
        int f8;
        int f9;
        int f10;
        int f11;
        int f12;
        int f13;
        int f14;
        int f15;
        int f16;
    };
    std::stringstream ss;
    ss << foo{
      .f1 = 1,
      .f2 = 2,
      .f3 = 3,
      .f4 = 4,
      .f5 = 5,
      .f6 = 6,
      .f7 = 7,
      .f8 = 8,
      .f9 = 9,
      .f10 = 10,
      .f11 = 11,
      .f12 = 12,
      .f13 = 13,
      .f14 = 14,
      .f15 = 15,
      .f16 = 16};
    BOOST_REQUIRE_EQUAL(ss.str(), "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16");
}
