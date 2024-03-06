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

#define BOOST_TEST_MODULE admin_util
#include "redpanda/admin/util.h"

#include <seastar/core/sstring.hh>

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_path_encode_unchanged) {
    auto unchanged_chars = seastar::sstring{
      "~abcdefghijklmnopqrstuvwhyz-ABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789.+"};
    auto result = seastar::sstring{};
    auto success = admin::path_decode(unchanged_chars, result);

    BOOST_REQUIRE(success);
    BOOST_REQUIRE_EQUAL(result, unchanged_chars);
}

BOOST_AUTO_TEST_CASE(test_path_encode_changed) {
    auto changed_chars = seastar::sstring{"%20"};
    auto result = seastar::sstring{};
    auto success = admin::path_decode(changed_chars, result);

    BOOST_REQUIRE(success);

    auto expected_chars = seastar::sstring{" "};
    BOOST_REQUIRE_EQUAL(result, expected_chars);
}
