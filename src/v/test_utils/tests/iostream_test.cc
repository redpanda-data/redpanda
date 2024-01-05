/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "test_utils/iostream.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/short_streams.hh>

#include <boost/test/tools/assertion.hpp>
#include <boost/test/tools/context.hpp>
#include <boost/test/tools/interface.hpp>

SEASTAR_TEST_CASE(test_varying_buffer_input_stream) {
    static constexpr std::string_view input
      = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec "
        "vulputate egestas est a condimentum. Nullam pretium maximus ipsum non "
        "molestie. Integer commodo turpis dolor, vel bibendum diam sodales eu. "
        "Aliquam mollis nisi a bibendum mattis. Nam a tempor dui. Vivamus "
        "porta "
        "mauris ac augue pretium ultrices ut sed felis. Pellentesque rutrum eu "
        "mauris a accumsan. Fusce feugiat enim lorem, vitae hendrerit libero "
        "faucibus vel. Vivamus felis augue, euismod vel accumsan a, auctor et "
        "neque. Quisque non magna condimentum, sodales libero vitae, vulputate "
        "metus. Aliquam ullamcorper dolor erat, quis rhoncus turpis tristique "
        "ac. Aliquam erat volutpat. Proin condimentum ligula vitae velit "
        "ultricies eleifend.";

    for (auto i = 0; i < 10; i++) {
        auto in = tests::varying_buffer_input_stream::create(
          input, 1, input.size() + 2);

        BOOST_TEST_INFO("Iteration " << i);
        BOOST_TEST_CHECK(
          co_await seastar::util::read_entire_stream_contiguous(in) == input);
    }
}
