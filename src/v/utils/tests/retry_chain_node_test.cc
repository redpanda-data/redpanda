// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <iostream>
#include <random>

using namespace std::chrono_literals;

static ss::abort_source never_abort;

SEASTAR_THREAD_TEST_CASE(check_fmt) {
    retry_chain_node n1(never_abort, ss::lowres_clock::now() + 100ms, 50ms);
    BOOST_REQUIRE_EQUAL(n1(), "[fiber0|0|100ms]");
    {
        retry_chain_node n2(&n1);
        BOOST_REQUIRE_EQUAL(n2(), "[fiber0~0|0|100ms]");
        {
            retry_chain_node n3(&n2);
            BOOST_REQUIRE_EQUAL(n3(), "[fiber0~0~0|0|100ms]");
            {
                retry_chain_node n4(&n3);
                BOOST_REQUIRE_EQUAL(n4(), "[fiber0~0~0~0|0|100ms]");
            }
            retry_chain_node n5(&n2);
            BOOST_REQUIRE_EQUAL(n5(), "[fiber0~0~1|0|100ms]");
            {
                retry_chain_node n6(&n5);
                BOOST_REQUIRE_EQUAL(n6(), "[fiber0~0~1~0|0|100ms]");
            }
        }
    }
    {
        retry_chain_node n7(&n1);
        BOOST_REQUIRE_EQUAL(n7(), "[fiber0~1|0|100ms]");
    }
    retry_chain_node n8(never_abort, ss::lowres_clock::now() + 100ms, 50ms);
    BOOST_REQUIRE_EQUAL(n8(), "[fiber1|0|100ms]");
    BOOST_REQUIRE_EQUAL(n8("{} + {}", 1, 2), "[fiber1|0|100ms 1 + 2]");
}

SEASTAR_THREAD_TEST_CASE(check_retry1) {
    retry_chain_node n1(never_abort);
    std::array<
      std::pair<ss::lowres_clock::duration, ss::lowres_clock::duration>,
      4>
      intervals = {{
        {100ms, 200ms},
        {200ms, 400ms},
        {400ms, 800ms},
        {800ms, 1600ms},
      }};

    retry_permit permit = n1.retry();
    BOOST_REQUIRE(!permit.is_allowed);
    retry_chain_node n2(ss::lowres_clock::now() + 1600ms, 100ms, &n1);
    for (int i = 0; i < 4; i++) {
        permit = n2.retry();
        BOOST_REQUIRE(permit.is_allowed);
        BOOST_REQUIRE(permit.abort_source == &never_abort);
        BOOST_REQUIRE(permit.delay >= intervals.at(i).first);
        BOOST_REQUIRE(permit.delay < intervals.at(i).second);
    }
    permit = n2.retry();
    BOOST_REQUIRE(!permit.is_allowed);
}

SEASTAR_THREAD_TEST_CASE(check_retry2) {
    ss::abort_source never_abort;
    retry_chain_node n1(never_abort, ss::lowres_clock::now() + 1600ms, 100ms);
    std::array<
      std::pair<ss::lowres_clock::duration, ss::lowres_clock::duration>,
      4>
      intervals = {{
        {100ms, 200ms},
        {200ms, 400ms},
        {400ms, 800ms},
        {800ms, 1600ms},
      }};
    retry_permit permit;
    for (int i = 0; i < 4; i++) {
        permit = n1.retry();
        BOOST_REQUIRE(permit.is_allowed);
        BOOST_REQUIRE(permit.abort_source == &never_abort);
        BOOST_REQUIRE(permit.delay >= intervals.at(i).first);
        BOOST_REQUIRE(permit.delay < intervals.at(i).second);
    }
    permit = n1.retry();
    BOOST_REQUIRE(!permit.is_allowed);

    // n2 should inherit all retry-related stuff
    retry_chain_node n2(&n1);
    for (int i = 0; i < 4; i++) {
        permit = n2.retry();
        BOOST_REQUIRE(permit.is_allowed);
        BOOST_REQUIRE(permit.abort_source == &never_abort);
        BOOST_REQUIRE(permit.delay >= intervals.at(i).first);
        BOOST_REQUIRE(permit.delay < intervals.at(i).second);
    }
    permit = n2.retry();
    BOOST_REQUIRE(!permit.is_allowed);
}

SEASTAR_THREAD_TEST_CASE(check_child_retry_canceled) {
    ss::abort_source as;
    retry_chain_node n1(as, ss::lowres_clock::now() + 1000ms, 100ms);
    retry_chain_node n2(retry_strategy::polling, &n1);
    retry_permit permit = n2.retry();
    BOOST_REQUIRE(permit.is_allowed);
    n1.request_abort();
    BOOST_REQUIRE_THROW(n2.retry(), ss::abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(check_root_retry_canceled) {
    ss::abort_source as;
    retry_chain_node n1(
      as, ss::lowres_clock::now() + 1000ms, 100ms, retry_strategy::polling);
    retry_chain_node n2(&n1);
    retry_permit permit = n1.retry();
    BOOST_REQUIRE(permit.is_allowed);
    n2.request_abort();
    BOOST_REQUIRE_THROW(n1.retry(), ss::abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(check_abort_requested) {
    ss::abort_source as;
    retry_chain_node n1(as, ss::lowres_clock::now() + 1000ms, 100ms);
    retry_chain_node n2(&n1);
    n1.request_abort();
    BOOST_REQUIRE_THROW(n2.check_abort(), ss::abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(check_deadline_propogation_1) {
    retry_chain_node n1(never_abort);
    retry_chain_node n2(1000ms, 100ms, &n1);
    BOOST_REQUIRE(n1.get_timeout() == 0ms);
    BOOST_REQUIRE(n2.get_timeout() == 1000ms);
}

SEASTAR_THREAD_TEST_CASE(check_deadline_propogation_2) {
    retry_chain_node n1(never_abort, 1000ms, 100ms);
    retry_chain_node n2(500ms, 100ms, &n1);
    BOOST_REQUIRE(n1.get_timeout() == 1000ms);
    BOOST_REQUIRE(n2.get_timeout() == 500ms);
}

SEASTAR_THREAD_TEST_CASE(check_deadline_propogation_3) {
    retry_chain_node n1(never_abort, 500ms, 100ms);
    retry_chain_node n2(1000ms, 100ms, &n1);
    BOOST_REQUIRE(n1.get_timeout() == 500ms);
    BOOST_REQUIRE(n2.get_timeout() == 500ms);
}

SEASTAR_THREAD_TEST_CASE(check_node_comparison) {
    ss::abort_source as;
    retry_chain_node r1(as);
    retry_chain_node r2(as);
    retry_chain_node c11(&r1);
    retry_chain_node c111(&c11);
    retry_chain_node c12(&r1);
    retry_chain_node c21(&r2);
    BOOST_REQUIRE(r1.same_root(r1));
    BOOST_REQUIRE(r1.same_root(c11));
    BOOST_REQUIRE(r1.same_root(c111));
    BOOST_REQUIRE(c11.same_root(c12));
    BOOST_REQUIRE(!r1.same_root(r2));
    BOOST_REQUIRE(!r1.same_root(c21));
    BOOST_REQUIRE(!c11.same_root(c21));
}

SEASTAR_THREAD_TEST_CASE(check_tracing) {
    ss::logger test_log("rtc_test_log");
    ss::abort_source as;
    retry_chain_context ctx1("test", as, 0x100);

    retry_chain_node rtc1(ctx1);
    retry_chain_logger rtc_log1(test_log, rtc1);

    // check that the message is printed if logger is
    // on trace
    test_log.set_level(ss::log_level::trace);
    rtc_log1.trace("first message");

    auto found = ctx1.get_trace_log().find("first message")
                 != ss::sstring::npos;
    BOOST_REQUIRE(found);

    // check that the message is printed even if the logger is
    // on info but the message is printed on trace
    test_log.set_level(ss::log_level::info);
    rtc_log1.trace("second message");

    found = ctx1.get_trace_log().find("second message") != ss::sstring::npos;
    BOOST_REQUIRE(found);

    std::vector<ss::sstring> actual;
    std::vector<ss::sstring> expected = {
      "first message",
      "second message",
    };
    for (const auto& trace : ctx1.traces()) {
        actual.push_back(trace);
    }
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      actual.begin(), actual.end(), expected.begin(), expected.end());

    // check that bypass_tracing method works correctly
    rtc_log1.bypass_tracing([&] {
        // this shouldn't be added to the trace
        rtc_log1.trace("unexpected message");
    });

    found = ctx1.get_trace_log().find("unexpected message")
            != ss::sstring::npos;
    BOOST_REQUIRE(!found);

    // check that reset works
    ctx1.reset();
    found = ctx1.get_trace_log().find("message") != ss::sstring::npos;
    BOOST_REQUIRE(!found);

    // check nested rtc
    {
        retry_chain_node rtc2(&rtc1);
        retry_chain_logger rtc_log2(test_log, rtc2);

        rtc_log2.trace("third message");
    }
    found = ctx1.get_trace_log().find("third message") != ss::sstring::npos;
    BOOST_REQUIRE(found);

    // check truncation
    for (int i = 0; i < 20; i++) {
        rtc_log1.trace("long message");
    }
    BOOST_REQUIRE(ctx1.truncation_warning());

    // check abort source functionality
    ctx1.as().request_abort();
    BOOST_REQUIRE_THROW(rtc1.check_abort(), ss::abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(test_circular_buffer_basic_operations) {
    detail::rtc_circular_buffer buf(3);

    // Test initial state
    BOOST_REQUIRE(buf.empty());
    BOOST_REQUIRE_EQUAL(buf.size(), 0);
    BOOST_REQUIRE_EQUAL(buf.capacity(), 3);
    BOOST_REQUIRE(!buf.full());
    BOOST_REQUIRE_EQUAL(buf.overwritten(), 0);

    // Test push_back and access
    buf.push_back(1);
    BOOST_REQUIRE_EQUAL(buf.size(), 1);
    BOOST_REQUIRE_EQUAL(buf[0], 1);
    BOOST_REQUIRE_EQUAL(buf.front(), 1);
    BOOST_REQUIRE_EQUAL(buf.back(), 1);

    buf.push_back(2);
    buf.push_back(3);
    BOOST_REQUIRE(buf.full());
    BOOST_REQUIRE_EQUAL(buf.size(), 3);
    BOOST_REQUIRE_EQUAL(buf[0], 1);
    BOOST_REQUIRE_EQUAL(buf[1], 2);
    BOOST_REQUIRE_EQUAL(buf[2], 3);
    BOOST_REQUIRE_EQUAL(buf.front(), 1);
    BOOST_REQUIRE_EQUAL(buf.back(), 3);
}

SEASTAR_THREAD_TEST_CASE(test_circular_buffer_wraparound) {
    detail::rtc_circular_buffer buf(3);

    // Fill buffer
    buf.push_back(1);
    buf.push_back(2);
    buf.push_back(3);

    // Test wraparound - should overwrite oldest element
    buf.push_back(4);
    BOOST_REQUIRE_EQUAL(buf.size(), 3);
    BOOST_REQUIRE_EQUAL(buf.overwritten(), 1);
    BOOST_REQUIRE_EQUAL(buf[0], 2); // oldest is now 2
    BOOST_REQUIRE_EQUAL(buf[1], 3);
    BOOST_REQUIRE_EQUAL(buf[2], 4); // newest is 4
    BOOST_REQUIRE_EQUAL(buf.front(), 2);
    BOOST_REQUIRE_EQUAL(buf.back(), 4);

    // Continue wraparound
    buf.push_back(5);
    buf.push_back(6);
    BOOST_REQUIRE_EQUAL(buf.overwritten(), 3);
    BOOST_REQUIRE_EQUAL(buf[0], 4);
    BOOST_REQUIRE_EQUAL(buf[1], 5);
    BOOST_REQUIRE_EQUAL(buf[2], 6);
}

SEASTAR_THREAD_TEST_CASE(test_circular_buffer_bounds_checking) {
    detail::rtc_circular_buffer buf(2);
    buf.push_back(1);

    // Valid access
    BOOST_REQUIRE_EQUAL(buf.at(0), 1);

    // Invalid access should throw
    BOOST_REQUIRE_THROW(buf.at(1), std::out_of_range);
    BOOST_REQUIRE_THROW(buf.at(2), std::out_of_range);
}

SEASTAR_THREAD_TEST_CASE(test_circular_buffer_string_view_specialization) {
    detail::rtc_circular_buffer buf(10);

    // Test empty string view
    buf.push_back(std::string_view{});
    BOOST_REQUIRE_EQUAL(buf.size(), 0);

    // Test normal string view
    ss::sstring test_str = "hello";
    buf.push_back(std::string_view{test_str});
    BOOST_REQUIRE_EQUAL(buf.size(), 5);

    // Check iteration
    std::string result;
    for (size_t i = 0; i < buf.size(); ++i) {
        result.push_back(buf[i]);
    }
    BOOST_REQUIRE_EQUAL(result, test_str);

    // Test string larger than capacity
    buf.clear();
    std::string large_str = "this_is_a_very_long_string";
    buf.push_back(std::string_view{large_str});
    BOOST_REQUIRE_EQUAL(buf.size(), buf.capacity());

    // Should contain only the last 'capacity' characters
    result.clear();
    for (size_t i = 0; i < buf.size(); ++i) {
        result.push_back(buf[i]);
    }
    BOOST_REQUIRE_EQUAL(
      result, large_str.substr(large_str.size() - buf.capacity()));
}

SEASTAR_THREAD_TEST_CASE(test_circular_buffer_string_view_wraparound) {
    detail::rtc_circular_buffer buf(8);

    // Fill buffer partially
    buf.push_back(std::string_view{"abc"});
    BOOST_REQUIRE_EQUAL(buf.size(), 3);

    // Add string that causes wraparound
    buf.push_back(std::string_view{"redpanda"});
    BOOST_REQUIRE_EQUAL(buf.size(), 8);
    BOOST_REQUIRE(buf.overwritten() > 0);

    std::string result;
    for (size_t i = 0; i < buf.size(); ++i) {
        result.push_back(buf[i]);
    }
    BOOST_REQUIRE_EQUAL(result, "redpanda");

    buf.push_back(std::string_view{"head"});
    // this is supposed to hit case two: not enough space, need two memcpy calls
    buf.push_back(std::string_view{"1234567"});

    result.clear();
    for (size_t i = 0; i < buf.size(); ++i) {
        result.push_back(buf[i]);
    }
    BOOST_REQUIRE_EQUAL(result, "d1234567");
}

SEASTAR_THREAD_TEST_CASE(test_circular_buffer_clear) {
    detail::rtc_circular_buffer buf(3);

    buf.push_back(1);
    buf.push_back(2);
    buf.push_back(3);
    buf.push_back(4); // causes overwrite

    BOOST_REQUIRE(!buf.empty());
    BOOST_REQUIRE(buf.overwritten() > 0);

    buf.clear();

    BOOST_REQUIRE(buf.empty());
    BOOST_REQUIRE_EQUAL(buf.size(), 0);
    BOOST_REQUIRE_EQUAL(buf.overwritten(), 0);
    BOOST_REQUIRE(!buf.full());
}

SEASTAR_THREAD_TEST_CASE(test_circular_buffer_view) {
    detail::rtc_circular_buffer buf(5);

    buf.push_back('a');
    buf.push_back('b');
    buf.push_back('c');

    auto view = buf.view();

    // Check that view contains correct data
    std::string result(view.begin(), view.end());
    BOOST_REQUIRE_EQUAL(result, "abc");

    // Test with wraparound
    buf.push_back('d');
    buf.push_back('e');
    buf.push_back('f'); // should overwrite 'a'

    view = buf.view();
    result = std::string(view.begin(), view.end());
    BOOST_REQUIRE_EQUAL(result, "bcdef");
}
