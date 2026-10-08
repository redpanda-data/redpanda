// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/future-util.h"

#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;

SEASTAR_THREAD_TEST_CASE(do_with_fwd_rvalue) {
    // Rvalue: should be moved into managed storage.
    std::vector<int> v = {1, 2, 3};
    auto f = ssx::do_with_fwd(std::move(v), [](std::vector<int>& moved) {
        BOOST_CHECK_EQUAL(moved.size(), 3);
        return seastar::make_ready_future<>();
    });
    f.get();
    // NOLINTNEXTLINE(bugprone-use-after-move) intentional: verify v was moved
    BOOST_CHECK(v.empty());
}

SEASTAR_THREAD_TEST_CASE(do_with_fwd_lvalue) {
    // Single lvalue: should be passed by reference, not copied.
    int x = 42;
    auto f = ssx::do_with_fwd(x, [](int& ref) {
        ref = 99;
        return seastar::make_ready_future<>();
    });
    f.get();
    BOOST_CHECK_EQUAL(x, 99);
}

SEASTAR_THREAD_TEST_CASE(do_with_fwd_async) {
    // Verify lvalue ref survives across a suspension point.
    int x = 0;
    auto f = ssx::do_with_fwd(x, [](int& ref) {
        return seastar::sleep(1ms).then([&ref] { ref = 77; });
    });
    f.get();
    BOOST_CHECK_EQUAL(x, 77);
}

SEASTAR_THREAD_TEST_CASE(with_timeout_abortable_test) {
    // future ready -> timeout
    {
        auto f = seastar::make_ready_future<int>(123);
        seastar::abort_source as;

        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::manual_clock::now() + 100ms, as);

        BOOST_CHECK_EQUAL(res.get(), 123);
    }

    // future ready -> timeout -> abort
    {
        auto f = seastar::sleep<seastar::manual_clock>(50ms).then(
          [] { return seastar::make_ready_future<int>(123); });

        seastar::abort_source as;
        auto abort_f = seastar::sleep<seastar::manual_clock>(150ms).then(
          [&as] { as.request_abort(); });

        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::manual_clock::now() + 100ms, as);

        seastar::manual_clock::advance(50ms);
        BOOST_CHECK_EQUAL(res.get(), 123);
        seastar::manual_clock::advance(100ms);
        abort_f.get();
    }

    // timeout -> future ready -> abort
    {
        auto f = seastar::sleep<seastar::manual_clock>(150ms).then(
          [] { return seastar::make_ready_future<int>(123); });

        seastar::abort_source as;
        auto abort_f = seastar::sleep<seastar::manual_clock>(200ms).then(
          [&as] { as.request_abort(); });

        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::manual_clock::now() + 100ms, as);

        seastar::manual_clock::advance(100ms);
        BOOST_CHECK_THROW(res.get(), seastar::timed_out_error);
        seastar::manual_clock::advance(100ms);
        abort_f.get();
    }

    // abort -> timeout -> future ready
    {
        auto f = seastar::sleep<seastar::manual_clock>(150ms).then(
          [] { return seastar::make_ready_future<int>(123); });

        seastar::abort_source as;
        auto abort_f = seastar::sleep<seastar::manual_clock>(50ms).then(
          [&as] { as.request_abort(); });

        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::manual_clock::now() + 100ms, as);

        seastar::manual_clock::advance(50ms);
        abort_f.get();
        BOOST_CHECK_THROW(res.get(), seastar::abort_requested_exception);
        seastar::manual_clock::advance(100ms);
    }

    // abort -> future ready -> timeout
    {
        auto f = seastar::sleep<seastar::manual_clock>(100ms).then(
          [] { return seastar::make_ready_future<int>(123); });

        seastar::abort_source as;
        auto abort_f = seastar::sleep<seastar::manual_clock>(50ms).then(
          [&as] { as.request_abort(); });

        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::manual_clock::now() + 150ms, as);

        seastar::manual_clock::advance(50ms);
        abort_f.get();

        BOOST_CHECK_THROW(res.get(), seastar::abort_requested_exception);
        seastar::manual_clock::advance(50ms);
    }

    // abort -> future created -> timeout
    {
        seastar::abort_source as;
        as.request_abort();

        auto f = seastar::sleep<seastar::manual_clock>(50ms).then(
          [] { return seastar::make_ready_future<int>(123); });
        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::manual_clock::now() + 100ms, as);

        seastar::manual_clock::advance(100ms);
        BOOST_CHECK_THROW(res.get(), seastar::abort_requested_exception);
    }
}

SEASTAR_THREAD_TEST_CASE(with_timeout_abortable_custom_ex_test) {
    // It's possible to throw custom exceptions with abort_source, make sure
    // we propagate them correctly.
    class custom_abort_exception : public std::exception {};
    {
        // Already aborted source
        seastar::abort_source as;
        as.request_abort_ex(custom_abort_exception{});

        auto f = seastar::sleep<seastar::manual_clock>(50ms).then(
          [] { return seastar::make_ready_future<int>(123); });
        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::manual_clock::time_point::max(), as);
        BOOST_CHECK_THROW(res.get(), custom_abort_exception);
        // f has to resolve or LSAN thinks there are leaks
        seastar::manual_clock::advance(50ms);
    }
    {
        seastar::abort_source as;

        auto f = seastar::sleep<seastar::manual_clock>(50ms).then(
          [] { return seastar::make_ready_future<int>(123); });
        auto res = ssx::with_timeout_abortable(
          std::move(f), seastar::manual_clock::time_point::max(), as);

        seastar::manual_clock::advance(10ms);
        as.request_abort_ex(custom_abort_exception{});
        BOOST_CHECK_THROW(res.get(), custom_abort_exception);
        // f has to resolve or LSAN thinks there are leaks
        seastar::manual_clock::advance(40ms);
    }
}
