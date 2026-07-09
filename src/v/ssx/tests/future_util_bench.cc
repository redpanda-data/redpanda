// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/testing/perf_tests.hh>

#include <stdexcept>

namespace {

constexpr size_t iterations = 10000;

template<typename Func>
ss::future<size_t> co_await_in_loop(Func func) {
    perf_tests::start_measuring_time();
    for (size_t i = 0; i < iterations; ++i) {
        co_await func();
    }
    perf_tests::stop_measuring_time();
    co_return iterations;
}

// Input producers for ignore_shutdown_exceptions.
//
// Keep these out of line to preserve the producer call in each measured
// iteration. Already-resolved futures are the common path being measured.

// Available, successful future.
[[gnu::noinline]] ss::future<> input_ready() { return ss::now(); }

// Available future failed with a gate_closed_exception (a shutdown exception).
[[gnu::noinline]] ss::future<> input_gate_closed() {
    return ss::make_exception_future<>(seastar::gate_closed_exception{});
}

// Available future failed with an abort_requested_exception (a shutdown
// exception).
[[gnu::noinline]] ss::future<> input_abort_requested() {
    return ss::make_exception_future<>(seastar::abort_requested_exception{});
}

// Available future failed with a std::runtime_error: a non-shutdown exception
// that ignore_shutdown_exceptions must rethrow to the caller.
[[gnu::noinline]] ss::future<> input_runtime_error() {
    return ss::make_exception_future<>(std::runtime_error("not shutdown"));
}

struct future_util_bench {};

ss::future<> ignore_shutdown_ready() {
    return ssx::ignore_shutdown_exceptions(input_ready());
}

ss::future<> ignore_shutdown_gate_closed() {
    return ssx::ignore_shutdown_exceptions(input_gate_closed());
}

ss::future<> ignore_shutdown_abort_requested() {
    return ssx::ignore_shutdown_exceptions(input_abort_requested());
}

// ignore_shutdown_exceptions rethrows non-shutdown exceptions, so swallow the
// result to keep the loop running. The trailing handle_exception is common-mode
// overhead (present for any caller that handles the rethrow), so the delta
// between builds still isolates the filter/rethrow cost.
ss::future<> ignore_shutdown_non_shutdown() {
    return ssx::ignore_shutdown_exceptions(input_runtime_error())
      .handle_exception([](std::exception_ptr) {});
}

} // namespace

PERF_TEST_F(future_util_bench, ignore_shutdown_ready) {
    return co_await_in_loop(ignore_shutdown_ready);
}

PERF_TEST_F(future_util_bench, ignore_shutdown_gate_closed) {
    return co_await_in_loop(ignore_shutdown_gate_closed);
}

PERF_TEST_F(future_util_bench, ignore_shutdown_abort_requested) {
    return co_await_in_loop(ignore_shutdown_abort_requested);
}

PERF_TEST_F(future_util_bench, ignore_shutdown_non_shutdown) {
    return co_await_in_loop(ignore_shutdown_non_shutdown);
}
