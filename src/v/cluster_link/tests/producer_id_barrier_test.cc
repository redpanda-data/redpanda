/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/producer_id_barrier.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>

#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>

namespace cluster_link::tests {

namespace {

class fake_ops final : public producer_id_barrier_impl::ops {
public:
    ss::future<result<::model::producer_id, errc>> scan_highest_pid() final {
        ++scans;
        if (scan_throws) {
            throw std::runtime_error("scan failure");
        }
        co_return scan_result;
    }

    ss::future<errc> reset_next_id(::model::producer_id pid) final {
        ++resets;
        last_reset = pid;
        co_return reset_result;
    }

    int scans{0};
    int resets{0};
    ::model::producer_id last_reset{};
    result<::model::producer_id, errc> scan_result{::model::producer_id{0}};
    errc reset_result{errc::success};
    bool scan_throws{false};
};

struct barrier_under_test {
    barrier_under_test() {
        auto o = std::make_unique<fake_ops>();
        ops = o.get();
        barrier = std::make_unique<producer_id_barrier_impl>(std::move(o));
    }
    fake_ops* ops;
    std::unique_ptr<producer_id_barrier_impl> barrier;
};

} // namespace

TEST_CORO(producer_id_barrier, resets_even_when_scanned_max_is_zero) {
    // Producer ID 0 is valid and indistinguishable from "no producers
    // found"; the barrier must reset in both cases.
    barrier_under_test t;
    auto res = co_await t.barrier->advance();
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value(), ::model::producer_id{0});
    ASSERT_EQ_CORO(t.ops->resets, 1);
    ASSERT_EQ_CORO(t.ops->last_reset, ::model::producer_id{1000});
}

TEST_CORO(producer_id_barrier, returns_scanned_max_and_adds_margin) {
    barrier_under_test t;
    t.ops->scan_result = ::model::producer_id{41};
    auto res = co_await t.barrier->advance();
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value(), ::model::producer_id{41});
    ASSERT_EQ_CORO(t.ops->last_reset, ::model::producer_id{1041});
}

TEST_CORO(producer_id_barrier, near_overflow_is_terminal_exhaustion) {
    barrier_under_test t;
    t.ops->scan_result = ::model::producer_id{
      std::numeric_limits<int64_t>::max() - 1};
    auto res = co_await t.barrier->advance();
    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), errc::producer_id_exhausted);
    ASSERT_EQ_CORO(t.ops->resets, 0);
    // Terminal and cached: a second advance must not rescan.
    auto again = co_await t.barrier->advance();
    ASSERT_TRUE_CORO(again.has_error());
    ASSERT_EQ_CORO(again.error(), errc::producer_id_exhausted);
    ASSERT_EQ_CORO(t.ops->scans, 1);
}

TEST_CORO(producer_id_barrier, scan_error_means_no_reset) {
    barrier_under_test t;
    t.ops->scan_result = errc::rpc_error;
    auto res = co_await t.barrier->advance();
    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), errc::rpc_error);
    ASSERT_EQ_CORO(t.ops->resets, 0);
    // Not terminal: a later attempt scans again.
    auto again = co_await t.barrier->advance();
    ASSERT_TRUE_CORO(again.has_error());
    ASSERT_EQ_CORO(t.ops->scans, 2);
}

TEST_CORO(producer_id_barrier, reset_error_is_not_success) {
    barrier_under_test t;
    t.ops->reset_result = errc::rpc_error;
    auto res = co_await t.barrier->advance();
    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), errc::rpc_error);
}

TEST_CORO(producer_id_barrier, exceptions_become_error_codes) {
    barrier_under_test t;
    t.ops->scan_throws = true;
    // Must not surface as an exceptional future — the caller is a noexcept
    // path.
    auto res = co_await t.barrier->advance();
    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), errc::rpc_error);
}

TEST_CORO(producer_id_barrier, shutdown_aborts_advances) {
    barrier_under_test t;
    t.barrier->shutdown();
    auto res = co_await t.barrier->advance();
    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), errc::service_shutting_down);
    ASSERT_EQ_CORO(t.ops->scans, 0);
}

} // namespace cluster_link::tests
