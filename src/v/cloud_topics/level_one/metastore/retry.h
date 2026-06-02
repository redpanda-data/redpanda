/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/metastore/metastore.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>

#include <chrono>
#include <concepts>
#include <expected>

namespace cloud_topics::l1 {

using namespace std::chrono_literals;

static constexpr auto default_metastore_retry_timeout = 5s;
static constexpr auto default_metastore_retry_backoff = 100ms;

inline retry_chain_node make_default_metastore_rtc(ss::abort_source& as) {
    return {
      as,
      ss::lowres_clock::now() + default_metastore_retry_timeout,
      default_metastore_retry_backoff};
}

template<typename Func>
concept metastore_operation = requires(Func f) {
    typename std::invoke_result_t<Func>::value_type;
    requires std::same_as<
      typename std::invoke_result_t<Func>::value_type::error_type,
      metastore::errc>;
};

/// Retry a metastore operation with exponential backoff on transport errors.
template<typename Func>
requires metastore_operation<Func>
auto retry_metastore_op(Func&& func, retry_chain_node& rtc)
  -> ss::future<typename std::invoke_result_t<Func>::value_type> {
    using value_type = typename std::invoke_result_t<Func>::value_type;
    for (auto permit = rtc.retry(); permit.is_allowed; permit = rtc.retry()) {
        auto fut = co_await ss::coroutine::as_future(func());

        if (!fut.failed()) {
            auto result = fut.get();
            if (result.has_value()) {
                co_return result;
            }
            if (result.error() != metastore::errc::transport_error) {
                co_return result;
            }
            // transport_error: fall through to backoff + retry.
        } else {
            // A thrown exception (e.g. a connection abort from the metastore
            // RPC transport) is treated as a transient transport error and
            // retried rather than propagated: in the fetch path an escaped
            // exception is turned into not_leader_for_partition, trapping
            // consumers in an endless metadata-refresh/retry loop. Genuine
            // shutdown exceptions still propagate.
            auto ex = fut.get_exception();
            if (ssx::is_shutdown_exception(ex)) {
                co_await ss::coroutine::return_exception_ptr(std::move(ex));
            }
            // fall through to backoff + retry.
        }

        co_await ss::sleep_abortable(permit.delay, rtc.root_abort_source());
    }

    co_return value_type{std::unexpected(metastore::errc::transport_error)};
}

template<typename Func>
requires metastore_operation<Func>
auto retry_metastore_op_with_default_rtc(Func&& func, ss::abort_source& as)
  -> ss::future<typename std::invoke_result_t<Func>::value_type> {
    auto rtc = make_default_metastore_rtc(as);
    co_return co_await retry_metastore_op(std::forward<Func>(func), rtc);
}

} // namespace cloud_topics::l1
