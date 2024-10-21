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

#pragma once

#include "serde/rw/reservable.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/as_future.hh>

#include <concepts>
#include <ranges>

namespace ssx {

namespace detail {

template<typename Container, typename... Args>
concept emplace_backable = requires(Container c, Args&&... args) {
    c.emplace_back(std::forward<Args>(args)...);
};

} // namespace detail

/// \brief Wait for a range of futures to complete.
///
/// Given a range of futures as input, wait for all of them
/// to resolve, and return a future containing a range with the
/// resolved values of the original futures.
///
/// If any future fails, one of the exceptions will be
/// returned as a failed future.
///
/// \param futures a \c FutureRange containing the futures to wait for.
/// \pre \c futures must be an owning range, i.e. responsible for managing the
/// lifetime of its elements.
///
/// \return a \c future<ResolvedContainer> with all
/// the resolved values of \c futures
template<typename ResolvedContainer, typename FutureRange>
requires seastar::Future<typename FutureRange::value_type>
         && std::ranges::input_range<FutureRange>
         && std::constructible_from<
           typename ResolvedContainer::value_type,
           typename std::ranges::range_value_t<FutureRange>::value_type>
         && detail::emplace_backable<
           ResolvedContainer,
           typename std::ranges::range_value_t<FutureRange>::value_type>
seastar::future<ResolvedContainer> when_all_succeed(FutureRange futures) {
    ResolvedContainer result{};

    if constexpr (serde::Reservable<ResolvedContainer>) {
        if constexpr (std::ranges::sized_range<FutureRange>) {
            result.reserve(std::ranges::size(futures));
        } else if constexpr (std::ranges::forward_range<FutureRange>) {
            result.reserve(std::ranges::distance(futures));
        } else {
            // Do nothing
            // Can't estimate the size of the FutureRange
        }
    }

    std::exception_ptr excp;

    for (auto& fut : futures) {
        auto ready_future = co_await seastar::coroutine::as_future(
          std::move(fut));

        if (excp) {
            ready_future.ignore_ready_future();
            continue;
        }

        if (ready_future.failed()) {
            excp = ready_future.get_exception();
            continue;
        }

        result.emplace_back(ready_future.get());
    }

    if (excp) {
        co_return seastar::coroutine::exception(excp);
    }

    co_return result;
}

} // namespace ssx
