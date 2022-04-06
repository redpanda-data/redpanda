/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "model/record.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>

#include <functional>
#include <tuple>

namespace coproc {

namespace internal {

template<typename Until, typename Func, typename T>
inline auto async_foldl_until(Until&&, Func&& func, T& element) {
    return func(element);
}

template<typename Until, typename Func, typename T, typename... Ts>
inline auto
async_foldl_until(Until&& stop, Func&& func, T& element, Ts&... elements) {
    auto f = func(element);
    return f.then([stop = std::forward<Until>(stop),
                   func = std::forward<Func>(func),
                   &elements...](auto val) mutable {
        if (stop(val)) {
            return ss::make_ready_future<decltype(val)>(val);
        }
        return async_foldl_until(
          std::forward<Until>(stop), std::forward<Func>(func), elements...);
    });
}

} // namespace internal

template<typename... RefConsumers>
class reference_window_consumer {
public:
    explicit reference_window_consumer(RefConsumers... c)
      : _ref_consumers(std::make_tuple(std::forward<RefConsumers>(c)...)) {}

    reference_window_consumer(const reference_window_consumer&) = delete;
    reference_window_consumer(reference_window_consumer&&) noexcept = default;
    reference_window_consumer& operator=(const reference_window_consumer&)
      = delete;
    reference_window_consumer& operator=(reference_window_consumer&&) = delete;

    virtual ~reference_window_consumer() = default;

    /**
     * Calls operator() on each Reference consumer, passing in the
     * record_batch by reference. Returns a stop_iteration which is the
     * aggregate of all of the return values from operator() from each
     * RefConsumer called
     */
    ss::future<ss::stop_iteration> operator()(model::record_batch& b) {
        return std::apply(
          [&b](RefConsumers&... cs) {
              return internal::async_foldl_until(
                [](auto stop) { return stop == ss::stop_iteration::yes; },
                [&b](auto& c) { return c.operator()(b); },
                cs...);
          },
          _ref_consumers);
    }

    /**
     * Returns a std::tuple<> with each argument being the return
     * value of 'end_of_stream()' called on the particular RefConsumer from the
     * input template argument list at index i
     */
    auto end_of_stream() {
        return std::apply(
          [](RefConsumers&... cs) {
              return std::make_tuple(cs.end_of_stream()...);
          },
          _ref_consumers);
    }

private:
    std::tuple<RefConsumers...> _ref_consumers;
};

} // namespace coproc
