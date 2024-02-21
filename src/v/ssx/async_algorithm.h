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

#include "base/seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <iterator>
#include <limits>

//
// async_algorithms.h
//
// These are implementations of a few algorithms, similar in nature to those in
// <algorithm>, but which are async-friendly in the sense that they yield
// periodically.
//
// For the simplest use cases we offer free functions like async_for_each, which
// is the same as for_each except that it yields every Traits::interval
// iterations: every call starts the counter anew.
//
// For more complicated cases, like nested loops, it may be convenient to carry
// the counter from one iteration to the next, in order to yield at the correct
// times. For example, in a doubly nested loop where the inner loop always ran
// for less than interval iterations, we would not yield with the simple
// functions.

namespace ssx {

struct async_counter {
    // internal details, don't rely on the values
    ssize_t count = 0;
};

struct async_algo_traits {
    // The number of elements processed before trying to yield
    constexpr static ssize_t interval = 100;
    // Called every time after we call maybe_yield, useful to
    // introspect the behavior in tests.
    static void yield_called() {}
};

namespace detail {

// value-semantic counter for when no external counter was passed
struct internal_counter {
    ssize_t count;
};

// ref-semantic counter for when an external counter was passed
// we need to jump though these semantics since using a ref-counter
// for the internal case would lead to a lifetime issue as the counter
// lives in the stack frame of a non-coroutine and is passed into a
// coroutine, which will UAF on suspension
struct ref_counter {
    ssize_t& count;
};

template<typename Traits, typename C>
static ssize_t remaining(const C& c) {
    // amount of work we can do before yielding
    return std::max(Traits::interval - c.count, (ssize_t)0);
}

/**
 * The fixed cost of just calling the async helper methods: it is
 * mostly important that this is non-zero so if you call the counter
 * version of the async methods on tons of empty containers, you do
 * yield: otherwise we would never yield in this case.
 *
 */
constexpr ssize_t FIXED_COST = 1;

template<
  typename Traits,
  typename Counter,
  typename Fn,
  std::random_access_iterator Iterator>
ss::future<>
async_for_each_coro(Counter counter, Iterator begin, Iterator end, Fn f) {
    do {
        auto chunk_size = std::min(remaining<Traits>(counter), end - begin);
        Iterator chunk_end = begin + chunk_size;
        std::for_each(begin, chunk_end, f);
        begin = chunk_end;
        counter.count += chunk_size;
        if (counter.count >= Traits::interval) {
            co_await ss::coroutine::maybe_yield();
            counter.count = 0;
            Traits::yield_called();
        }
    } while (begin != end);

    counter.count += FIXED_COST;
}

/**
 * Helper to combine the internal and external counter implementations.
 */
template<
  typename Traits = async_algo_traits,
  typename Counter,
  typename Fn,
  std::random_access_iterator Iterator>
ss::future<>
async_for_each_fast(Counter counter, Iterator begin, Iterator end, Fn f) {
    // This first part is an important optimization: if the input range is small
    // enough, we don't want to create a coroutine frame as that's costly, so
    // this function is not coroutine and we do the whole iteration here (as we
    // won't yield), otherwise we defer to the coroutine-based helper.
    if (auto total_size = (end - begin) + FIXED_COST;
        total_size <= detail::remaining<Traits>(counter)) {
        std::for_each(begin, end, std::move(f));
        counter.count += total_size;
        return ss::make_ready_future();
    }

    return async_for_each_coro<Traits>(counter, begin, end, std::move(f));
}

} // namespace detail

/**
 * @brief Call f on every element, yielding occasionally.
 *
 * This is equivalent to std::for_each, except that the computational
 * loop yields every Traits::interval (default 100) iterations in order
 * to avoid reactor stalls. The returned future resolves when all elements
 * have been processed.
 *
 * The function is taken by value.
 *
 * The iterators must remain valid until the returned future resolves.
 *
 * @param begin the beginning of the range to process
 * @param end the end of the range to process
 * @param f the function to call on each element
 * @return ss::future<> a future which resolves when all elements have been
 * processed
 */
template<
  typename Traits = async_algo_traits,
  typename Fn,
  std::random_access_iterator Iterator>
ss::future<> async_for_each(Iterator begin, Iterator end, Fn f) {
    return async_for_each_fast<Traits>(
      detail::internal_counter{}, begin, end, std::move(f));
}

/**
 * @brief Call f on every element, yielding occasionally and accepting
 * an externally provided counter for yield control.
 *
 * This is equivalent to std::for_each, except that the computational
 * loop yields every Traits::interval (default 100) iterations in order
 * to avoid reactor stalls. The returned future resolves when all elements
 * have been processed.
 *
 * This behaves similarly to async_for_each except that the counter used to
 * track how much work as been done since the last attempted yield is passed
 * in by the caller. This allows use in more complex cases such as nested loops
 * where the inner loop may itself not do sufficient work to ever trigger the
 * yield condition, even though the total amount of work done across all
 iterations
 * is very high.
 *
 * Use case:
 *
 * Replace something like:
 *
 * for (... outer loop ...) {
 *   for (... inner loop ...) {
 *      f(elem);
 *   }
 * }
 * with:
 *
 * async_counter counter;
 * for (... outer loop ...) {
 *    co_await async_for_each(counter, begin, end, f);
 * }
 *
 * The counter is taken by reference and must live at least until the
 * returned future resolves: this usually trivial when the caller is a
 * coroutine but may require some care when continuation style is used.
 *
 * The function is taken by value.
 *
 * The iterators must remain valid until the returned future resolves.
 *
 * @param begin the beginning of the range to process
 * @param end the end of the range to process
 * @param f the function to call on each element
 * @return ss::future<> a future which resolves when all elements have been
 * processed
 */
template<
  typename Traits = async_algo_traits,
  typename Fn,
  std::random_access_iterator Iterator>
ss::future<> async_for_each_counter(
  async_counter& counter, Iterator begin, Iterator end, Fn f) {
    return detail::async_for_each_fast<Traits>(
      detail::ref_counter{counter.count}, begin, end, std::move(f));
}

} // namespace ssx
