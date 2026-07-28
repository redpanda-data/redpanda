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
#include <type_traits>

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
inline ssize_t remaining(const C& c) {
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

template<typename I>
struct iter_size {
    I iter;
    ssize_t count;
};

/**
 * A mix of for_each and for_each_n: iterates from begin to end, or until
 * limit elements have been visited, whichever comes first, applying f to
 * each element.
 *
 * Returns the number of elements visited as well as the iterator to the first
 * unvisited element. This can be implemented more efficiently with random
 * access iterators since we can calculate the exact end iterator up front and
 * so do an efficient loop with a single sentinel. The forward iterator version
 * must increment an count in the loop and check both end iterator and counter
 * as the termination condition.
 */
template<
  std::random_access_iterator I,
  std::sized_sentinel_for<I> S,
  typename Fn>
iter_size<I> for_each_limit(const I begin, const S end, ssize_t limit, Fn f) {
    auto chunk_size = std::min(limit, end - begin);
    I chunk_end = begin + chunk_size;
    for (I i = begin; i != chunk_end; ++i) {
        f(*i);
    }
    return {chunk_end, chunk_size};
}

template<std::forward_iterator I, std::sentinel_for<I> S, typename Fn>
iter_size<I> for_each_limit(const I begin, const S end, ssize_t limit, Fn f) {
    ssize_t count = 0;
    auto i = begin;
    while (i != end && count < limit) {
        f(*i);
        ++i;
        ++count;
    }
    return {i, count};
}

template<
  typename Traits,
  typename Counter,
  typename Fn,
  std::forward_iterator Iterator,
  std::sentinel_for<Iterator> EndIterator>
ss::future<>
async_for_each_coro(Counter counter, Iterator begin, EndIterator end, Fn f) {
    do {
        auto new_begin = for_each_limit(
          begin, end, remaining<Traits>(counter), f);
        begin = new_begin.iter;
        counter.count += new_begin.count;
        if (counter.count >= Traits::interval) {
            co_await ss::coroutine::maybe_yield();
            counter.count = 0;
            Traits::yield_called();
        }
    } while (begin != end);
}

/**
 * Helper to combine the internal and external counter implementations.
 */
template<
  typename Traits = async_algo_traits,
  typename Counter,
  typename Fn,
  std::forward_iterator Iterator,
  std::sentinel_for<Iterator> EndIterator>
ss::future<>
async_for_each_fast(Counter counter, Iterator begin, EndIterator end, Fn f) {
    // This first part is an important optimization: if the input range is small
    // enough, we don't want to create a coroutine frame as that's costly, so
    // this function is not coroutine and we do the whole iteration here (as we
    // won't yield), otherwise we defer to the coroutine-based helper.

    ssize_t limit = detail::remaining<Traits>(counter);
    auto new_begin = for_each_limit(begin, end, limit, f);
    counter.count += new_begin.count + FIXED_COST;
    if (new_begin.iter == end && counter.count < Traits::interval) [[likely]] {
        return ss::make_ready_future();
    }

    return async_for_each_coro<Traits>(
      counter, new_begin.iter, end, std::move(f));
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
  std::forward_iterator Iterator,
  std::sentinel_for<Iterator> EndIterator>
ss::future<> async_for_each(Iterator begin, EndIterator end, Fn f) {
    return async_for_each_fast<Traits>(
      detail::internal_counter{}, begin, end, std::move(f));
}

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
 * Until the returned future resolves, the ranges begin and end iterators,
 * as they were when called, and the range itself must remain valid.
 *
 * @param range universal reference to range
 * @param f the function to call on each element
 * @return ss::future<> a future which resolves when all elements have been
 * processed
 */
template<
  typename Traits = async_algo_traits,
  typename Fn,
  std::ranges::input_range Range>
requires requires(Range c, Fn fn) {
    {
        ss::futurize_invoke(fn, *std::ranges::begin(c))
    } -> std::same_as<ss::future<>>;
    std::end(c);
}
ss::future<> async_for_each(Range&& range, Fn f) {
    return async_for_each_fast<Traits>(
      detail::internal_counter{},
      std::ranges::begin(range),
      std::ranges::end(range),
      std::move(f));
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
 * @param counter the counter object to use, may be reused across invocations
 * @param begin the beginning of the range to process
 * @param end the end of the range to process
 * @param f the function to call on each element
 * @return ss::future<> a future which resolves when all elements have been
 * processed
 */
template<
  typename Traits = async_algo_traits,
  typename Fn,
  std::forward_iterator Iterator>
ss::future<> async_for_each_counter(
  async_counter& counter, Iterator begin, Iterator end, Fn f) {
    return detail::async_for_each_fast<Traits>(
      detail::ref_counter{counter.count}, begin, end, std::move(f));
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
 * Until the returned future resolves, the container's begin and end iterators,
 * as they were when called, and the container itself must remain valid.
 *
 * @param counter the counter object to use, may be reused across invocations
 * @param container universal reference to container
 * @param f the function to call on each element
 * @return ss::future<> a future which resolves when all elements have been
 * processed
 */
template<typename Traits = async_algo_traits, typename Fn, typename Container>
requires requires(Container c, Fn fn) {
    { ss::futurize_invoke(fn, *std::begin(c)) } -> std::same_as<ss::future<>>;
    std::end(c);
}
ss::future<>
async_for_each_counter(async_counter& counter, Container&& container, Fn f) {
    return detail::async_for_each_fast<Traits>(
      counter, std::begin(container), std::end(container), std::move(f));
}

/**
 * @brief Call f until cond is false, yielding occasionally and accepting
 * an externally provided counter for yield control.
 *
 * This is equivalent to while (cond()) f(); except that the computational
 * loop yields every Traits::interval (default 100) iterations in order
 * to avoid reactor stalls. The returned future resolves when cond() is false.
 *
 * The counter is taken by reference and must live at least until the
 * returned future resolves: this usually trivial when the caller is a
 * coroutine but may require some care when continuation style is used.
 *
 * The functions are taken by value.
 *
 * @param counter the counter object to use, may be reused across invocations
 * @param cond the synchronous nullary predicate to call to check the condition
 * @param f the synchronous nullary function to call on each element
 * @return ss::future<> a future which resolves when !cond()
 */
template<
  typename Traits = async_algo_traits,
  std::predicate Cond,
  std::invocable Fn>
requires std::same_as<std::invoke_result_t<Fn&>, void>
ss::future<> async_while_counter(async_counter& counter, Cond cond, Fn f) {
    bool stop{false};
    while (!stop) {
        int i = 0;
        for (; i < Traits::interval; ++i) {
            if (cond()) {
                f();
            } else {
                stop = true;
            }
        }
        counter.count += i;
        if (!stop && counter.count >= Traits::interval) {
            co_await ss::coroutine::maybe_yield();
            counter.count = 0;
            Traits::yield_called();
        }
    }
}

/**
 * @brief Call f until cond is false, yielding occasionally.
 *
 * This is equivalent to while (cond()) f(); except that the computational
 * loop yields every Traits::interval (default 100) iterations in order
 * to avoid reactor stalls. The returned future resolves when cond() is false.
 *
 * The functions are taken by value.
 *
 * @param cond the synchronous nullary predicate to call to check the condition
 * @param f the synchronous nullary function to call on each element
 * @return ss::future<> a future which resolves when !cond()
 */
template<
  typename Traits = async_algo_traits,
  std::predicate Cond,
  std::invocable Fn>
requires std::same_as<std::invoke_result_t<Fn&>, void>
ss::future<> async_while(Cond cond, Fn f) {
    async_counter counter{0};
    co_await async_while_counter<Traits>(
      counter, std::move(cond), std::move(f));
}

} // namespace ssx
