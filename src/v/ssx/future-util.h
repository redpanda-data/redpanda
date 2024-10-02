/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/vassert.h"
#include "utils/functional.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>

#include <algorithm>
#include <iterator>
#include <memory>
#include <type_traits>

namespace ssx {

namespace detail {

template<typename ResultType, typename Iterator, typename Func>
inline seastar::future<std::vector<ResultType>>
async_transform(Iterator begin, Iterator end, Func&& func) {
    std::vector<ResultType> res;
    res.reserve(std::distance(begin, end));
    return seastar::do_with(
      std::move(res),
      std::move(begin),
      std::move(end),
      [func = std::forward<Func>(func)](
        std::vector<ResultType>& res, Iterator& begin, Iterator& end) mutable {
          /// Since its not known what type of iterator 'Iterator' is, a
          /// universal ref must be used. For example an rval-ref to a temporary
          /// (when Iterator is a boost::range::iterator_range) or an l-val ref
          /// (when Iterator is std::vector<>::iterator).
          return seastar::do_for_each(
                   begin,
                   end,
                   [&res,
                    func = std::forward<Func>(func)](auto&& value) mutable {
                       return func(res, std::forward<decltype(value)>(value));
                   })
            .then([&res] { return std::move(res); });
      });
}

} // namespace detail

/// \brief Run tasks synchronously in order and wait for completion only
/// invoking futures one after the previous has completed
///
/// Given a range [\c begin, \c end) of objects, run \c func on each \c *i in
/// the range, and return a \c future<> containing a \c std::vector<> of values.
/// In case any of the given tasks fails, one of the exceptions is returned by
/// this function as a failed future.
///
/// \param begin an \c InputIterator designating the beginning of the range
/// \param end an \c InputIterator designating the end of the range
/// \param func Function to invoke with each element in the range (will be
/// futurized if it doesn't return a \c future<>)
/// \return a \c future<> containing a \c std::vector<> of the results of the
/// function invocations that resolves when all the function invocations
/// complete.  If one or more return an exception, the return value contains one
/// of the exceptions.
template<typename Iterator, typename Func>
requires requires(Func f, Iterator i) {
    *(++i);
    { i != i } -> std::convertible_to<bool>;
    seastar::futurize_invoke(f, *i).get();
}
inline auto async_transform(Iterator begin, Iterator end, Func&& func) {
    using result_type = std::remove_reference_t<
      decltype(seastar::futurize_invoke(func, *begin).get())>;
    return detail::async_transform<result_type>(
      std::move(begin),
      std::move(end),
      [func = std::forward<Func>(func)](
        std::vector<result_type>& acc, auto&& x) {
          return seastar::futurize_invoke(func, std::forward<decltype(x)>(x))
            .then(
              [&acc](result_type result) { acc.push_back(std::move(result)); });
      });
}

/// \brief Run tasks synchronously in order and wait for completion only
/// invoking futures one after a previous has completed (range version)
///
/// Given a range \c rng of objects, run \c func on each element in the range,
/// and return a \c future<> containing a \c std::vector<> of values. In case
/// any of the given tasks fails, one of the exceptions is returned by this
/// function as a failed future.
///
/// If \c rng is an rvalue reference, it will be kept alive.
///
/// \param rng an \c InputRange
/// \param func Function to invoke with each element in the range (will be
/// futurized if it doesn't return a \c future<>)
/// \return a \c future<> containing a \c std::vector<> of the results of the
/// function invocations that resolves when all the function invocations
/// complete.  If one or more return an exception, the return value contains one
/// of the exceptions.
template<typename Rng, typename Func>
requires requires(Func f, Rng r) {
    r.begin();
    r.end();
    { r.begin() != r.begin() } -> std::convertible_to<bool>;
    seastar::futurize_invoke(f, *r.begin()).get();
}
inline auto async_transform(Rng& rng, Func&& func) {
    return async_transform(rng.begin(), rng.end(), std::forward<Func>(func));
}

/// \brief Run tasks synchronously in order and wait for completion only
/// invoking futures one after a previous has completed (range version).
/// Difference is that the results are expected to be a series of collections
/// for which are then transformed into one single collection.
///
/// Given a range [\c begin, \c end) of objects, run \c func on each \c *i in
/// the range, and return a \c future<> containing a \c std::vector<> of
/// std::vector<> of values. These values are then unflattened into a single
/// vector as the functions return value. In case any of the given tasks fails,
/// one of the exceptions is returned by this function as a failed future.
///
/// \param begin an \c InputIterator designating the beginning of the range
/// \param end an \c InputIterator designating the end of the range
/// \param func Function to invoke with each element in the range (will be
/// futurized if it doesn't return a \c future<> and expected to return a
/// std::vector<> of values)
/// \return a \c future<> containing a \c std::vector<>
/// of the results of the function invocations that resolves when all the
/// function invocations complete.  If one or more return an exception, the
/// return value contains one of the exceptions.
template<typename Iterator, typename Func>
requires requires(Func f, Iterator i) {
    *(++i);
    { i != i } -> std::convertible_to<bool>;
    seastar::futurize_invoke(f, *i).get().begin();
    seastar::futurize_invoke(f, *i).get().end();
}
inline auto async_flat_transform(Iterator begin, Iterator end, Func&& func) {
    using result_type = std::remove_reference_t<
      decltype(seastar::futurize_invoke(func, *begin).get())>;
    using value_type = typename result_type::value_type;
    return detail::async_transform<value_type>(
      std::move(begin),
      std::move(end),
      [func = std::forward<Func>(func)](
        std::vector<value_type>& acc, auto&& x) {
          return seastar::futurize_invoke(func, std::forward<decltype(x)>(x))
            .then([&acc](std::vector<value_type> x) {
                std::copy(
                  std::make_move_iterator(x.begin()),
                  std::make_move_iterator(x.end()),
                  std::back_inserter(acc));
            });
      });
}

template<typename Rng, typename Func>
requires requires(Func f, Rng r) {
    r.begin();
    r.end();
    { r.begin() != r.begin() } -> std::convertible_to<bool>;
    seastar::futurize_invoke(f, *r.begin()).get();
}
inline auto async_flat_transform(Rng& rng, Func&& func) {
    return async_flat_transform(
      rng.begin(), rng.end(), std::forward<Func>(func));
}

/// \brief Run tasks in parallel and wait for completion, capturing possible
/// errors (iterator version).
///
/// Given a range [\c begin, \c end) of objects, run \c func on each \c *i in
/// the range, and return a \c future<> containing a \c std::vector<> of
/// values. In case any of the given tasks fails, one of the exceptions is
/// returned by this function as a failed future.
///
/// \param begin an \c InputIterator designating the beginning of the range
/// \param end an \c InputIterator designating the end of the range
/// \param func Function to invoke with each element in the range (will be
/// futurized if it doesn't return a \c future<>)
/// \return a \c future<> containing a \c std::vector<> of the results of the
/// function invocations that resolves when all the function invocations
/// complete.  If one or more return an exception, the return value contains one
/// of the exceptions.
template<typename Iterator, typename Func>
requires requires(Func f, Iterator i) {
    *(++i);
    { i != i } -> std::convertible_to<bool>;
}
inline auto parallel_transform(Iterator begin, Iterator end, Func func) {
    using value_type = typename std::iterator_traits<Iterator>::value_type;
    using future = decltype(seastar::futurize_invoke(
      std::move(func), std::move(*begin)));
    std::vector<future> res;
    res.reserve(std::distance(begin, end));
    std::transform(
      begin,
      end,
      std::back_inserter(res),
      [func{std::move(func)}](value_type val) mutable {
          return seastar::futurize_invoke(std::move(func), std::move(val));
      });
    return seastar::do_with(std::move(res), [](std::vector<future>& res) {
        return seastar::when_all_succeed(
          std::make_move_iterator(res.begin()),
          std::make_move_iterator(res.end()));
    });
}

/// \brief Run tasks in parallel and wait for completion, capturing possible
/// errors (range version).
///
/// Given a range \c rng of objects, run \c func on each element in the range,
/// and return a \c future<> containing a \c std::vector<> of values. In case
/// any of the given tasks fails, one of the exceptions is returned by this
/// function as a failed future.
///
/// If \c rng is an rvalue reference, it will be kept alive.
///
/// \param rng an \c InputRange
/// \param func Function to invoke with each element in the range (will be
/// futurized if it doesn't return a \c future<>)
/// \return a \c future<> containing a \c std::vector<> of the results of the
/// function invocations that resolves when all the function invocations
/// complete.  If one or more return an exception, the return value contains one
/// of the exceptions.
template<typename Rng, typename Func>
requires requires(Func f, Rng r) {
    r.begin();
    r.end();
    { r.begin() != r.begin() } -> std::convertible_to<bool>;
}
inline auto parallel_transform(Rng rng, Func func) {
    return seastar::do_with(
      std::move(rng), [func{std::move(func)}](Rng& rng) mutable {
          return parallel_transform(
            std::make_move_iterator(rng.begin()),
            std::make_move_iterator(rng.end()),
            std::move(func));
      });
}

/// \brief Helper for explicitly ignoring a future's result, for use when
/// backgrounding a task.
///
/// For example:
///   ssx::background = ssx::spawn_with_gate_then...
///
/// This replaces use of (void) when spawning background fibers, and makes it
/// easier to search for places we start background work.
namespace detail {
struct background_t {
    template<typename T>
    constexpr void operator=(T&&) const noexcept {}
};
} // namespace detail
inline constexpr detail::background_t background;

/// \brief Create a new future, handling common shutdown exception types.
inline seastar::future<>
ignore_shutdown_exceptions(seastar::future<> fut) noexcept {
    try {
        co_await std::move(fut);
    } catch (const seastar::abort_requested_exception&) {
    } catch (const seastar::gate_closed_exception&) {
    } catch (const seastar::broken_semaphore&) {
    } catch (const seastar::broken_promise&) {
    } catch (const seastar::broken_condition_variable&) {
    }
}

/// \brief Check if the exception is a commonly ignored shutdown exception.
inline bool is_shutdown_exception(const std::exception_ptr& e) {
    try {
        std::rethrow_exception(e);
    } catch (const seastar::abort_requested_exception&) {
        return true;
    } catch (const seastar::gate_closed_exception&) {
        return true;
    } catch (const seastar::broken_semaphore&) {
        return true;
    } catch (const seastar::broken_promise&) {
        return true;
    } catch (const seastar::broken_condition_variable&) {
        return true;
    } catch (...) {
    }
    return false;
}

/// \brief Create a future holding a gate, handling common shutdown exception
/// types.  Returns the resulting future, onto which further exception handling
/// may be chained.
///
/// \param g Gate to enter, passed through to ss::try_with_gate
/// \param func Function to invoke, passed through to ss::try_with_gate
///
/// This is an alternative to spawn_with_gate for when the caller wants to
/// do extra exception handling, such as ignoring+logging all exceptions in
/// places where success is optional.  The caller will never see
/// gate_closed_exception or abort_requested_exception, avoiding log
/// noise on shutdown if the caller is logging exceptions.
template<typename Func>
inline auto spawn_with_gate_then(seastar::gate& g, Func&& func) noexcept {
    return ignore_shutdown_exceptions(
      seastar::try_with_gate(g, std::forward<Func>(func)));
}

/// \brief Detach a fiber holding a gate, with exception handling to ignore
/// any shutdown exceptions.
///
/// \param g Gate to enter, passed through to ss::try_with_gate
/// \param func Function to invoke, passed through to ss::try_with_gate
///
/// This is a common pattern in redpanda: we might spawn off a fiber to handle
/// a request within a gate, but process shutdown might happen at any time,
/// resulting in gate_closed and/or abort_requested_exception.
template<typename Func>
inline void spawn_with_gate(seastar::gate& g, Func&& func) noexcept {
    background = spawn_with_gate_then(g, std::forward<Func>(func));
}

/// \brief Repeats the passed func in a detached fiber until the gate is closed.
///
/// \param gate Gate object to use.
/// \param func function to invoke.
template<class Func>
inline void
repeat_until_gate_closed(seastar::gate& gate, Func&& func) noexcept {
    spawn_with_gate(gate, [&gate, func = std::forward<Func>(func)]() mutable {
        return ss::do_until(
          [&gate] { return gate.is_closed(); },
          [func = std::forward<Func>(func)]() mutable {
              return ss::futurize_invoke(func).handle_exception(
                [](const std::exception_ptr&) {
                    // A generic catch all to avoid exceptional futures.
                    // The input func may include it's own exception handling.
                });
          });
    });
}

/// \brief Repeats the passed func in a detached fiber until the gate is closed
/// or abort is triggered.
///
/// \param gate Gate object to use.
/// \param as Abort source to use.
/// \param func function to invoke.
template<class Func>
inline void repeat_until_gate_closed_or_aborted(
  ss::gate& gate, ss::abort_source& as, Func&& func) noexcept {
    spawn_with_gate(
      gate, [&gate, &as, func = std::forward<Func>(func)]() mutable {
          return ss::do_until(
            [&gate, &as] { return as.abort_requested() || gate.is_closed(); },
            [func = std::forward<Func>(func)]() mutable {
                return ss::futurize_invoke(func).handle_exception(
                  [](const std::exception_ptr&) {
                      // A generic catch all to avoid exceptional futures.
                      // The input func may include it's own exception handling.
                  });
            });
      });
}

/// \brief Works the same as std::partition however assumes that the predicate
/// returns item of type seastar::future<bool> instead of bool.
/// \param begin an \c InputIterator designating the beginning of the range
/// \param end an \c InputIterator designating the end of the range
/// \param p Function to invoke with each element in the range
template<typename Iter, typename UnaryAsyncPredicate>
requires requires(UnaryAsyncPredicate f, Iter i) {
    *(++i);
    { i != i } -> std::convertible_to<bool>;
    seastar::futurize_invoke(f, *i).get();
}
seastar::future<Iter> partition(Iter begin, Iter end, UnaryAsyncPredicate p) {
    auto itr = begin;
    for (; itr != end; ++itr) {
        if (!co_await p(*itr)) {
            break;
        }
    }
    if (itr == end) {
        co_return itr;
    }

    for (auto i = std::next(itr); i != end; ++i) {
        if (co_await p(*i)) {
            std::iter_swap(i, itr);
            ++itr;
        }
    }
    co_return itr;
}

/// \brief Create a future that resolves either when the original future
/// resolves, a timeout elapses (in which case timed_out_error exception is
/// returned), or the abort source fires (in which case
/// abort_requested_exception exception is returned), whichever comes first. In
/// case of either timeout or abort the result of the original future is
/// ignored.
///
/// \param f A future to wrap.
/// \param deadline A deadline to wait until.
/// \param as Abort source.
template<typename Clock, typename Duration, typename... T>
seastar::future<T...> with_timeout_abortable(
  seastar::future<T...> f,
  std::chrono::time_point<Clock, Duration> deadline,
  seastar::abort_source& as) {
    if (f.available()) {
        return f;
    }

    struct state {
        seastar::promise<T...> done;
        seastar::timer<Clock> timer;
        seastar::abort_source::subscription sub;

        state(typename Clock::time_point deadline, seastar::abort_source& as)
          : timer([this] {
              sub = seastar::abort_source::subscription{};
              done.set_exception(seastar::timed_out_error{});
          }) {
            try {
                as.check();
            } catch (...) {
                done.set_to_current_exception();
                return;
            }
            auto maybe_sub = as.subscribe(
              [this,
               &as](const std::optional<std::exception_ptr>& ex) noexcept {
                  timer.cancel();
                  done.set_exception(ex.value_or(as.get_default_exception()));
              });
            vassert(
              maybe_sub,
              "abort_source was just checked this should never happen.");
            sub = std::move(*maybe_sub);
            timer.arm(deadline);
        }
    };

    auto st = std::make_unique<state>(deadline, as);
    auto result = st->done.get_future();

    background = f.then_wrapped([st = std::move(st)](auto&& f) {
        bool fired = !st->timer.armed() || !st->sub;
        st->timer.cancel();
        st->sub = seastar::abort_source::subscription{};

        if (fired) {
            f.ignore_ready_future();
        } else {
            f.forward_to(std::move(st->done));
        }
    });

    return result;
}

// Create a ready future with template deduction.
//
// In most cases you should not need specify a template parameter using this
// function over seastar's make_ready_future function.
template<typename T>
seastar::future<std::remove_cvref_t<T>> now(T&& v) noexcept {
    return seastar::make_ready_future<std::remove_cvref_t<T>>(
      std::forward<T>(v));
}

} // namespace ssx
