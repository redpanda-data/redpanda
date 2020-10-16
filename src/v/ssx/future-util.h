#pragma once
#include "utils/concepts-enabled.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>

#include <algorithm>
#include <iterator>

namespace ssx {

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
CONCEPT(requires requires(Func f, Iterator i) {
    *i++;
    { i != i }
    ->bool;
    seastar::futurize_invoke(f, *i);
    sizeof(seastar::futurize_invoke(f, *i)::value_type) == 1;
})
inline auto async_transform(Iterator begin, Iterator end, Func&& func) {
    using value_type = typename std::iterator_traits<Iterator>::value_type;
    using result_type = decltype(
      seastar::futurize_invoke(std::forward<Func>(func), *begin).get0());
    std::vector<result_type> res;
    res.reserve(std::distance(begin, end));
    return seastar::do_with(
      std::move(res),
      std::move(begin),
      std::move(end),
      [func{std::forward<Func>(func)}](
        std::vector<result_type>& res, Iterator& begin, Iterator& end) mutable {
          return seastar::do_for_each(
                   begin,
                   end,
                   [&res, func{std::forward<Func>(func)}](value_type val) {
                       return seastar::futurize_invoke(
                                func, std::forward<value_type>(val))
                         .then([&res](auto r) { res.push_back(std::move(r)); });
                   })
            .then([&res] { return std::move(res); });
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
CONCEPT(requires requires(Func f, Rng r) {
    r.begin();
    r.end();
    *r.begin()++;
    { r.begin() != r.begin() }
    ->bool;
    seastar::futurize_invoke(f, *r.begin());
    sizeof(seastar::futurize_invoke(f, *i)::value_type) == 1;
})
inline auto async_transform(Rng&& rng, Func&& func) {
    return seastar::do_with(
      std::forward<Rng>(rng), [func{std::forward<Func>(func)}](Rng& rng) {
          return async_transform(rng.begin(), rng.end(), func);
      });
}

/// \brief Run tasks in parallel and wait for completion, capturing possible
/// errors (iterator version).
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
CONCEPT(requires requires(Func f, Iterator i) {
    *i++;
    { i != i }
    ->bool;
    seastar::futurize_invoke(f, *i);
})
inline auto parallel_transform(Iterator begin, Iterator end, Func&& func) {
    using value_type = typename std::iterator_traits<Iterator>::value_type;
    using future = decltype(
      seastar::futurize_invoke(std::forward<Func>(func), *begin));
    std::vector<future> res;
    res.reserve(std::distance(begin, end));
    std::transform(
      begin,
      end,
      std::back_inserter(res),
      [func{std::forward<Func>(func)}](const value_type& val) mutable {
          return seastar::futurize_invoke(std::forward<Func>(func), val);
      });
    return seastar::do_with(std::move(res), [](std::vector<future>& res) {
        return seastar::when_all_succeed(res.begin(), res.end());
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
CONCEPT(requires requires(Func f, Rng r) {
    r.begin();
    r.end();
    *r.begin()++;
    { r.begin() != r.begin() }
    ->bool;
    seastar::futurize_invoke(f, *r.begin());
})
inline auto parallel_transform(Rng&& rng, Func&& func) {
    return seastar::do_with(
      std::forward<Rng>(rng), [func{std::forward<Func>(func)}](Rng& rng) {
          return parallel_transform(rng.begin(), rng.end(), func);
      });
}

} // namespace ssx
