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

#include "base/outcome.h"
#include "base/seastarx.h"

#include <seastar/core/future-util.hh>

/// Helper to wrap type with a result if it is not one already. It works for
/// both types being a result already and for futures returning result
namespace internal {
template<typename T>
struct result_wrap {
    using type = result<T>;
};

template<typename Inner>
struct result_wrap<ss::future<Inner>> {
    using type = ss::future<typename result_wrap<Inner>::type>;
};

template<typename Inner>
struct result_wrap<result<Inner>> {
    using type = result<Inner>;
};
} // namespace internal

template<typename T>
using result_wrap_t = typename internal::result_wrap<T>::type;

/// \brief Wait for either a future, or a timeout, whichever comes first
///        same as ss::with_timeout(timeout, future); except that we
///        do not set the exception, only the result with default error code
///
///        We explicitly take an \param error to allow std::make_error_code
///        ADL overload.
///
///        When timeout is reached the returned future resolves with \param
///        error
///
///        Note that timing out doesn't cancel any tasks associated with the
///        original future. It also doesn't cancel the callback registerred on
///        it.
///
/// \param timeout time point after which the returned future should be failed
/// \param error error code when timeout occurs
/// \param f future to wait for
///
/// \return a future which will be either resolved with f or a error
template<
  typename EC = std::error_code,
  typename Clock,
  typename Duration,
  typename T>
ss::future<result<T, EC>> result_with_timeout(
  std::chrono::time_point<Clock, Duration> timeout, EC error, ss::future<T> f) {
    constexpr bool is_already_result = outcome::is_basic_result_v<T>;
    static_assert(!is_already_result, "nested result<T> not yet supported");
    using ret_t = result<T, EC>;
    if (f.available()) {
        return ss::make_ready_future<ret_t>(f.get());
    }
    auto pr = std::make_unique<ss::promise<ret_t>>();
    auto result = pr->get_future();
    ss::timer<Clock> timer([&pr = *pr, error] { pr.set_value(error); });
    timer.arm(timeout);
    (void)f.then_wrapped(
      [pr = std::move(pr), timer = std::move(timer), error](auto&& f) mutable {
          if (timer.cancel()) {
              try {
                  pr->set_value(f.get());
              } catch (...) {
                  pr->set_exception(std::current_exception());
              }
          } else {
              f.ignore_ready_future();
          }
      });
    return result;
}

template<typename Ex, typename EC = std::error_code, typename T>
ss::future<result<T, EC>>
wrap_exception_with_result(EC error, ss::future<T> f) {
    constexpr bool is_already_result = outcome::is_basic_result_v<T>;
    static_assert(!is_already_result, "nested result<T> not yet supported");

    using ret_t = result<T, EC>;
    return f
      .then([](T t) { return ss::make_ready_future<ret_t>(std::move(t)); })
      .handle_exception_type([error]([[maybe_unused]] Ex& e) {
          return ss::make_ready_future<ret_t>(error);
      });
}
