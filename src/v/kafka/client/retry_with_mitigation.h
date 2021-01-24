/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "utils/concepts-enabled.h"
#include "utils/retry.h"

namespace pandaproxy::client {

/// \brief Perform an action with retry on failure.
///
/// If the action returns an error, it is retried with a backoff.
/// There is an attempt to mitigate the error after the backoff and prior
/// to the retry.
template<
  typename Func,
  typename ErrFunc,
  typename Futurator = ss::futurize<std::invoke_result_t<Func>>>
CONCEPT(requires std::regular_invocable<Func>)
auto retry_with_mitigation(
  int32_t retries,
  std::chrono::milliseconds retry_base_backoff,
  Func func,
  ErrFunc errFunc) {
    using namespace std::chrono_literals;
    return ss::do_with(
      std::move(func),
      std::move(errFunc),
      std::exception_ptr(),
      [retries, retry_base_backoff](
        Func& func, ErrFunc& errFunc, std::exception_ptr& eptr) {
          return retry_with_backoff(
            retries,
            [&func, &errFunc, &eptr]() {
                auto fut = ss::now();
                if (eptr) {
                    auto fut = errFunc(eptr).handle_exception(
                      [](const std::exception_ptr&) {
                          // ignore failed mitigation
                      });
                }
                return fut.then(func).handle_exception(
                  [&eptr](std::exception_ptr ex) mutable {
                      eptr = ex;
                      return Futurator::make_exception_future(eptr);
                  });
            },
            retry_base_backoff);
      });
}

} // namespace pandaproxy::client
