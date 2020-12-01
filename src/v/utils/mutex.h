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
#include "seastarx.h"

#include <seastar/core/semaphore.hh>

/*
 * A traditional mutex. If you are trying to count things or need timeouts, you
 * probably want to stick with a standard semaphore. The primary motivation for
 * this class is to formalize the mutex pattern and avoid mistakes with
 * specifying resource units which are binary with a mutex.
 *
 * Usage
 * =====
 *
 *    ```
 *    mutex m;
 *    return m.with([] { ... });
 *    ```
 *
 * The `named_mutex` variant is analgous to `seastar::named_semaphore`. See
 * `seastar/core/semaphore.hh` for more information using an exception factory.
 *
 */
template<
  typename ExceptionFactory,
  typename Clock = typename ss::timer<>::clock>
class basic_mutex {
public:
    using underlying_t = ss::basic_semaphore<ExceptionFactory, Clock>;
    using duration = typename underlying_t::duration;
    using time_point = typename underlying_t::time_point;

    basic_mutex()
      : _sem(1) {}

    basic_mutex(ExceptionFactory&& factory)
      : _sem(1, std::forward<ExceptionFactory>(factory)) {}

    template<typename Func>
    auto with(Func&& func) noexcept {
        return ss::with_semaphore(_sem, 1, std::forward<Func>(func));
    }

    template<typename Func>
    auto with(duration timeout, Func&& func) noexcept {
        return ss::with_semaphore(_sem, 1, timeout, std::forward<Func>(func));
    }

    template<typename Func>
    auto with(time_point timeout, Func&& func) noexcept {
        return ss::get_units(_sem, 1, timeout)
          .then([func = std::forward<Func>(func)](auto units) mutable {
              return ss::futurize_invoke(std::forward<Func>(func))
                .finally([units = std::move(units)] {});
          });
    }

    auto get_units() noexcept { return ss::get_units(_sem, 1); }

private:
    underlying_t _sem;
};

using mutex = basic_mutex<ss::semaphore_default_exception_factory>;
using named_mutex = basic_mutex<ss::named_semaphore_exception_factory>;
