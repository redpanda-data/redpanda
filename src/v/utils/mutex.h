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
 */
class mutex {
public:
    using duration = typename ss::semaphore::duration;
    using time_point = typename ss::semaphore::time_point;

    mutex()
      : _sem(1) {}

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

    auto try_get_units() noexcept { return ss::try_get_units(_sem, 1); }

    void broken() noexcept { _sem.broken(); }

    bool ready() { return _sem.waiters() == 0 && _sem.available_units() == 1; }

private:
    ss::semaphore _sem;
};
