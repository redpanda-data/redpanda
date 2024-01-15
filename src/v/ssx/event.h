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
#include "ssx/semaphore.h"

namespace ssx {

/*
 * A simple notification mechanism. One fiber can set() the event and another
 * can wait() for it. If there are no waiters the event is not lost. If there
 * are several waiters they are all woken up. Successful wait resets the event.
 *
 * wait() methods
 *
 * This is pretty similar to ss::condition_variable, but is simpler and
 * requires less exception handling.
 */
template<typename Clock>
class basic_event {
public:
    using duration = typename Clock::duration;
    using time_point = typename Clock::time_point;

    explicit basic_event(ss::sstring name)
      : _sem(0, std::move(name)) {}

    bool is_set() const noexcept {
        return _sem.waiters() == 0 && _sem.available_units() > 0;
    }

    void set() noexcept {
        if (is_set()) {
            return;
        } else {
            size_t waiters = _sem.waiters();
            if (waiters > 0) {
                _sem.signal(waiters);
            } else {
                _sem.signal(1);
            }
        }
    }

    /// Can throw ss::broken_semaphore exception.
    ss::future<> wait() { return _sem.wait(1); }

    /// Returns true if the wait was successful. Can throw ss::broken_semaphore
    /// exception.
    ss::future<bool> wait(time_point deadline) {
        return _sem.wait(deadline)
          .then([] { return true; })
          .handle_exception_type(
            [](const ss::semaphore_timed_out&) { return false; });
    }

    /// Returns true if the wait was successful. Can throw ss::broken_semaphore
    /// exception.
    ss::future<bool> wait(duration d) { return wait(Clock::now() + d); }

    void broken() noexcept { _sem.broken(); }

    size_t waiters() const noexcept { return _sem.waiters(); }

private:
    ssx::named_semaphore<Clock> _sem;
};

using event = basic_event<ssx::semaphore::clock>;

} // namespace ssx
