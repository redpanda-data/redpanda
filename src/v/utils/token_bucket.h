/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include "base/vassert.h"
#include "ssx/semaphore.h"

#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/later.hh>

/*
 * Token bucket-based raft recovery throttling.
 *
 * Rate should be more than 1s/45ms = 22.(2) to perform throttling.
 */
template<typename clock_type = ss::lowres_clock>
class token_bucket {
    static constexpr std::chrono::milliseconds refresh_error{5};
    static constexpr std::chrono::milliseconds refresh_interval{50};

public:
    using throttle_duration = clock_type::duration;

    token_bucket(size_t rate, ss::sstring name)
      : _rate(rate)
      , _capacity(rate)
      , _sem{rate, std::move(name)}
      , _last_refresh(clock_type::now())
      , _refresh_timer([this] { handle_refresh(); }) {}

    token_bucket(size_t rate, ss::sstring name, size_t capacity)
      : _rate(rate)
      , _capacity(capacity)
      , _sem{rate, std::move(name)}
      , _last_refresh(clock_type::now())
      , _refresh_timer([this] { handle_refresh(); }) {}

    ss::future<> throttle(size_t size, ss::abort_source& as) {
        return maybe_throttle(size, as).discard_result();
    }

    /// Return nullopt if throttling is not applied or throttle duration
    ss::future<std::optional<throttle_duration>>
    maybe_throttle(size_t size, ss::abort_source& as) {
        using res_t = std::optional<throttle_duration>;
        _refresh_timer.cancel();
        refresh();
        /*
         * when try_wait succeeds it implies that there are no waiters so there
         * is no risk in returning without arming the refresh timer.
         */
        if (_sem.try_wait(size)) {
            return ss::make_ready_future<res_t>(std::nullopt);
        }

        auto elapsed = clock_type::now() - _last_refresh;
        if (elapsed >= refresh_interval) {
            _refresh_timer.arm(refresh_interval);
        } else {
            _refresh_timer.arm(refresh_interval - elapsed);
        }

        auto start_time = clock_type::now();
        return _sem.wait(as, size).then([start_time] {
            auto end_time = clock_type::now();
            auto duration = typename clock_type::duration(0);
            if (end_time > start_time) {
                // Return 0 in case if the clock is monotonic and
                // the end_time is actually before the start_time.
                duration = end_time - start_time;
            }
            return ss::make_ready_future<res_t>(duration);
        });
    }

    bool try_throttle(size_t size) {
        _refresh_timer.cancel();
        refresh();

        /*
         * when try_wait succeeds it implies that there are no waiters so there
         * is no risk in returning without arming the refresh timer.
         */
        return _sem.try_wait(size);
    }
    void shutdown() {
        _refresh_timer.cancel();
        _sem.broken();
    }

    void update_capacity(size_t new_capacity) {
        if (_capacity == new_capacity) {
            return;
        }

        if (new_capacity < _capacity && _sem.current() > new_capacity) {
            _sem.consume(_sem.current() - new_capacity);
        }

        _capacity = new_capacity;
    }

    void update_rate(size_t new_rate) {
        if (_rate == new_rate) {
            return;
        }

        if (_rate > new_rate) {
            if (_sem.current() <= _rate) {
                _sem.consume(_rate - new_rate);
            } else {
                /*
                 * if current > rate it means that we have accumulated tokens
                 * we should change current to new_rate to start accumulation
                 * from begining
                 */
                _sem.consume(_sem.current() - new_rate);
            }
        } else {
            if (_sem.current() <= _rate) {
                _sem.signal(new_rate - _rate);
            } else {
                // Same as previous comment
                _sem.consume(_sem.current() - new_rate);
            }
        }

        if (_rate == _capacity) {
            _capacity = new_rate;
        }

        _rate = new_rate;
    }

    size_t available() {
        refresh();
        return _sem.current();
    }

private:
    void refresh() {
        auto now = clock_type::now();
        auto elapsed = now - _last_refresh;
        if (elapsed < refresh_interval) {
            return;
        }
        _last_refresh = now;

        /*
         * subtract out half the lowres clock granularity as an error adjustment
         * that will be pessimistic and error on the side of lower throughput.
         */
        auto refresh = _rate * (elapsed - refresh_error)
                       / std::chrono::milliseconds(1000);

        _sem.signal(refresh);

        /*
         * throttling is based on an estimate. if rate is low and a waiter
         * underestimated we may need to allow the available tokens to exceed
         * the rate to let a waiter through.
         */
        if (_sem.current() > _capacity && !_sem.waiters()) {
            _sem.consume(_sem.current() - _capacity);
        }
    }

    void handle_refresh() {
        refresh();
        /*
         * if a waiter exists continue refreshing since it is not guaranteed
         * that throttle will be invoked (e.g. exactly one recovering group).
         */
        if (_sem.waiters()) {
            _refresh_timer.arm(refresh_interval);
        }
    }

    size_t _rate;
    size_t _capacity;
    ssx::semaphore _sem;
    typename clock_type::time_point _last_refresh;
    ss::timer<> _refresh_timer;
};
