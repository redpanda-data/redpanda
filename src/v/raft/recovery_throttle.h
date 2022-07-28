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
#include "config/property.h"
#include "raft/logger.h"
#include "seastarx.h"
#include "ssx/semaphore.h"

#include <seastar/core/smp.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/later.hh>

namespace raft {

/*
 * Token bucket-based raft recovery throttling.
 *
 * Improvements
 *
 *  - cross-core bandwidth sharing
 *      https://github.com/redpanda-data/redpanda/issues/1770
 *
 *  - cluster-level recovery control
 *      https://github.com/redpanda-data/redpanda/issues/1771
 */
class recovery_throttle {
    using clock_type = ss::lowres_clock;
    static constexpr std::chrono::milliseconds refresh_error{5};
    static constexpr std::chrono::milliseconds refresh_interval{50};

public:
    explicit recovery_throttle(config::binding<size_t> rate_binding)
      : _rate_binding(std::move(rate_binding))
      , _rate(get_per_core_rate())
      , _sem{get_per_core_rate(), "raft/recovery-rate"}
      , _last_refresh(clock_type::now())
      , _refresh_timer([this] { handle_refresh(); }) {
        _rate_binding.watch([this]() { update_rate(); });
    }

    ss::future<> throttle(size_t size, ss::abort_source& as) {
        _refresh_timer.cancel();
        refresh();

        /*
         * when try_wait succeeds it implies that there are no waiters so there
         * is no risk in returning without arming the refresh timer.
         */
        if (_sem.try_wait(size)) {
            return ss::now();
        }

        auto elapsed = clock_type::now() - _last_refresh;
        if (elapsed >= refresh_interval) {
            _refresh_timer.arm(refresh_interval);
        } else {
            _refresh_timer.arm(refresh_interval - elapsed);
        }

        return _sem.wait(as, size);
    }

    void shutdown() {
        _refresh_timer.cancel();
        _sem.broken();
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
        if (_sem.current() > _rate && !_sem.waiters()) {
            _sem.consume(_sem.current() - _rate);
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

    size_t get_per_core_rate() { return _rate_binding() / ss::smp::count; }

    void update_rate() {
        auto const per_core_rate = get_per_core_rate();

        if (_rate == per_core_rate) {
            return;
        }

        vlog(
          raftlog.info,
          "Updating recovery throttle with new rate of {}, old rate: {}",
          per_core_rate,
          _rate);

        if (_rate > per_core_rate) {
            _sem.consume(_rate - per_core_rate);
        } else {
            _sem.signal(per_core_rate - _rate);
        }
        _rate = per_core_rate;
    }

    config::binding<size_t> _rate_binding;
    size_t _rate;
    ssx::semaphore _sem;
    clock_type::time_point _last_refresh;
    ss::timer<> _refresh_timer;
};

} // namespace raft
