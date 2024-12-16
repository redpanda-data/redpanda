/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "base/seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

#include <chrono>

namespace experimental::cloud_topics::core {

enum class circuit_breaker_state {
    open,
    half_open,
    closed,
};

// Simple circuit breaker implementation.
//
// The initial status is 'open'. When errors are registered
// and number of errors is below the 'threshold' the CB is still
// 'closed'. When the errors are above the 'threshold' the CB is in
// 'open' state for the 'cooldown' period. After the cooldown period
// the CB goes into the half-open state for another cooldown period.
// If no errors are registered while the Cb is in the half open state
// the CB goes back into the 'open' state.
//
// In 'closed' state the service should block all requests. In 'half_open'
// state it should block only some % of requests (the exact amount is
// decided by the service). In 'open' state no requests are blocked.
//
// NOTE: https://martinfowler.com/bliki/CircuitBreaker.html assigns
// different meanings to 'closed' and 'open' states. In 'closed'
// state the requests are passing and in 'open' state they're not
// passing. That makes perfect sense if you're an electrician and
// less so if you're a software developer.
template<class Clock>
class circuit_breaker {
public:
    circuit_breaker(size_t error_capacity, std::chrono::milliseconds cooldown)
      : _error_capacity(error_capacity)
      , _cooldown(cooldown) {}

    circuit_breaker_state state() {
        // State method is not just returning current status
        // but also triggers time-based state transitions
        switch (_status) {
        case circuit_breaker_state::open:
            // This state is indefinite
            break;
        case circuit_breaker_state::half_open:
            // This state has deadline
            if (Clock::now() >= _deadline) {
                // No errors are registered. Otherwise the CB
                // would be returned into the closed state
                _status = circuit_breaker_state::open;
            }
            break;
        case circuit_breaker_state::closed:
            // This state has deadline
            {
                auto now = Clock::now();
                auto next = _deadline + _cooldown;
                if (now >= _deadline && now < next) {
                    _deadline = next;
                    _status = circuit_breaker_state::half_open;
                } else if (now >= _deadline && now >= next) {
                    _deadline = {};
                    _status = circuit_breaker_state::open;
                }
            }
            break;
        }
        return _status;
    }

    void register_error() {
        auto now = Clock::now();
        if (_errors_registered > 0 && now >= _last_error_ts + _cooldown) {
            // Expire old errors if enough time have passed since the
            // previous error.
            _errors_registered = 0;
        }
        _last_error_ts = now;
        _errors_registered++;
        switch (_status) {
        case circuit_breaker_state::open:
            // We can stay in this state indefinitely
            if (_errors_registered > _error_capacity) {
                _status = circuit_breaker_state::closed;
                _deadline = now + _cooldown;
                _errors_registered = 0;
            }
            break;
        case circuit_breaker_state::half_open:
            // Any error while in half open state triggers
            // transition into the closed state.
            _status = circuit_breaker_state::closed;
            _deadline = now + _cooldown;
            _errors_registered = 0;
            break;
        case circuit_breaker_state::closed:
            // We're not supposed to do anything while in
            // this state.
            break;
        }
    }

private:
    size_t _errors_registered{0};
    size_t _error_capacity;
    std::chrono::milliseconds _cooldown;
    circuit_breaker_state _status{circuit_breaker_state::open};
    Clock::time_point _deadline;
    Clock::time_point _last_error_ts;
};

} // namespace experimental::cloud_topics::core
