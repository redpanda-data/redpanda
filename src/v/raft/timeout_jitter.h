#pragma once

#include "raft/types.h"
#include "random/fast_prng.h"

#include <seastar/core/timer.hh>

#include <chrono>
#include <cstdint>

namespace raft {
class timeout_jitter {
public:
    timeout_jitter(duration_type base_timeout, duration_type jitter)
      : _jitter(jitter)
      , _election_duration(base_timeout) {
    }

    explicit timeout_jitter(duration_type base_timeout)
      : timeout_jitter(base_timeout, base_timeout / 2) {
    }

    timeout_jitter(timeout_jitter&& o) noexcept
      : _jitter(o._jitter)
      , _election_duration(std::move(o._election_duration))
      , _prng(std::move(o._prng)) {
    }

    /// adds a random amount of jitter time to base_timeout
    /// mostly used in leader-election type of timeouts
    clock_type::time_point operator()() {
        return clock_type::now() + _election_duration
               + duration_type(_prng() % _jitter.count());
    }
    duration_type base_duration() const {
        return _election_duration;
    }

private:
    duration_type _jitter;
    duration_type _election_duration;
    fast_prng _prng;
};
} // namespace raft
