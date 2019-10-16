#pragma once

#include "raft/types.h"
#include "random/fast_prng.h"

#include <seastar/core/timer.hh>

#include <chrono>
#include <cstdint>

namespace raft {
class timeout_jitter {
public:
    explicit timeout_jitter(uint32_t base_timeout_ms, uint32_t jitter = 50)
      : _jitter(jitter)
      , _election_duration(std::chrono::milliseconds(base_timeout_ms)) {
    }

    timeout_jitter(timeout_jitter&& o) noexcept
      : _jitter(o._jitter)
      , _election_duration(std::move(o._election_duration))
      , _prng(std::move(o._prng)) {
    }

    /// \brief adds jitter ammount of milliseconds according to the raft paper
    clock_type::time_point operator()() {
        using ms = std::chrono::milliseconds;
        return clock_type::now()
               + std::chrono::duration_cast<ms>(_election_duration)
               + ms(_prng() % _jitter);
    }
    duration_type base_duration() const {
        return _election_duration;
    }

private:
    const uint32_t _jitter;
    duration_type _election_duration;
    fast_prng _prng;
};
} // namespace raft
