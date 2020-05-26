#pragma once

#include "random/fast_prng.h"

template<
  typename ClockType,
  typename DurationType = typename ClockType::duration>
class simple_time_jitter {
public:
    using time_point = typename ClockType::time_point;

    explicit simple_time_jitter(DurationType base, DurationType jitter) noexcept
      : _base(base)
      , _jitter(jitter) {}
    explicit simple_time_jitter(DurationType base_timeout)
      : simple_time_jitter(base_timeout, base_timeout / 2) {}

    time_point operator()() { return ClockType::now() + next_duration(); }

    DurationType jitter_duration() const { return _jitter; }
    DurationType base_duration() const { return _base; }
    DurationType next_jitter_duration() {
        return DurationType(_rand() % _jitter.count());
    }
    DurationType next_duration() { return _base + next_jitter_duration(); }

private:
    DurationType _base;
    DurationType _jitter;
    fast_prng _rand{};
};
