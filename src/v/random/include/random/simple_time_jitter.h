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
