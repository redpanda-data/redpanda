/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "likely.h"

#include <array>
#include <iterator>
#include <numeric>

/*
 * simple moving average with configurable sample count
 */

template<typename T, size_t Samples>
class moving_average {
public:
    explicit moving_average(T initial_value)
      : _value(initial_value) {}

    void update(T v) {
        _running_sum -= _samples[_idx];
        _samples[_idx] = v;
        _running_sum += v;
        _idx = (_idx + 1) % Samples;
        if (unlikely(_valid_samples < Samples)) {
            _valid_samples++;
        }
        _value = _running_sum / _valid_samples;
    }

    T get() const { return _value; }

private:
    std::array<T, Samples> _samples{0};
    T _running_sum{0};
    T _value{0};
    size_t _idx{0};
    size_t _valid_samples{0};
};
