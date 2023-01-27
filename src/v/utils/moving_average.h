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
#include "named_type.h"
#include "vassert.h"

#include <array>
#include <chrono>
#include <iterator>
#include <numeric>
#include <ratio>

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

template<typename T, typename Clock>
class timed_moving_average {
    /// Timestamp normalized by resolution
    using normalized_timestamp_t
      = named_type<size_t, struct normalized_time_tag>;
    struct bucket {
        T value{};
        size_t num_samples{0};
        normalized_timestamp_t ix;
    };

public:
    using clock_t = Clock;
    using duration_t = typename Clock::duration;
    using timestamp_t = typename Clock::time_point;
    timed_moving_average(
      T initial_value,
      std::chrono::nanoseconds depth,
      std::chrono::nanoseconds resolution = std::chrono::milliseconds(100))
      : _num_buckets(depth.count() / resolution.count())
      , _resolution(resolution)
      , _buckets(_num_buckets, bucket{})
      , _end(_num_buckets) {
        vassert(_num_buckets > 0, "Resolution is too small");
        _buckets[index(_end)] = {
          .value = initial_value, .num_samples = 1, .ix = _end};
    }

    // Update moving average
    // \note time should never go back
    void update(T v, timestamp_t ts) noexcept {
        // NOTE: we have to add num_buckets to the value to avoid
        // overflow.
        auto end = normalize(ts) + normalized_timestamp_t(_num_buckets);
        vassert(
          end >= _end,
          "Timestamp moved backward in time, current {}, previous {}",
          _end,
          end);
        _end = end;
        auto& bucket = _buckets[index(end)];
        if (bucket.ix == _end) {
            bucket.value += v;
            bucket.num_samples += 1;
        } else {
            bucket.ix = _end;
            bucket.value = v;
            bucket.num_samples = 1;
        }
    }

    T get() const noexcept {
        T running_sum{};
        size_t num_samples = 0;
        for (size_t i = 0; i < _num_buckets; i++) {
            auto ix = _end - normalized_timestamp_t(i);
            auto& b = _buckets[index(ix)];
            if (b.ix != ix) {
                break;
            }
            running_sum += b.value;
            num_samples += b.num_samples;
        }
        vassert(num_samples != 0, "timed_moving_average invariant broken");
        return running_sum / num_samples;
    }

private:
    size_t index(normalized_timestamp_t ts) const noexcept {
        return ts() % _num_buckets;
    }
    normalized_timestamp_t normalize(timestamp_t ts) {
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
          ts.time_since_epoch());
        auto norm = nanos.count() / _resolution.count();
        return normalized_timestamp_t(norm);
    }
    const size_t _num_buckets;
    std::chrono::nanoseconds _resolution;
    std::vector<bucket> _buckets;
    normalized_timestamp_t _end;
};
