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
#include "serde/parquet/value.h"

#include <cmath>
#include <compare>
#include <cstdint>
#include <optional>
#include <type_traits>

namespace serde::parquet {

namespace ordering {

std::strong_ordering boolean(const boolean_value, const boolean_value);
std::strong_ordering int32(const int32_value, const int32_value);
std::strong_ordering uint32(const int32_value, const int32_value);
std::strong_ordering int64(const int64_value, const int64_value);
std::strong_ordering uint64(const int64_value, const int64_value);
std::strong_ordering float32(const float32_value, const float32_value);
std::strong_ordering float64(const float64_value, const float64_value);
std::strong_ordering
byte_array(const byte_array_value&, const byte_array_value&);
std::strong_ordering
fixed_byte_array(const fixed_byte_array_value&, const fixed_byte_array_value&);
std::strong_ordering
int128_be(const fixed_byte_array_value&, const fixed_byte_array_value&);

} // namespace ordering

namespace internal {
template<typename value_type>
value_type copy(value_type& v) {
    return v;
}
template<>
byte_array_value copy(byte_array_value&);
template<>
fixed_byte_array_value copy(fixed_byte_array_value&);
} // namespace internal

// We incrementally collect stats on columns so we can serialize
// it in the metadata for query engine performance.
template<typename value_type, auto comparator>
class column_stats_collector {
public:
    using ref_type = std::conditional_t<
      std::is_trivially_copyable_v<value_type>,
      value_type,
      value_type&>;
    using bound_ref_type = std::conditional_t<
      std::is_trivially_copyable_v<value_type>,
      std::optional<value_type>,
      std::optional<value_type>&>;

    // Record a value in the collector
    void record_value(ref_type v) {
        if constexpr (std::is_floating_point_v<decltype(v.val)>) {
            if (std::isnan(v.val)) {
                ++_nan_count;
                return;
            }
        }
        if (!_min || comparator(v, *_min) == std::strong_ordering::less) {
            _min = internal::copy(v);
        }
        if (!_max || comparator(v, *_max) == std::strong_ordering::greater) {
            _max = internal::copy(v);
        }
    }

    // Record a null in the collector
    void record_null(int64_t n = 1) { _null_count += n; }

    // Merge another stats collector into this one.
    void merge(column_stats_collector<value_type, comparator>& other) {
        _null_count += other._null_count;
        _nan_count += other._nan_count;
        if (
          other._min
          && (!_min || comparator(*other._min, *_min) == std::strong_ordering::less)) {
            _min = internal::copy(*other._min);
        }
        if (
          other._max
          && (!_max || comparator(*other._max, *_max) == std::strong_ordering::greater)) {
            _max = internal::copy(*other._max);
        }
    }

    void reset() {
        _null_count = 0;
        _nan_count = 0;
        _min = std::nullopt;
        _max = std::nullopt;
    }

    int64_t null_count() const { return _null_count; }
    int64_t nan_count() const { return _nan_count; }

    // Byte-array bounds are retained untruncated.
    int64_t memory_usage() const {
        return bound_bytes(_min) + bound_bytes(_max);
    }

    bound_ref_type min() { return normalize(_min, true); }
    bound_ref_type max() { return normalize(_max, false); }

private:
    static int64_t bound_bytes(const std::optional<value_type>& bound) {
        if constexpr (std::is_trivially_copyable_v<value_type>) {
            return 0;
        } else {
            return bound.has_value()
                     ? static_cast<int64_t>(bound->val.size_bytes())
                     : 0;
        }
    }

    bound_ref_type normalize(bound_ref_type v, bool min) {
        if constexpr (std::is_floating_point_v<decltype(v->val)>) {
            // min floats are always written as -0 and max as 0
            if (v && v->val == 0.0) {
                return std::make_optional<value_type>(min ? -0.0 : 0.0);
            }
        }
        return v;
    }

    std::optional<value_type> _min;
    std::optional<value_type> _max;
    int64_t _null_count = 0;
    int64_t _nan_count = 0;
};

} // namespace serde::parquet
