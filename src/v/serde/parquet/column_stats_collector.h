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
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"

#include <seastar/util/noncopyable_function.hh>

#include <cmath>
#include <compare>
#include <cstdint>
#include <optional>
#include <type_traits>

namespace serde::parquet {

namespace internal {

ss::noncopyable_function<
  std::strong_ordering(const boolean_value, const boolean_value)>
  make_comparator(boolean_value, logical_type);
ss::noncopyable_function<
  std::strong_ordering(const int32_value, const int32_value)>
  make_comparator(int32_value, logical_type);
ss::noncopyable_function<
  std::strong_ordering(const int64_value, const int64_value)>
  make_comparator(int64_value, logical_type);
ss::noncopyable_function<
  std::strong_ordering(const float32_value, const float32_value)>
  make_comparator(float32_value, logical_type);
ss::noncopyable_function<
  std::strong_ordering(const float64_value, const float64_value)>
  make_comparator(float64_value, logical_type);
ss::noncopyable_function<
  std::strong_ordering(const byte_array_value&, const byte_array_value&)>
  make_comparator(byte_array_value, logical_type);
ss::noncopyable_function<std::strong_ordering(
  const fixed_byte_array_value&, const fixed_byte_array_value&)>
  make_comparator(fixed_byte_array_value, logical_type);

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
template<typename value_type>
class column_stats_collector {
    using ref_type = std::conditional_t<
      std::is_trivially_copyable_v<value_type>,
      value_type,
      value_type&>;
    using const_ref_type = std::conditional_t<
      std::is_trivially_copyable_v<value_type>,
      const value_type,
      const value_type&>;

public:
    // Construct a new stats collector that uses the given logical type for
    // comparison to get min/max values.
    explicit column_stats_collector(logical_type lt)
      : _cmp(internal::make_comparator(value_type{}, lt)) {};

    // Record a value in the collector
    void record_value(ref_type v) {
        if constexpr (std::is_floating_point_v<decltype(v.val)>) {
            if (std::isnan(v.val)) {
                return;
            }
        }
        if (!_min || _cmp(v, *_min) == std::strong_ordering::less) {
            _min = internal::copy(v);
        }
        if (!_max || _cmp(v, *_max) == std::strong_ordering::greater) {
            _max = internal::copy(v);
        }
    }

    // Record a null in the collector
    void record_null() { ++_null_count; }

    // Merge another stats collector into this one.
    //
    // NOTE: The caller must ensure that the other collector and this collector
    // are for the same logical type.
    void merge(column_stats_collector<value_type> other) {
        _null_count += other._null_count;
        if (
          other._min
          && (!_min || _cmp(*other._min, *_min) == std::strong_ordering::less)) {
            _min = internal::copy(*other._min);
        }
        if (
          other._max
          && (!_max || _cmp(*other._max, *_max) == std::strong_ordering::greater)) {
            _max = internal::copy(*other._max);
        }
    }

    void reset() {
        _null_count = 0;
        _min = std::nullopt;
        _max = std::nullopt;
    }

    int64_t null_count() const { return _null_count; }
    const std::optional<value_type>& min() const { return _min; }
    const std::optional<value_type>& max() const { return _max; }

private:
    ss::noncopyable_function<std::strong_ordering(
      const_ref_type, const_ref_type)>
      _cmp;
    std::optional<value_type> _min;
    std::optional<value_type> _max;
    int64_t _null_count = 0;
};

} // namespace serde::parquet
