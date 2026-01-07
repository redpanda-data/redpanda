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

class noop_bound_truncator {
public:
    explicit noop_bound_truncator(size_t) {}
    std::optional<iobuf> get_min_bound(iobuf&) { return {}; }
    std::optional<iobuf> get_max_bound(iobuf&) { return {}; }
};

class binary_bound_truncator {
public:
    explicit binary_bound_truncator(size_t max_bound_size_bytes)
      : _max_bound_size(max_bound_size_bytes) {}

    std::optional<iobuf> get_min_bound(iobuf&) const;
    std::optional<iobuf> get_max_bound(iobuf&) const;

private:
    size_t _max_bound_size;

    bool is_valid_utf8(const iobuf&) const;
    std::optional<iobuf> try_increment(iobuf&, bool) const;
    std::optional<iobuf> try_increment_utf8(iobuf&) const;
    std::optional<iobuf> try_increment_bytes(iobuf&) const;
    std::optional<iobuf> truncate_to_max_bound_size(iobuf&, bool) const;
};

} // namespace internal

// We incrementally collect stats on columns so we can serialize
// it in the metadata for query engine performance.
template<
  typename value_type,
  auto comparator,
  typename truncator = internal::noop_bound_truncator>
class column_stats_collector {
    struct bound_t {
        std::optional<value_type> val;
        bool is_exact = false;
    };

public:
    using ref_type = std::conditional_t<
      std::is_trivially_copyable_v<value_type>,
      value_type,
      value_type&>;
    using bound_ref_type = std::conditional_t<
      std::is_trivially_copyable_v<value_type>,
      std::optional<value_type>,
      std::optional<value_type>&>;

    column_stats_collector() = default;
    explicit column_stats_collector(std::optional<size_t> max_bound_size)
      : _max_bound_size(max_bound_size) {}

    // Record a value in the collector
    void record_value(ref_type v) {
        if constexpr (std::is_floating_point_v<decltype(v.val)>) {
            if (std::isnan(v.val)) {
                return;
            }
        }

        if (
          !_min.val || comparator(v, *_min.val) == std::strong_ordering::less) {
            set_bound<true>(_min, v);
        }
        if (
          !_max.val
          || comparator(v, *_max.val) == std::strong_ordering::greater) {
            set_bound<false>(_max, v);
        }
    }

    // Record a null in the collector
    void record_null(int64_t n = 1) { _null_count += n; }

    // Merge another stats collector into this one.
    void merge(column_stats_collector<value_type, comparator>& other) {
        _null_count += other._null_count;
        if (
          other._min.val
          && (!_min.val || comparator(*other._min.val, *_min.val) == std::strong_ordering::less)) {
            _min = {internal::copy(*other._min.val), other._min.is_exact};
        }
        if (
          other._max.val
          && (!_max.val || comparator(*other._max.val, *_max.val) == std::strong_ordering::greater)) {
            _max = {internal::copy(*other._max.val), other._max.is_exact};
        }
    }
    void reset() {
        _null_count = 0;
        _min = {std::nullopt, false};
        _max = {std::nullopt, false};
    }

    int64_t null_count() const { return _null_count; }

    bound_ref_type min() { return normalize(_min.val, true); }
    bool min_is_exact() const { return _min.is_exact; }

    bound_ref_type max() { return normalize(_max.val, false); }
    bool max_is_exact() const { return _max.is_exact; }

private:
    bound_ref_type normalize(bound_ref_type v, bool min) {
        if constexpr (std::is_floating_point_v<decltype(v->val)>) {
            // min floats are always written as -0 and max as 0
            if (v && v->val == 0.0) {
                return std::make_optional<value_type>(min ? -0.0 : 0.0);
            }
        }
        return v;
    }

    template<bool is_min_bound>
    void set_bound(bound_t& b, ref_type v) {
        if constexpr (
          std::is_same_v<value_type, byte_array_value>
          || std::is_same_v<value_type, fixed_byte_array_value>) {
            if (_max_bound_size) {
                iobuf& val = v.val;
                std::optional<iobuf> t_b;
                // Immediately truncating the new bound limits the length of any
                // comparison in `record_value` to `_max_bound_size`.
                if constexpr (is_min_bound) {
                    t_b = truncator{*_max_bound_size}.get_min_bound(val);
                } else {
                    t_b = truncator{*_max_bound_size}.get_max_bound(val);
                }

                if (t_b) {
                    b = {value_type{std::move(*t_b)}, false};
                    return;
                }
            }
        }

        b = {internal::copy(v), true};
    }

    std::optional<size_t> _max_bound_size;
    bound_t _min;
    bound_t _max;
    int64_t _null_count = 0;
};

} // namespace serde::parquet
