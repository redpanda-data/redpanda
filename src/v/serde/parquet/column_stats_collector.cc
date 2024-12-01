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

#include "serde/parquet/column_stats_collector.h"

#include "bytes/iobuf_parser.h"
#include "serde/parquet/value.h"

#include <absl/numeric/int128.h>

#include <cassert>
#include <cmath>
#include <compare>
#include <stdexcept>

namespace serde::parquet {

namespace ordering {

std::strong_ordering boolean(const boolean_value a, const boolean_value b) {
    return a.val <=> b.val;
}

std::strong_ordering int32(const int32_value a, const int32_value b) {
    return a.val <=> b.val;
}

std::strong_ordering uint32(const int32_value a, const int32_value b) {
    return static_cast<uint32_t>(a.val) <=> static_cast<uint32_t>(b.val);
}

std::strong_ordering int64(const int64_value a, const int64_value b) {
    return a.val <=> b.val;
}

std::strong_ordering uint64(const int64_value a, const int64_value b) {
    return static_cast<uint64_t>(a.val) <=> static_cast<uint64_t>(b.val);
}

std::strong_ordering float32(const float32_value a, const float32_value b) {
    std::partial_ordering cmp = a.val <=> b.val;
    if (cmp == std::partial_ordering::less) {
        return std::strong_ordering::less;
    } else if (cmp == std::partial_ordering::greater) {
        return std::strong_ordering::greater;
    } else {
        // NOTE we can assume this because nans are filtered out
        // negative zero is handled in min/max
        assert(!std::isnan(a.val));
        assert(!std::isnan(b.val));
        return std::strong_ordering::equivalent;
    }
}

std::strong_ordering float64(const float64_value a, const float64_value b) {
    std::partial_ordering cmp = a.val <=> b.val;
    if (cmp == std::partial_ordering::less) {
        return std::strong_ordering::less;
    } else if (cmp == std::partial_ordering::greater) {
        return std::strong_ordering::greater;
    } else {
        // NOTE we can assume this because nans are filtered out
        // negative zero is handled in min/max
        assert(!std::isnan(a.val));
        assert(!std::isnan(b.val));
        return std::strong_ordering::equivalent;
    }
}

std::strong_ordering
byte_array(const byte_array_value& a, const byte_array_value& b) {
    return a.val <=> b.val;
}

std::strong_ordering fixed_byte_array(
  const fixed_byte_array_value& a, const fixed_byte_array_value& b) {
    return a.val <=> b.val;
}

std::strong_ordering
int128_be(const fixed_byte_array_value& a, const fixed_byte_array_value& b) {
    if (
      a.val.size_bytes() != sizeof(absl::int128)
      || b.val.size_bytes() != sizeof(absl::int128)) {
        throw std::runtime_error("unable to convert input to int128");
    }
    iobuf_const_parser ap(a.val);
    auto a_hi = ap.consume_be_type<int64_t>();
    auto a_lo = ap.consume_be_type<uint64_t>();
    iobuf_const_parser bp(b.val);
    auto b_hi = bp.consume_be_type<int64_t>();
    auto b_lo = bp.consume_be_type<uint64_t>();
    // TODO: Switch to <=> on absl::int128 when
    // cmake is dropped (the cmake build has an
    // old absl version)
    auto cmp = a_hi <=> b_hi;
    if (cmp == std::strong_ordering::equal) {
        cmp = a_lo <=> b_lo;
    }
    return cmp;
}

} // namespace ordering

namespace internal {

template<>
byte_array_value copy(byte_array_value& v) {
    return {v.val.share(0, v.val.size_bytes())};
}

template<>
fixed_byte_array_value copy(fixed_byte_array_value& v) {
    return {v.val.share(0, v.val.size_bytes())};
}

} // namespace internal

} // namespace serde::parquet
