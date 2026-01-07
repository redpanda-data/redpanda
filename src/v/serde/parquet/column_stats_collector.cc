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

#include "absl/numeric/int128.h"
#include "bytes/iobuf_parser.h"
#include "serde/parquet/value.h"

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

std::optional<iobuf> binary_bound_truncator::get_min_bound(iobuf& b) const {
    if (b.size_bytes() <= _max_bound_size) {
        return {};
    }
    return truncate_to_max_bound_size(b, is_valid_utf8(b));
}

std::optional<iobuf> binary_bound_truncator::get_max_bound(iobuf& b) const {
    if (b.size_bytes() <= _max_bound_size) {
        return {};
    }

    bool is_utf8 = is_valid_utf8(b);
    return truncate_to_max_bound_size(b, is_utf8).and_then([&](auto trun) {
        return try_increment(trun, is_utf8);
    });
}

bool binary_bound_truncator::is_valid_utf8(const iobuf& b) const {
    auto cbegin = iobuf::byte_iterator(b.cbegin(), b.cend());
    auto cend = iobuf::byte_iterator(b.cend(), b.cend());
    // Only validate up to the max bound size. This makes the runtime constant
    // and an incremented utf8 sequence will still be greater than or equal to
    // an incremented plain byte sequence.
    return utf::is_valid_utf8(cbegin, cend, _max_bound_size);
}

std::optional<iobuf>
binary_bound_truncator::try_increment(iobuf& b, bool is_utf8) const {
    if (is_utf8) {
        return try_increment_utf8(b);
    } else {
        return try_increment_bytes(b);
    }
}

std::optional<iobuf>
binary_bound_truncator::try_increment_utf8(iobuf& b) const {
    auto crbegin = iobuf::reverse_byte_iterator(b.crbegin(), b.crend());
    auto crend = iobuf::reverse_byte_iterator(b.crend(), b.crend());
    auto utf_iter = utf::utf32_reverse_iterator(crbegin, crend);
    auto utf_end = utf::utf32_reverse_iterator(crend, crend);

    size_t bytes_read = 0;
    size_t code_points_read = 0;
    std::optional<utf::utf32_code_point> inc_char;
    for (; utf_iter != utf_end; ++utf_iter) {
        ++code_points_read;
        bytes_read += utf_iter->utf8_encoding_length();
        // TODO: `try_increment` only increments if the resulting utf8 encoding
        // is the same length as the original. This can likely be relaxed if its
        // acceptable that the truncated value exceeds `_max_bound_size` by a
        // few bytes.
        if (auto inc_c = utf_iter->try_increment()) {
            inc_char = inc_c;
            break;
        }
    }

    if (!inc_char) {
        return {};
    }

    auto trun = b.share(0, b.size_bytes() - bytes_read);
    auto ph = trun.reserve(
      inc_char->utf8_encoding_length() + (code_points_read - 1));
    ph.write(
      inc_char->utf8_encoding().data(), inc_char->utf8_encoding_length());
    const uint8_t z = 0;
    for (size_t i = 0; i < (code_points_read - 1); ++i) {
        ph.write(&z, 1);
    }
    return trun;
}

std::optional<iobuf>
binary_bound_truncator::try_increment_bytes(iobuf& b) const {
    size_t bytes_read = 0;
    std::optional<uint8_t> char_found;
    auto crbegin = iobuf::reverse_byte_iterator(b.crbegin(), b.crend());
    auto crend = iobuf::reverse_byte_iterator(b.crend(), b.crend());
    for (; crbegin != crend; crbegin++) {
        bytes_read++;
        uint8_t c = *crbegin;
        if (c != std::numeric_limits<uint8_t>::max()) {
            char_found = c + 1;
            break;
        }
    }

    if (!char_found) {
        return {};
    }

    auto trun = b.share(0, b.size_bytes() - bytes_read);
    auto ph = trun.reserve(bytes_read);
    ph.write(&char_found.value(), 1);
    // Note that it'd be more efficient to just re-use the tail-end of `b`.
    // However, zero-ing out the tail end results in a closer bound.
    const uint8_t z = 0;
    for (size_t i = 0; i < (bytes_read - 1); i++) {
        ph.write(&z, 1);
    }
    return trun;
}

std::optional<iobuf> binary_bound_truncator::truncate_to_max_bound_size(
  iobuf& b, bool valid_utf8) const {
    auto trun = b.share(0, _max_bound_size);

    if (valid_utf8) {
        auto crbegin = iobuf::reverse_byte_iterator(
          trun.crbegin(), trun.crend());
        auto crend = iobuf::reverse_byte_iterator(trun.crend(), trun.crend());
        auto n = utf::find_incomplete_code_point(crbegin, crend);
        if (trun.size_bytes() == n) {
            return {};
        }

        trun = trun.share(0, trun.size_bytes() - n);
    }

    return trun;
}

} // namespace internal

} // namespace serde::parquet
