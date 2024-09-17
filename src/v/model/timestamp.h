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

#include "base/seastarx.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>

#include <chrono>
#include <compare>
#include <cstdint>
#include <iosfwd>
#include <limits>

class iobuf;
class iobuf_parser;

namespace model {

enum class timestamp_type : uint8_t { create_time, append_time };

std::ostream& operator<<(std::ostream&, timestamp_type);
std::istream& operator>>(std::istream&, timestamp_type&);

class timestamp {
public:
    using type = int64_t;

    timestamp() noexcept = default;

    constexpr explicit timestamp(type v) noexcept
      : _v(v) {}

    constexpr type value() const noexcept { return _v; }
    constexpr type operator()() const noexcept { return _v; }

    constexpr static timestamp min() noexcept { return timestamp(0); }

    constexpr static timestamp max() noexcept {
        return timestamp(std::numeric_limits<type>::max());
    }

    constexpr static timestamp missing() noexcept { return timestamp(-1); }

    bool operator<(const timestamp& other) const { return _v < other._v; }

    bool operator<=(const timestamp& other) const { return _v <= other._v; }

    bool operator>(const timestamp& other) const { return other._v < _v; }

    bool operator>=(const timestamp& other) const { return other._v <= _v; }

    bool operator==(const timestamp& other) const { return _v == other._v; }

    bool operator!=(const timestamp& other) const { return !(*this == other); }

    auto operator<=>(const timestamp&) const = default;

    timestamp& operator-=(const timestamp& rhs) {
        _v -= rhs();
        return *this;
    }

    friend timestamp operator-(timestamp lhs, const timestamp& rhs) {
        lhs -= rhs;
        return lhs;
    }

    friend std::ostream& operator<<(std::ostream&, timestamp);

    // ADL helpers for interfacing with the serde library.
    friend void write(iobuf& out, timestamp ts);
    friend void
    read_nested(iobuf_parser& in, timestamp& ts, const size_t bytes_left_limit);

    static timestamp now();

private:
    type _v = missing().value();
};

using timestamp_clock = std::chrono::system_clock;

inline timestamp_clock::duration duration_since_epoch(timestamp ts) {
    return std::chrono::duration_cast<timestamp_clock::duration>(
      std::chrono::milliseconds{ts.value()});
}

inline timestamp to_timestamp(timestamp_clock::time_point ts) {
    return timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
                       ts.time_since_epoch())
                       .count());
}

inline timestamp to_timestamp(ss::manual_clock::time_point ts) {
    return timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
                       ts.time_since_epoch())
                       .count());
}

inline timestamp new_timestamp() {
    // This mimics System.currentTimeMillis() which needs a duration_cast<>
    // around the current timestamp computed from epoch.
    return to_timestamp(timestamp_clock::now());
}

inline timestamp timestamp::now() { return new_timestamp(); }
} // namespace model
