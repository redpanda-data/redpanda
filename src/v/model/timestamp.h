/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <compare>
#include <cstdint>
#include <iosfwd>
#include <limits>

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

    friend std::ostream& operator<<(std::ostream&, timestamp);

    static timestamp now();

private:
    type _v = missing().value();
};

using timestamp_clock = std::chrono::system_clock;

inline timestamp to_timestamp(timestamp_clock::time_point ts) {
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
