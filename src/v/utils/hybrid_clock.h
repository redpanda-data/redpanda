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
#include "base/likely.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>

#include <chrono>

/// The hybrid_clock is a thin wrapper around the seastar::lowres_clock
/// that can be swapped from seastar::manual_clock in tests by flipping
/// a runtime flag (per shard). The timestamps and durations are using
/// last bit as a flag indicating the type of clock used to produce
/// them.
class hybrid_clock {
    struct duration_with_flag {
        uint64_t nanoseconds{0};
        bool manual{false};
    };

public:
    // Same as std::chrono::steady_clock::rep
    //
    using duration = std::chrono::nanoseconds;
    using rep = duration::rep;
    using period = duration::period;
    using time_point = std::chrono::time_point<hybrid_clock, duration>;
    static constexpr bool is_steady = true;

    inline static time_point now() noexcept;

    inline static void update() noexcept;

    inline static void advance(duration d);

    inline static void set_manual_mode(bool mode);

    inline static bool is_manual(const time_point& t);

    friend bool operator==(const time_point& lhs, const time_point& rhs);
    friend auto operator<=>(const time_point& lhs, const time_point& rhs);
    friend duration operator-(const time_point& lhs, const time_point& rhs);
    friend time_point operator-(const time_point& lhs, const duration& rhs);
    friend time_point operator+(const time_point& lhs, const duration& rhs);

private:
    inline static void throw_clock_mismatch() {
        throw std::logic_error("clock mismatch");
    }
    // This will set zero bit of the duration to 0 if the lowres_clock  is used
    // or to 1 if manual clock is used. That's totally fine given that the clock
    // is lowres and the duration is expressed in nanoseconds. So the bit is
    // normally set to zero and will only be flipped to 1 in case if manual
    // clock is used.
    inline static void duration_bit_hack(duration& target, bool manual);
    // Extract the actual timestamp with the flag
    inline static duration_with_flag decode(const duration& d);

    inline static thread_local bool _manual{false}; // NOLINT
    inline static constexpr uint64_t mask = uint64_t(0) - 2;
};

inline hybrid_clock::time_point hybrid_clock::now() noexcept {
    auto dur = std::chrono::duration_cast<duration>(
      seastar::lowres_clock::now().time_since_epoch());
    if (unlikely(_manual)) {
        auto now = seastar::manual_clock::now();
        dur = std::chrono::duration_cast<duration>(now.time_since_epoch());
    }
    duration_bit_hack(dur, _manual);
    return time_point(dur);
}

inline void hybrid_clock::update() noexcept { seastar::lowres_clock::update(); }

inline void hybrid_clock::advance(hybrid_clock::duration d) {
    seastar::manual_clock::advance(
      std::chrono::duration_cast<seastar::manual_clock::duration>(d));
}

inline void hybrid_clock::set_manual_mode(bool mode) { _manual = mode; }

inline bool hybrid_clock::is_manual(const time_point& t) {
    const auto [nanos, flag] = decode(t.time_since_epoch());
    return flag;
}

inline void
hybrid_clock::duration_bit_hack(hybrid_clock::duration& target, bool manual) {
    bool zero_bit = manual == true;
    auto count = target.count();
    uint64_t bits = 0;
    static_assert(sizeof(bits) == sizeof(count));
    std::memcpy(&bits, &count, sizeof(bits));
    std::cout << "BH bits: " << bits << std::endl;
    if (zero_bit) {
        bits = bits | 1UL;
    } else {
        bits = bits & mask;
    }
    std::cout << "BH bits (after): " << bits << std::endl;
    std::memcpy(&count, &bits, sizeof(bits));
    target = duration(count);
}

// Extract the actual timestamp with the flag
inline hybrid_clock::duration_with_flag
hybrid_clock::decode(const hybrid_clock::duration& d) {
    auto count = d.count();
    uint64_t bits = count;
    std::memcpy(&bits, &count, sizeof(bits));
    std::cout << "Bits1: " << bits << std::endl;
    bool flag = (bits & 1UL) != 0;
    std::cout << "Flag: " << flag << std::endl;
    bits = bits & mask;
    std::cout << "Bits2: " << bits << std::endl;
    return {
      .nanoseconds = bits,
      .manual = flag,
    };
}

inline bool operator==(
  const hybrid_clock::time_point& lhs, const hybrid_clock::time_point& rhs) {
    auto [lhs_nanos, lhs_manual] = hybrid_clock::decode(lhs.time_since_epoch());
    auto [rhs_nanos, rhs_manual] = hybrid_clock::decode(rhs.time_since_epoch());
    if (lhs_manual != rhs_manual) {
        hybrid_clock::throw_clock_mismatch();
    }
    return lhs_nanos == rhs_nanos;
}

inline auto operator<=>(
  const hybrid_clock::time_point& lhs, const hybrid_clock::time_point& rhs) {
    auto [lhs_nanos, lhs_manual] = hybrid_clock::decode(lhs.time_since_epoch());
    auto [rhs_nanos, rhs_manual] = hybrid_clock::decode(rhs.time_since_epoch());
    if (lhs_manual != rhs_manual) {
        hybrid_clock::throw_clock_mismatch();
    }
    return lhs_nanos <=> rhs_nanos;
}

inline hybrid_clock::duration operator-(
  const hybrid_clock::time_point& lhs, const hybrid_clock::time_point& rhs) {
    std::cout << "NEEDLE1" << std::endl;
    auto [lhs_nanos, lhs_manual] = hybrid_clock::decode(lhs.time_since_epoch());
    auto [rhs_nanos, rhs_manual] = hybrid_clock::decode(rhs.time_since_epoch());
    if (lhs_manual != rhs_manual) {
        hybrid_clock::throw_clock_mismatch();
    }
    auto d = hybrid_clock::duration(lhs_nanos - rhs_nanos);
    // The bitflag is always inherited from the operands
    hybrid_clock::duration_bit_hack(d, lhs_manual);
    return d;
}

inline hybrid_clock::time_point operator-(
  const hybrid_clock::time_point& lhs, const hybrid_clock::duration& rhs) {
    auto [lhs_nanos, lhs_manual] = hybrid_clock::decode(lhs.time_since_epoch());
    auto d = std::chrono::nanoseconds(lhs_nanos) + rhs;
    hybrid_clock::duration_bit_hack(d, lhs_manual);
    return hybrid_clock::time_point(d);
}

inline hybrid_clock::time_point operator+(
  const hybrid_clock::time_point& lhs, const hybrid_clock::duration& rhs) {
    auto [lhs_nanos, lhs_manual] = hybrid_clock::decode(lhs.time_since_epoch());
    auto d = std::chrono::nanoseconds(lhs_nanos) + rhs;
    hybrid_clock::duration_bit_hack(d, lhs_manual);
    return hybrid_clock::time_point(d);
}

class manual_clock_scope {
public:
    manual_clock_scope() { hybrid_clock::set_manual_mode(true); }
    ~manual_clock_scope() { hybrid_clock::set_manual_mode(false); }
};
