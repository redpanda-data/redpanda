/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "absl/time/time.h"
#include "base/format_to.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>

#include <chrono>
#include <cstdint>
#include <type_traits>

/*
 * SSX Temporal Library - Type-Safe Time Value Types
 * ==================================================
 *
 * This library provides strong time value types with saturating arithmetic:
 *
 * - `duration`: Represents a time interval (can be negative)
 * - `instant`: Monotonic clock instant (steady, cannot go backwards)
 * - `wall_time`: Wall clock instant (can jump due to NTP/DST adjustments)
 *
 * All types use int64_t nanoseconds internally and employ saturating
 * arithmetic to prevent overflow/underflow bugs. Type safety prevents
 * mixing instant and wall_time incorrectly.
 *
 * Usage:
 *   ssx::duration d = ssx::duration::seconds(1);
 *   ssx::instant t1 = clock.now();
 *   ssx::instant t2 = t1 + d;
 *   ssx::duration elapsed = t2 - t1;
 *
 * Special Values:
 *   ssx::duration::infinite()
 *   ssx::instant::infinite_future()
 *   ssx::instant::infinite_past()
 */

namespace ssx {

// Forward declarations
class duration;
class instant;
class wall_time;

// Time duration value type with saturating arithmetic
// Represents a time interval in nanoseconds (can be negative)
class duration {
public:
    // Create infinite duration
    static constexpr duration infinite() noexcept {
        return duration(absl::InfiniteDuration());
    }

    // Create zero duration
    static constexpr duration zero() noexcept {
        return duration(absl::ZeroDuration());
    }

    // Saturating factory methods
    static constexpr duration hours(int64_t s) noexcept {
        return duration(absl::Hours(s));
    }
    static constexpr duration minutes(int64_t s) noexcept {
        return duration(absl::Minutes(s));
    }
    static constexpr duration seconds(int64_t s) noexcept {
        return duration(absl::Seconds(s));
    }
    static constexpr duration milliseconds(int64_t ms) noexcept {
        return duration(absl::Milliseconds(ms));
    }
    static constexpr duration microseconds(int64_t us) noexcept {
        return duration(absl::Microseconds(us));
    }
    static constexpr duration nanoseconds(int64_t ns) noexcept {
        return duration(absl::Nanoseconds(ns));
    }

    static duration from_chrono(std::chrono::hours s) noexcept {
        return duration::hours(s.count());
    }
    static duration from_chrono(std::chrono::minutes s) noexcept {
        return duration::minutes(s.count());
    }
    static duration from_chrono(std::chrono::seconds s) noexcept {
        return duration::seconds(s.count());
    }
    static duration from_chrono(std::chrono::milliseconds ms) noexcept {
        return duration::milliseconds(ms.count());
    }
    static duration from_chrono(std::chrono::microseconds us) noexcept {
        return duration::microseconds(us.count());
    }
    static duration from_chrono(std::chrono::nanoseconds ns) noexcept {
        return duration::nanoseconds(ns.count());
    }

    // Construct from nanoseconds
    explicit constexpr duration(absl::Duration duration) noexcept
      : _dur(duration) {}

    // Default construction = zero
    constexpr duration() noexcept
      : _dur(absl::ZeroDuration()) {}

    constexpr bool is_infinite() const noexcept {
        return _dur == absl::InfiniteDuration();
    }

    // Convert to std::chrono duration
    template<typename Duration>
    Duration to_chrono() const noexcept {
        if constexpr (std::is_same_v<Duration, std::chrono::nanoseconds>) {
            return absl::ToChronoNanoseconds(_dur);
        } else if constexpr (
          std::is_same_v<Duration, std::chrono::microseconds>) {
            return absl::ToChronoMicroseconds(_dur);
        } else if constexpr (
          std::is_same_v<Duration, std::chrono::milliseconds>) {
            return absl::ToChronoMilliseconds(_dur);
        } else if constexpr (std::is_same_v<Duration, std::chrono::seconds>) {
            return absl::ToChronoSeconds(_dur);
        } else if constexpr (std::is_same_v<Duration, std::chrono::minutes>) {
            return absl::ToChronoMinutes(_dur);
        } else if constexpr (std::is_same_v<Duration, std::chrono::hours>) {
            return absl::ToChronoHours(_dur);
        } else {
            static_assert(false, "unsupported type");
        }
    }

    // Conversion helpers (truncating)
    constexpr int64_t to_hours() const noexcept {
        return absl::ToInt64Hours(_dur);
    }
    constexpr int64_t to_minutes() const noexcept {
        return absl::ToInt64Minutes(_dur);
    }
    constexpr int64_t to_seconds() const noexcept {
        return absl::ToInt64Seconds(_dur);
    }
    constexpr int64_t to_milliseconds() const noexcept {
        return absl::ToInt64Milliseconds(_dur);
    }
    constexpr int64_t to_microseconds() const noexcept {
        return absl::ToInt64Microseconds(_dur);
    }
    constexpr int64_t to_nanoseconds() const noexcept {
        return absl::ToInt64Nanoseconds(_dur);
    }

    // Saturating addition
    constexpr duration operator+(duration other) const noexcept {
        return duration(_dur + other._dur);
    }

    // Saturating subtraction
    constexpr duration operator-(duration other) const noexcept {
        return duration(_dur - other._dur);
    }

    // Saturating multiplication by scalar
    constexpr duration operator*(int64_t scalar) const noexcept {
        return duration(_dur * scalar);
    }

    // Division by scalar (saturating)
    constexpr duration operator/(int64_t scalar) const noexcept {
        return duration(_dur / scalar);
    }

    // Negation (saturating)
    constexpr duration operator-() const noexcept { return duration(-_dur); }

    // Compound assignment operators
    constexpr duration& operator+=(duration other) noexcept {
        _dur += other._dur;
        return *this;
    }

    constexpr duration& operator-=(duration other) noexcept {
        _dur -= other._dur;
        return *this;
    }

    constexpr duration& operator*=(int64_t scalar) noexcept {
        _dur *= scalar;
        return *this;
    }

    constexpr duration& operator/=(int64_t scalar) noexcept {
        _dur /= scalar;
        return *this;
    }

    // Comparison operators
    constexpr bool operator==(const duration& other) const noexcept = default;
    constexpr auto operator<=>(const duration& other) const noexcept = default;

    // Formatting support
    fmt::format_context::iterator
    format_to(fmt::format_context::iterator out) const;

    explicit operator absl::Duration() const { return _dur; }

private:
    absl::Duration _dur;
};

// Monotonic clock instant value type with saturating arithmetic
// Cannot move backwards in time (steady/monotonic)
class instant {
public:
    // Create infinite future instant
    static constexpr instant infinite_future() noexcept {
        return instant(absl::InfiniteFuture());
    }

    // Create infinite past instant
    static constexpr instant infinite_past() noexcept {
        return instant(absl::InfinitePast());
    }

    static instant from_chrono(seastar::lowres_clock::time_point tp) noexcept {
        return instant() + duration::from_chrono(tp.time_since_epoch());
    }
    static instant from_chrono(seastar::manual_clock::time_point tp) noexcept {
        return instant() + duration::from_chrono(tp.time_since_epoch());
    }
    static instant
    from_chrono(std::chrono::steady_clock::time_point tp) noexcept {
        return instant() + duration::from_chrono(tp.time_since_epoch());
    }

    // Construct from nanoseconds since epoch
    explicit constexpr instant(absl::Time time) noexcept
      : _time(time) {}

    // Default construction = (zero instant)
    constexpr instant() noexcept = default;

    // Check if infinite future
    constexpr bool is_infinite_future() const noexcept {
        return _time == absl::InfiniteFuture();
    }

    // Check if infinite past
    constexpr bool is_infinite_past() const noexcept {
        return _time == absl::InfinitePast();
    }

    template<typename Clock>
    typename Clock::time_point to_chrono() const noexcept {
        auto diff = *this - instant();
        return typename Clock::time_point(
          diff.to_chrono<typename Clock::duration>());
    }

    // Add duration to instant (saturating)
    constexpr instant operator+(duration d) const noexcept {
        return instant(_time + absl::Duration(d));
    }

    // Subtract duration from instant (saturating)
    constexpr instant operator-(duration d) const noexcept {
        return instant(_time - absl::Duration(d));
    }

    // Subtract two instants to get duration (saturating)
    constexpr duration operator-(instant other) const noexcept {
        return duration(_time - other._time);
    }

    // Compound assignment
    constexpr instant& operator+=(duration d) noexcept {
        _time += absl::Duration(d);
        return *this;
    }

    constexpr instant& operator-=(duration d) noexcept {
        _time -= absl::Duration(d);
        return *this;
    }

    // Comparison operators
    constexpr bool operator==(const instant& other) const noexcept = default;
    constexpr auto operator<=>(const instant& other) const noexcept = default;

    // Formatting support
    fmt::format_context::iterator
    format_to(fmt::format_context::iterator out) const;

private:
    // Time since boot
    absl::Time _time;
};

// Wall clock instant value type with saturating arithmetic
// Represents real-world time, can jump (NTP adjustments, DST, etc.)
class wall_time {
public:
    // Create infinite future wall_time
    static constexpr wall_time infinite_future() noexcept {
        return wall_time(absl::InfiniteFuture());
    }

    // Create infinite past wall_time
    static constexpr wall_time infinite_past() noexcept {
        return wall_time(absl::InfinitePast());
    }

    static wall_time
    from_chrono(std::chrono::system_clock::time_point tp) noexcept {
        return wall_time(absl::FromChrono(tp));
    }
    static wall_time
    from_chrono(seastar::lowres_system_clock::time_point tp) noexcept {
        return wall_time() + duration::from_chrono(tp.time_since_epoch());
    }
    static wall_time
    from_chrono(seastar::manual_clock::time_point tp) noexcept {
        return wall_time() + duration::from_chrono(tp.time_since_epoch());
    }

    // Construct from nanoseconds since Unix epoch
    explicit constexpr wall_time(absl::Time t) noexcept
      : _time(t) {}

    // Default construction = Unix epoch (zero)
    constexpr wall_time() noexcept
      : _time(absl::UnixEpoch()) {}

    // Check if infinite future
    constexpr bool is_infinite_future() const noexcept {
        return _time == absl::InfiniteFuture();
    }

    // Check if infinite past
    constexpr bool is_infinite_past() const noexcept {
        return _time == absl::InfinitePast();
    }

    template<typename Clock>
    typename Clock::time_point to_chrono() const noexcept {
        if constexpr (std::is_same_v<std::chrono::system_clock, Clock>) {
            return absl::ToChronoTime(_time);
        }
        auto diff = *this - wall_time();
        return typename Clock::time_point(
          diff.to_chrono<typename Clock::duration>());
    }

    // Add duration to wall_time (saturating)
    constexpr wall_time operator+(duration d) const noexcept {
        return wall_time(_time + absl::Duration(d));
    }

    // Subtract duration from wall_time (saturating)
    constexpr wall_time operator-(duration d) const noexcept {
        return wall_time(_time - absl::Duration(d));
    }

    // Subtract two wall_times to get duration (saturating)
    constexpr duration operator-(wall_time other) const noexcept {
        return duration(_time - other._time);
    }

    // Compound assignment
    constexpr wall_time& operator+=(duration d) noexcept {
        _time += absl::Duration(d);
        return *this;
    }

    constexpr wall_time& operator-=(duration d) noexcept {
        _time -= absl::Duration(d);
        return *this;
    }

    // Comparison operators
    constexpr bool operator==(const wall_time& other) const noexcept = default;
    constexpr auto operator<=>(const wall_time& other) const noexcept = default;

    // Formatting support
    fmt::format_context::iterator
    format_to(fmt::format_context::iterator out) const;

private:
    absl::Time _time;
};

// Allow duration * scalar (commutative, saturating)
constexpr duration operator*(int64_t scalar, duration d) noexcept {
    return d * scalar;
}

} // namespace ssx
