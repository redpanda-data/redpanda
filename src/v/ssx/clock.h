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

#include "ssx/time.h"

/*
 * SSX Temporal Library - Clock Virtual Interfaces
 * ================================================
 *
 * This library provides virtual clock interfaces that enable dependency
 * injection and testing without template-based code. Instead of templating
 * all your types on clock (e.g., `MyClass<Clock>`), you can now use
 * references to clock interfaces (e.g., `MyClass(wall_clock& clock)`).
 *
 * Two clock types are provided:
 * - `steady_clock`: Monotonic clock that never goes backwards
 * - `wall_clock`: Real-world time clock (can jump due to NTP/DST)
 *
 * Usage in production code:
 *   void my_function(ssx::wall_clock& clock) {
 *       auto now = clock.now();
 *       auto deadline = now + ssx::duration(1'000'000'000);
 *   }
 *
 * Usage in tests:
 *   auto& test_clock = ssx::manual_wall_clock();
 *   my_function(test_clock);
 *   ss::manual_clock::advance(1s);
 *   // Verify timeout behavior...
 */

namespace ssx {

/// Virtual interface for monotonic steady clocks
/// Monotonic clocks never go backwards and are not affected by system time
/// changes
class steady_clock {
public:
    virtual ~steady_clock() = default;

    /// Get current instant from the steady clock
    virtual instant now() const noexcept = 0;

    /// Get a descriptive name for this clock (for debugging/logging)
    virtual const char* name() const noexcept = 0;
};

/// Virtual interface for wall clocks
/// Wall clocks represent real-world time and can jump due to NTP adjustments,
/// DST changes, or manual system time changes
class wall_clock {
public:
    virtual ~wall_clock() = default;

    /// Get current wall time from the clock
    virtual wall_time now() const noexcept = 0;

    /// Get a descriptive name for this clock (for debugging/logging)
    virtual const char* name() const noexcept = 0;
};

// Get the high-resolution steady clock (seastar::steady_clock)
// Returns a singleton instance - use for precise monotonic time
steady_clock& hires_steady_clock() noexcept;

// Get the low-resolution steady clock (seastar::lowres_clock)
// Returns a singleton instance - more efficient for coarse monotonic time
steady_clock& lowres_steady_clock() noexcept;

// Get the high-resolution wall clock (std::chrono::system_clock)
// Returns a singleton instance - use for precise system time
wall_clock& hires_wall_clock() noexcept;

// Get the low-resolution wall clock (seastar::lowres_system_clock)
// Returns a singleton instance - more efficient for coarse system time
wall_clock& lowres_wall_clock() noexcept;

// Create a manual wall clock for testing
// Control time with seastar::manual_clock::advance()
wall_clock& manual_wall_clock();

// Create a manual steady clock for testing
// Control time with seastar::manual_clock::advance()
steady_clock& manual_steady_clock();

} // namespace ssx
