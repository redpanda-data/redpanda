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

#include "ssx/clock.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>

#include <chrono>

namespace ssx {

// ============================================================================
// Clock Implementation Classes
// ============================================================================

// High-resolution steady clock (monotonic)
class hires_steady_clock_impl final : public steady_clock {
public:
    instant now() const noexcept override {
        return instant::from_chrono(seastar::steady_clock_type::now());
    }

    const char* name() const noexcept override {
        return "seastar::steady_clock";
    }
};

// Low-resolution steady clock (monotonic)
class lowres_steady_clock_impl final : public steady_clock {
public:
    instant now() const noexcept override {
        return instant::from_chrono(seastar::lowres_clock::now());
    }

    const char* name() const noexcept override {
        return "seastar::lowres_clock";
    }
};

// High-resolution wall clock (system time)
class hires_wall_clock_impl final : public wall_clock {
public:
    wall_time now() const noexcept override {
        return wall_time::from_chrono(std::chrono::system_clock::now());
    }

    const char* name() const noexcept override {
        return "std::chrono::system_clock";
    }
};

// Low-resolution wall clock (system time)
class lowres_wall_clock_impl final : public wall_clock {
public:
    wall_time now() const noexcept override {
        return wall_time::from_chrono(seastar::lowres_system_clock::now());
    }

    const char* name() const noexcept override {
        return "seastar::lowres_system_clock";
    }
};

// Wrapper for seastar::manual_clock as wall_clock (testing)
class manual_wall_clock_impl final : public wall_clock {
public:
    wall_time now() const noexcept override {
        return wall_time::from_chrono(seastar::manual_clock::now());
    }

    const char* name() const noexcept override {
        return "seastar::manual_clock";
    }
};

// Wrapper for seastar::manual_clock as steady_clock (testing)
class manual_steady_clock_impl final : public steady_clock {
public:
    instant now() const noexcept override {
        return instant::from_chrono(seastar::manual_clock::now());
    }

    const char* name() const noexcept override {
        return "seastar::manual_clock";
    }
};

// ============================================================================
// Singleton Clock Accessors
// ============================================================================

ssx::steady_clock& hires_steady_clock() noexcept {
    static hires_steady_clock_impl instance;
    return instance;
}

ssx::steady_clock& lowres_steady_clock() noexcept {
    static lowres_steady_clock_impl instance;
    return instance;
}

ssx::wall_clock& hires_wall_clock() noexcept {
    static hires_wall_clock_impl instance;
    return instance;
}

ssx::wall_clock& lowres_wall_clock() noexcept {
    static lowres_wall_clock_impl instance;
    return instance;
}

ssx::wall_clock& manual_wall_clock() {
    static manual_wall_clock_impl instance;
    return instance;
}

ssx::steady_clock& manual_steady_clock() {
    static manual_steady_clock_impl instance;
    return instance;
}

} // namespace ssx
