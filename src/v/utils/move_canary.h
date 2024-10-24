/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <cassert>
#include <utility>

/**
 * @brief An object which tracks whether it is in a moved-from state.
 *
 * This object simply tracks whether it is in a moved-from that: that is
 * whether the last relevant operation on this object (or ancestor object) was
 * to use it as the source for move-construction or move-assignmnet.
 *
 * Once a canary is in a moved-from state, it could be reset to not-moved-from
 * via assignment if the rhs is not moved from.
 */
class move_canary {
    bool _moved_from{};

public:
    /**
     * @brief Return true iff this object is in a moved-from state.
     */
    constexpr bool is_moved_from() const { return _moved_from; }

    /**
     * @brief Assert that the object is not in a moved-from state.
     *
     * Fails with the given message if the object is moved-from.
     */
    void assert_not_moved_from() { assert(!is_moved_from()); }

    /**
     * @brief Default construct a canary in a not-moved-from state.
     */
    constexpr move_canary() = default;
    constexpr ~move_canary() = default;

    /**
     * @brief Move-contruct a canary.
     *
     * This results in the source object being moved-from and the moved-from
     * state of this object being the same as the source's original value.
     */
    constexpr move_canary(move_canary&& rhs) noexcept
      : _moved_from(rhs._moved_from) {
        rhs._moved_from = true;
    }

    /**
     * @brief Copy-contruct a canary.
     *
     * This object will have the same moved-from state as the source.
     */
    constexpr move_canary(const move_canary&) = default;

    /**
     * @brief Move-assign a canary.
     *
     * This results in the source object being moved-from and the moved-from
     * state of this object being the same as the source's original value.
     */
    constexpr move_canary& operator=(move_canary&& rhs) noexcept {
        _moved_from = std::exchange(rhs._moved_from, true);
        return *this;
    }

    /**
     * @brief Assign to this canary.
     *
     * The state is simply copied from the source.
     */
    constexpr move_canary& operator=(const move_canary& rhs) noexcept = default;
};

/**
 * @brief A inactive move_canary-like object.
 *
 * This is the inactive version of the canary used to implement the release side
 * of the debug-only canary. It has no state and should have no size impact if
 * used as a base class or a [[no_unique_address]] member and no CPU impact when
 * callign the trivial methods.
 *
 */
struct inactive_move_canary {
    constexpr bool is_moved_from() const { return false; }
};

/**
 * This is a move_canary object that is active only in debug mode (when NDEBUG
 * is not defined). In release mode, it simply always reports that the object is
 * not in a moved-from state.
 *
 * It is intended for use in cases where you wish to
 * check the moved-from state only in debug mode due to the memory and CPU costs
 * of checking in release.
 */
#ifndef NDEBUG
using debug_move_canary = move_canary;
#else
using debug_move_canary = inactive_move_canary;
#endif
