/*
 * Copyright 2021 Redpanda Data, Inc.
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

#include <seastar/core/gate.hh>

/// \brief RAII wrapper that holds the door^W gate open
///        until the end of scope
///
/// The guard is moveable. The lifetime will be transferred
/// to the new gate_guard instance.
///
/// \example ss::gate gate;
///          {
///             gate_guard g{gate};
///             assert(gate.get_count() == 1);
///          }
///          assert(gate.get_count() == 0);
struct gate_guard final {
    explicit gate_guard(ss::gate& g)
      : _g(&g) {
        _g->enter();
    }
    gate_guard(gate_guard&& o) noexcept
      : _g(o._g) {
        o._g = nullptr;
    }
    gate_guard& operator=(gate_guard&& o) noexcept {
        if (this != &o) {
            this->~gate_guard();
            _g = o._g;
            o._g = nullptr;
        }
        return *this;
    }
    gate_guard(const gate_guard&) = delete;
    gate_guard& operator=(const gate_guard&) = delete;
    ~gate_guard() noexcept {
        if (_g) {
            _g->leave();
        }
    }

private:
    ss::gate* _g;
};
