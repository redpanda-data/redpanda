/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "config/property.h"

namespace cloud_topics::prefetch {

/// Per-shard byte reservation accounting for the L1 prefetch scheduler.
///
/// Tracks bytes reserved against a configurable budget so the scheduler can
/// admit or deny prefetch downloads, and allows starving streams to borrow
/// beyond the budget (anti-starvation). Pure synchronous accounting — no IO,
/// no Seastar futures.
class l1_memory_broker {
public:
    explicit l1_memory_broker(config::binding<size_t> budget_bytes);

    /// Current budget in bytes (re-read on each call to reflect live config).
    size_t budget() const;

    /// Total bytes currently reserved (including borrows).
    size_t reserved() const;

    /// Bytes available for normal prefetch: max(0, budget - reserved).
    size_t available() const;

    /// True when reserved exceeds the current budget.
    bool over_budget() const;

    /// Try to reserve n bytes for a normal prefetch.
    ///
    /// Returns false without modifying state if the reservation would exceed
    /// the budget.
    [[nodiscard]] bool try_reserve(size_t n);

    /// Force a reservation for an anti-starvation borrow (may exceed budget).
    void borrow(size_t n);

    /// Release n previously reserved bytes.
    ///
    /// Asserts that n does not exceed the current reservation.
    void release(size_t n);

private:
    config::binding<size_t> _budget;
    size_t _reserved{0};
};

} // namespace cloud_topics::prefetch
