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

#include "base/seastarx.h"
#include "cloud_io/scheduler_types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/util/later.hh>

#include <cstddef>

namespace cloud_io {

/// Abstract base for cloud_io::scheduler admission policies. A policy
/// decides whether and when an admit request is allowed to proceed.
class scheduler_policy {
public:
    explicit scheduler_policy(size_t capacity) noexcept
      : _capacity(capacity) {}
    scheduler_policy(const scheduler_policy&) = delete;
    scheduler_policy& operator=(const scheduler_policy&) = delete;
    scheduler_policy(scheduler_policy&&) = delete;
    scheduler_policy& operator=(scheduler_policy&&) = delete;
    virtual ~scheduler_policy() noexcept = default;

    /// Wait until the policy admits an op tagged with `g`. Throws
    /// ss::abort_requested_exception if `as` fires during the wait.
    virtual ss::future<> admit(group_id g, ss::abort_source& as) = 0;

    /// Non-blocking variant. Returns true if the op is admitted
    /// immediately, false if it would queue.
    [[nodiscard]] virtual bool try_admit(group_id g) = 0;

    /// Return one admitted slot. Called on the owning shard's
    /// scheduler when a lease drops (locally or via invoke_on for
    /// cross-shard borrows).
    virtual void release(group_id g) = 0;

    /// Observability getters.
    virtual size_t in_flight(group_id) const = 0;
    virtual size_t waiters(group_id) const = 0;
    virtual size_t available_slots() const = 0;
    virtual size_t total_capacity() const = 0;

    /// Optional lifecycle hook. Default no-op.
    virtual ss::future<> stop() { return ss::now(); }

protected:
    size_t _capacity;
};

} // namespace cloud_io
