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
#include "cloud_io/admission_types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <memory>

namespace cloud_io {

class admission_policy;

/// Per-shard admission gate for cloud_io operations.
///
/// The admission_controller sits inside cloud_storage_clients::client_pool and is
/// consulted on every lease acquisition. Each pool shard owns one
/// admission_controller instance and one policy, chosen at construction from the
/// cloud_io_admission_policy cluster property.
///
///   admission_controller ─owns→ unique_ptr<admission_policy>
///                       │
///                       └─ passthrough_policy │ ...
///
/// admit/release/observability calls are forwarded to the active
/// policy. Admission state lives in the policy (per-group counters,
/// waiters, etc.). The caller is responsible for pairing each `admit`
/// with a `release` on the same admission_controller instance.
class admission_controller {
public:
    admission_controller(size_t capacity, admission_config = {});
    admission_controller(const admission_controller&) = delete;
    admission_controller& operator=(const admission_controller&) = delete;
    admission_controller(admission_controller&&) = delete;
    admission_controller& operator=(admission_controller&&) = delete;
    ~admission_controller() noexcept;

    /// Drains waiters, stops the policy.
    ss::future<> stop();

    /// Wait until the policy admits an op tagged with `g`.
    /// \throws ss::abort_requested_exception
    ss::future<> admit(group_id g, ss::abort_source& as);

    /// Non-blocking admit. Returns false if admit would queue.
    [[nodiscard]] bool try_admit(group_id g);

    /// Release a slot. Called by the lease deleter, so may be called
    /// across a shard boundary for a borrowed lease.
    void release(group_id g);

    size_t in_flight(group_id) const;
    size_t waiters(group_id) const;
    size_t available_slots() const;
    size_t total_capacity() const;
    bool has_waiters() const;

private:
    static std::unique_ptr<admission_policy>
    make_policy(size_t capacity, admission_config);

    std::unique_ptr<admission_policy> _policy;
    bool _draining = false;
};

} // namespace cloud_io
