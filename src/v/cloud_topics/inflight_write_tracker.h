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

#include "cloud_topics/types.h"

#include <seastar/core/future.hh>

#include <memory>

namespace cloud_topics {

/// Tracks in-flight cloud topic writes for epoch barrier drain.
///
/// The frontend adds tokens on the produce path; the epoch barrier drains
/// to ensure no writes at stale epochs are in flight before writing the
/// advance_gc_epoch command.
class inflight_write_tracker {
public:
    virtual ~inflight_write_tracker() = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    /// Create a token linked to this shard's tracking list.
    /// Caller must set token->done when the write completes.
    virtual std::unique_ptr<inflight_write_token> track() = 0;

    /// Atomically detach all shards' lists and await every outstanding
    /// token's done promise.
    virtual ss::future<> drain() = 0;

    /// Create the production implementation backed by a sharded
    /// per-shard intrusive list.
    static std::unique_ptr<inflight_write_tracker> make_default();
};

} // namespace cloud_topics
