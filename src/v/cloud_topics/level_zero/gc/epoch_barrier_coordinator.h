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
#include "cloud_topics/types.h"
#include "rpc/errc.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <expected>
#include <optional>

namespace cluster {
template<typename Clock>
class cluster_epoch_service;
} // namespace cluster

namespace cloud_topics {
class data_plane_api;
} // namespace cloud_topics

namespace cloud_topics::l0::gc {

/// Per-node coordinator for the epoch barrier protocol.
///
/// Each node runs this sharded service. On receipt of an invalidate or
/// poll_drain RPC from the barrier manager leader, it coordinates locally:
/// - invalidate: forwards to the local cluster_epoch_service to invalidate
///   the cached epoch
/// - poll_drain: drains all in-flight writes across all local shards and
///   reports completion once no writes from before the invalidation remain
class epoch_barrier_coordinator
  : public ss::peering_sharded_service<epoch_barrier_coordinator> {
public:
    epoch_barrier_coordinator(
      ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>&
        epoch_service,
      data_plane_api& data_plane);

    ss::future<> start();
    ss::future<> stop();

    /// Invalidate the local epoch cache so that all future epoch queries
    /// return values > candidate.
    ss::future<std::expected<std::monostate, ::rpc::errc>>
    invalidate(cluster_epoch candidate);

    /// Check if all local shards have drained in-flight uploads at
    /// epochs <= candidate.
    ss::future<std::expected<bool, ::rpc::errc>>
    poll_drain(cluster_epoch candidate);

    /// Store the safe-to-GC epoch published by the barrier manager leader.
    ss::future<std::expected<std::monostate, ::rpc::errc>>
    publish_safe_epoch(cluster_epoch safe_epoch);

    /// Returns the latest safe-to-GC epoch, or nullopt if none published yet.
    std::optional<cluster_epoch> safe_epoch() const noexcept {
        return _safe_epoch;
    }

private:
    ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>&
      _epoch_service;
    data_plane_api& _data_plane;
    std::optional<cluster_epoch> _safe_epoch;
};

} // namespace cloud_topics::l0::gc
