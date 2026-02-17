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
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "rpc/errc.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <expected>
#include <memory>
#include <optional>
#include <utility>

namespace cluster {
template<typename Clock>
class cluster_epoch_service;
class partition_manager;
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
///   the cached epoch, and resets round state so the next poll_drain
///   re-collects seal points with current leadership
/// - poll_drain: drains all in-flight writes across all local shards,
///   records seal points (each leader partition's committed offset and term
///   at drain time), then polls until each partition's LRO has caught up
///   to its seal point
class epoch_barrier_coordinator
  : public ss::peering_sharded_service<epoch_barrier_coordinator> {
public:
    /// Abstraction over partition_manager for testability. The coordinator
    /// only needs to enumerate cloud topic partitions and look up individual
    /// ones — this interface captures exactly those two operations.
    class partition_source {
    public:
        struct info {
            model::offset committed_offset;
            model::term_id term;
            bool is_leader;
            std::optional<model::offset> last_reconciled_log_offset;
        };
        virtual ~partition_source() = default;

        /// All kafka-namespace cloud topic partitions on this shard.
        virtual chunked_vector<std::pair<model::ntp, info>>
        cloud_topic_partitions() const = 0;

        /// Look up a specific partition. Returns nullopt if not found or
        /// not a cloud topic.
        virtual std::optional<info> get(const model::ntp&) const = 0;
    };

    /// Factory for the default production partition source backed by
    /// cluster::partition_manager.
    static std::unique_ptr<partition_source>
    make_default_partition_source(cluster::partition_manager&);

    epoch_barrier_coordinator(
      ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>&
        epoch_service,
      data_plane_api& data_plane,
      std::unique_ptr<partition_source> partitions);

    ss::future<> start();
    ss::future<> stop();

    /// Invalidate the local epoch cache so that all future epoch queries
    /// return values > candidate. Also resets round state so the next
    /// poll_drain re-drains and re-collects seal points.
    ss::future<std::expected<std::monostate, ::rpc::errc>>
    invalidate(cluster_epoch candidate);

    /// Drain in-flight writes and wait for reconciliation.
    ///
    /// On first call after invalidation: drains in-flight writes and
    /// records seal points (each leader partition's committed offset and
    /// term). On subsequent calls: checks whether each partition's LRO
    /// has caught up to its seal point. Fails if any partition's term
    /// changed since the seal point was recorded.
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
    struct seal_point {
        model::offset committed;
        model::term_id term;
    };

    /// Seal points keyed by topic -> partition_id, avoiding redundant copies
    /// of namespace and topic name strings across partitions of the same
    /// topic. Only kafka-namespace cloud topic partitions are tracked.
    using partition_seals = chunked_hash_map<model::partition_id, seal_point>;
    using seal_map = chunked_hash_map<model::topic, partition_seals>;

    struct round_state {
        cluster_epoch candidate;
        seal_map seals;
    };

    /// Result of checking seal points on a single shard. Values are
    /// ordered so that std::max over shards yields the worst case.
    enum class seal_check_result : uint8_t {
        /// All seal points verified: LRO >= committed for every entry.
        reconciled,
        /// Some partitions not yet caught up; keep polling.
        pending,
        /// A sealed partition lost leadership or was removed. The seal
        /// table is stale — must reset and redrain.
        stale,
    };

    /// Record each leader cloud topic partition's committed offset and
    /// term on this shard.
    void collect_local_seal_points(cluster_epoch candidate);

    /// Check whether each leader partition's LRO >= its seal point.
    /// On term mismatch, refreshes that entry's seal point (pushing the
    /// target offset forward) and returns pending. Also adds seal points
    /// for any new leaders not yet in the map. Returns stale if any
    /// sealed partition lost leadership, resetting the local round.
    seal_check_result check_local_seal_points();

    /// Look up the seal point for a given topic/partition, or nullptr.
    seal_point* find_seal(const model::topic& topic, model::partition_id pid);

    /// Insert or update a seal point for a given topic/partition.
    void upsert_seal(
      const model::topic& topic, model::partition_id pid, seal_point sp);

    ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>&
      _epoch_service;
    data_plane_api& _data_plane;
    std::unique_ptr<partition_source> _partitions;
    std::optional<round_state> _round;
    std::optional<cluster_epoch> _safe_epoch;
};

} // namespace cloud_topics::l0::gc
