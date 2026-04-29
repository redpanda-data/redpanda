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
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <memory>
#include <optional>
#include <utility>

namespace cluster {
template<typename Clock>
class cluster_epoch_service;
class partition_manager;
class members_table;
} // namespace cluster

namespace rpc {
class connection_cache;
} // namespace rpc

namespace cloud_topics {
class inflight_write_tracker;
} // namespace cloud_topics

namespace cloud_topics::l0::gc {

/// Establishes a cluster-wide safe-to-GC epoch for L0 garbage collection.
///
/// GC must not delete L0 objects at epoch E unless no future writes will
/// use epoch <= E and all existing data at epoch <= E has been reconciled
/// to L1.
///
/// **Leader (barrier_loop):** Computes a candidate epoch from health
/// reports, fans out advance_barrier RPCs to all nodes concurrently,
/// polls until every node reports ready. Loop and poll intervals are
/// cluster-tunable.
///
/// **Handler (handle_barrier):** On first call for a new candidate:
/// invalidates the epoch cache, kicks off an async drain of in-flight
/// writes, returns pending immediately (drain does not block the RPC).
/// On subsequent calls: checks drain completion, then writes
/// advance_gc_epoch(candidate) to all local leader partitions. Returns
/// ready when all writes succeed.
class epoch_barrier : public ss::peering_sharded_service<epoch_barrier> {
public:
    /// Abstraction over partition_manager for testability. The barrier
    /// only needs to enumerate cloud topic partitions and look up
    /// individual ones.
    class partition_source {
    public:
        struct info {
            model::term_id term;
            bool is_leader;
            bool has_epoch;
        };
        virtual ~partition_source() = default;

        /// All kafka-namespace cloud topic partitions on this shard.
        virtual chunked_vector<std::pair<model::ntp, info>>
        cloud_topic_partitions() const = 0;

        /// Look up a specific partition. Returns nullopt if not found or
        /// not a cloud topic.
        virtual std::optional<info> get(const model::ntp&) const = 0;

        /// Write advance_gc_epoch(epoch) to the named partition.
        /// Returns true on success, false on failure (e.g. not leader,
        /// timeout).
        virtual ss::future<bool> write_gc_epoch(
          const model::ntp& ntp,
          cluster_epoch epoch,
          model::timeout_clock::time_point deadline,
          ss::abort_source& as) = 0;
    };

    /// Factory for the default production partition source backed by
    /// cluster::partition_manager.
    static std::unique_ptr<partition_source>
    make_default_partition_source(cluster::partition_manager&);

    /// Abstraction over cluster membership for testability. The barrier
    /// needs the local node's ID and the set of all node IDs.
    class node_source {
    public:
        virtual ~node_source() = default;
        virtual model::node_id self() const = 0;
        virtual std::vector<model::node_id> node_ids() const = 0;
    };

    /// Factory for the default production node source backed by
    /// cluster::members_table.
    static std::unique_ptr<node_source>
    make_default_node_source(model::node_id, cluster::members_table&);

    /// Result of a single advance_barrier call.
    enum class barrier_status : uint8_t {
        ready,
        pending,
        error,
    };

    epoch_barrier(
      ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>&
        epoch_service,
      inflight_write_tracker& tracker,
      std::unique_ptr<partition_source> partitions,
      std::unique_ptr<node_source> nodes,
      ss::sharded<::rpc::connection_cache>* connections);

    ~epoch_barrier();

    ss::future<> stop();

    /// Core barrier method. Called on every node (via RPC dispatch or
    /// advance_local). Always runs on shard 0 — callers must dispatch.
    ///
    /// @param candidate The candidate epoch for this round.
    /// @param prev_safe_epoch Safe epoch from the previous completed round.
    ///        If set, advance_gc_epoch(prev_safe_epoch) is written to local
    ///        leader partitions (fire-and-forget) before starting the drain.
    ///
    /// 1. If new candidate: publishes prev_safe_epoch if set, invalidates
    ///    epoch cache, kicks off async drain, returns pending.
    /// 2. If draining: checks completion. Returns ready when drain finishes.
    ss::future<barrier_status> handle_barrier(
      cluster_epoch candidate,
      std::optional<cluster_epoch> prev_safe_epoch = std::nullopt);

    /// Start or stop the leader-side background loop.
    ss::future<> set_leader(bool is_leader);

    /// Fire-and-forget leadership notification. Bridges the synchronous
    /// leadership callback into the async set_leader method.
    void notify_leadership_change(bool is_leader) noexcept;

private:
    ss::future<bool> advance_local(
      cluster_epoch candidate, std::optional<cluster_epoch> prev_safe_epoch);
    ss::future<bool> advance_remote(
      model::node_id node_id,
      cluster_epoch candidate,
      std::optional<cluster_epoch> prev_safe_epoch);
    ss::future<bool> fan_out_advance_barrier(
      cluster_epoch candidate, std::optional<cluster_epoch> prev_safe_epoch);
    ss::future<bool> publish_safe_epoch(cluster_epoch epoch);
    ss::future<bool> publish_safe_epoch_local(
      cluster_epoch epoch, model::timeout_clock::time_point deadline);

    struct round_state {
        cluster_epoch candidate;
    };

    class barrier_loop;

    // Every-shard state.
    ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>&
      _epoch_service;
    inflight_write_tracker& _tracker;
    std::unique_ptr<partition_source> _partitions;
    std::optional<round_state> _round;
    // Shard-0 only: in-progress drain future for the current round.
    // Set when a new round starts; consumed when the drain completes.
    std::optional<ss::future<>> _drain_future;

    // Leader-only state.
    std::unique_ptr<node_source> _nodes;
    ss::sharded<::rpc::connection_cache>* _connections;
    std::unique_ptr<barrier_loop> _loop;
    ss::gate _gate;
};

} // namespace cloud_topics::l0::gc
