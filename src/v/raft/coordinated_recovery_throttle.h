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

#include "base/seastarx.h"
#include "config/property.h"
#include "metrics/metrics.h"
#include "ssx/semaphore.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace raft {

/// A recovery throttle that coordinates the total available rate across shards.
/// Coordination is done by shard 0. The intent of the coordination is to
/// saturate the available node recovery rate (bandwidth). This is achieved by
/// redistributing the unused bandwidth from idle shards among the busy shards.
/// Internally it wraps a simple token bucket (semaphore) that is periodically
/// reset with the desired capacity as deemed by the coordinator.

/// In order to guarantee fairness, each shard always has access to its fair
/// share of bandwidth (= total_available/num_shards). Any unused portion of
/// this fair share is redistributed among the busy shards.
class coordinated_recovery_throttle
  : public ss::peering_sharded_service<coordinated_recovery_throttle> {
public:
    explicit coordinated_recovery_throttle(
      config::binding<size_t> /* node capacity in bytes per sec*/,
      config::binding<bool> /* use static rate allocation*/);
    coordinated_recovery_throttle(const coordinated_recovery_throttle&)
      = delete;
    coordinated_recovery_throttle&
    operator=(const coordinated_recovery_throttle&)
      = delete;
    coordinated_recovery_throttle(coordinated_recovery_throttle&&) = delete;
    coordinated_recovery_throttle& operator=(coordinated_recovery_throttle&&)
      = delete;
    ~coordinated_recovery_throttle() noexcept = default;

    void shutdown();

    ss::future<> start();
    ss::future<> stop();

    /// A test helper to step through the ticks.
    ss::future<> tick_for_testing() { return do_coordinate_tick(); }

    ss::future<> throttle(size_t size, ss::abort_source& as) {
        return _throttler.throttle(size, as);
    }

    size_t available() const { return _throttler.available(); }
    size_t waiting_bytes() const { return _throttler.waiting_bytes(); }
    size_t admitted_bytes() const { return _throttler.admitted_bytes(); }

    void setup_metrics();

private:
    using clock_t = ss::lowres_clock;
    static constexpr ss::shard_id _coordinator_shard = ss::shard_id{0};

    /// A wrapper around a sempahore that tracks additional metrics
    /// used for coordination. The capacity of the bucket is refilled
    /// by the recovery throttle periodically as a part of coordination.
    class token_bucket {
    public:
        explicit token_bucket(size_t /*initial_size*/);
        ss::future<> throttle(size_t /*bytes*/, ss::abort_source&);
        /// Resets to the newly passed capacity argument. If the current
        /// availability is higher, the capacity is reduced, otherwise
        /// the delta is added.
        void reset_capacity(size_t /*new capacity*/);
        void shutdown() { return _sem.broken(); }
        size_t waiting_bytes() const { return _waiting_bytes; }
        size_t admitted_bytes() const {
            return _admitted_bytes_since_last_reset;
        }
        size_t available() const { return _sem.current(); }
        size_t last_reset_capacity() const { return _last_reset_capacity; }

    private:
        ssx::named_semaphore<> _sem;
        /// Counter tracking the total bytes throttled.
        size_t _waiting_bytes{0};
        /// Counter tracking the total bytes admitted since the capacity
        /// is last reset. We use this as a heuristic when estimating
        /// the capacity needed for the next tick. See required_capacity().
        size_t _admitted_bytes_since_last_reset{0};
        size_t _last_reset_capacity;
    };

    /// Rate allocated to a shard if the total rate is uniformly allocated
    /// among all shards. We call this a 'fair rate' in multiple places.
    size_t fair_rate_per_shard() const {
        return _rate_binding() / ss::smp::count;
    }

    /// Helpers to reset capacity on all/specific shard to the passed capacity.
    /// Used by the coordinator.
    ss::future<> reset_capacity_all_shards(size_t /* new capacity*/);
    ss::future<> reset_capacity_on_shard(ss::shard_id, size_t /*new capacity*/);

    void arm_coordinator_timer();

    /// The capacity in bytes required in the next period as reported to the
    /// callee shard. See below.
    size_t required_capacity() const;

    /// Throttle operates in ticks of period 1sec (hard code becaused rate is
    /// measured in bytes / sec).
    /// In each tick the coordinator shard (shard 0) requests the capacity
    /// needed by each shard for the next period. It gathers this information
    /// from all the shards and redistributes the total bandwidth among them
    /// them and fills up the shard local token buckets accordingly. See
    /// the implementation for more details.
    ss::future<> coordinate_tick();
    ss::future<> do_coordinate_tick();

    config::binding<size_t> _rate_binding;
    // Allocates fair share to all shards. A fall back option for issues
    // with dynamic allocation.
    config::binding<bool> _use_static_allocation;
    token_bucket _throttler;
    ss::gate _gate;
    ss::timer<clock_t> _coordinator;

    metrics::internal_metric_groups _internal_metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace raft
