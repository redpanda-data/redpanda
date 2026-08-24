/**
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cluster_link/errc.h"
#include "model/record.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

namespace cluster_link {

/**
 * \brief Confirms the local cluster's producer-ID allocator is advanced past
 * every producer ID already present in this cluster's partitions.
 *
 * Mirrored batches carry the source cluster's producer IDs verbatim, and each
 * partition's rm_stm rebuilds idempotency state from them on apply. Both
 * clusters allocate producer IDs from zero, so once writes are released after
 * a failover, a fresh producer could otherwise be handed an ID that collides
 * with mirrored state and have its writes silently deduplicated against it.
 * The failover path must therefore await this barrier before releasing
 * writes.
 */
class producer_id_barrier {
public:
    producer_id_barrier() = default;
    producer_id_barrier(const producer_id_barrier&) = delete;
    producer_id_barrier(producer_id_barrier&&) = delete;
    producer_id_barrier& operator=(const producer_id_barrier&) = delete;
    producer_id_barrier& operator=(producer_id_barrier&&) = delete;
    virtual ~producer_id_barrier() = default;

    /// Returns the observed cluster-wide maximum on success. Any error means
    /// the allocator was NOT confirmed advanced and the caller must not
    /// promote. Never returns an exceptional future — the caller is a
    /// noexcept path.
    virtual ss::future<result<::model::producer_id, errc>>
    advance() noexcept = 0;
};

/**
 * \brief Default barrier: scan the cluster-wide maximum producer ID, then
 * reset the allocator to that maximum plus a safety margin, awaited.
 *
 * Serializes concurrent callers (every link runs its own reconciler fiber)
 * and caches producer-ID exhaustion as a terminal state so a hopeless
 * all-broker scan is not repeated every reconciliation tick.
 */
class producer_id_barrier_impl final : public producer_id_barrier {
public:
    /**
     * \brief The scan and reset operations the barrier drives.
     *
     * Injected so the barrier logic is unit-testable and this header stays
     * free of the cluster/cloud_metadata dependency; the production adapter
     * lives with the service, which owns the required frontends.
     */
    class ops {
    public:
        ops() = default;
        ops(const ops&) = delete;
        ops(ops&&) = delete;
        ops& operator=(const ops&) = delete;
        ops& operator=(ops&&) = delete;
        virtual ~ops() = default;

        /// Cluster-wide maximum producer ID over all partitions. The scan
        /// cannot distinguish "no producers anywhere" from a real maximum of
        /// 0 — producer ID 0 is valid, so the barrier resets in both cases.
        virtual ss::future<result<::model::producer_id, errc>>
        scan_highest_pid() = 0;

        /// Advance the ID allocator so the next allocated producer ID is at
        /// least the given one. Monotonic on the allocator side.
        virtual ss::future<errc> reset_next_id(::model::producer_id) = 0;
    };

    explicit producer_id_barrier_impl(std::unique_ptr<ops> ops)
      : _ops(std::move(ops)) {}

    ss::future<result<::model::producer_id, errc>> advance() noexcept final;

    /// Aborts current and queued advances; safe to call more than once.
    void shutdown() noexcept;

private:
    ss::future<result<::model::producer_id, errc>> do_serialized_advance();
    ss::future<result<::model::producer_id, errc>> do_advance();

    std::unique_ptr<ops> _ops;
    ssx::semaphore _sem{1, "cluster_link::pid_barrier"};
    ss::abort_source _as;
    // Terminal: producer IDs only grow and the allocator never moves
    // backwards, so once the margin no longer fits below the int64 maximum a
    // rescan cannot succeed.
    bool _pid_exhausted{false};
};

} // namespace cluster_link
