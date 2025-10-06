/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_zero/stm/ctp_stm_state.h"
#include "cloud_topics/level_zero/stm/types.h"
#include "raft/persisted_stm.h"

#include <seastar/core/rwlock.hh>

namespace cloud_topics {

class ctp_stm_api;

/// The STM that tracks current cluster epoch and LRO.
/// The goal is to guarantee that the cluster epoch is monotonic and
/// to provide the smallest cluster epoch available through the
/// underlying partition.
///
/// In order to provide this information the STM applies every L0
/// metadata batch to its in-memory state.
class ctp_stm final : public raft::persisted_stm<> {
    friend class ctp_stm_api;

public:
    static constexpr const char* name = "ctp_stm";

    ss::future<> start() override;
    ss::future<> stop() override;

    ctp_stm(ss::logger&, raft::consensus*);

    const model::ntp& ntp() const noexcept;

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        return raft::stm_initial_recovery_policy::read_everything;
    }

    const ctp_stm_state& state() const noexcept { return _state; }

    void advance_max_seen_epoch(cluster_epoch epoch) {
        _state.advance_max_seen_epoch(epoch);
    }

    ss::future<cluster_epoch_fence> fence_epoch(cluster_epoch e);

    /// Return inactive epoch of the CTP
    ///
    /// The inactive epoch is any epoch which is no longer referenced
    /// by the CTP and will not be referenced in the future. There could be
    /// multiple inactive epochs at any given moment. This method returns the
    /// largest one `max(∀ epoch ∈ inactive_epoch)`.
    /// The nullopt result indicates that no data was produced to the CTP yet.
    ///
    /// The method is not syncing with the STM (the STM state might be stale
    /// compared to the content of the log) but even if this is the case it
    /// is safe to use it. It will return stale epoch in this case but this
    /// alone can't cause data loss.
    ss::future<std::optional<cluster_epoch>> get_inactive_epoch();

    /// Return inactive epoch of the CTP
    std::optional<cluster_epoch> estimate_inactive_epoch() const noexcept;

    /// Sync with the STM
    ///
    /// \brief The method is syncing the STM  to minimize races.
    /// \return 'true' if the replica is a leader and the in-memory state of
    /// the STM is up-to-date. Otherwise, return 'false'.
    ss::future<bool> sync_in_term(
      model::timeout_clock::time_point deadline, ss::abort_source& as);

private:
    ss::future<> do_apply(const model::record_batch&) override;
    void apply_placeholder(const model::record_batch&);
    void apply_advance_reconciled_offset(model::record);
    void apply_set_start_offset(model::record);

    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;

    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units u) override;

    ss::future<> apply_raft_snapshot(const iobuf&) override;
    ss::future<iobuf> take_raft_snapshot(model::offset) override;
    model::offset max_removable_local_log_offset() override;

    // A function invoked in a background loop that attempts to truncate the log
    // below the current start offset.
    ss::future<> prefix_truncate_below_lro();
    ss::future<> do_write_raft_snapshot(model::offset truncation_point);

private:
    /// Lock to protect the state from concurrent access.
    /// When the new epoch is applied we need to acquire a write lock.
    /// Otherwise, we need to acquire a read lock.
    ss::rwlock _lock;
    /// Current in-memory state of the STM
    ctp_stm_state _state;

    // The last observed epoch to be applied to the state machine. This value is
    // used to check for violations of monotonicity in epoch order.
    cluster_epoch _last_seen_epoch{};

    // An abort source to stop the prefix truncation loop on stop.
    ss::condition_variable _lro_advanced;
    ss::abort_source _as;

    // The last point that we truncated to, so we can skip writing a raft
    // snapshot if needed. This is volatile state (which is fine).
    model::offset _last_truncation_point;
};

} // namespace cloud_topics
