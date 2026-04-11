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

#include "cloud_topics/level_zero/common/producer_queue.h"
#include "cloud_topics/level_zero/stm/ctp_stm_state.h"
#include "cloud_topics/level_zero/stm/types.h"
#include "raft/persisted_stm.h"
#include "ssx/mutex.h"

#include <seastar/core/semaphore.hh>

#include <expected>

namespace cloud_topics {

class ctp_stm_api;
struct ctp_stm_accessor;

// Log sliding window state, we use this to assert that the log contents
// never break the invariants that the epoch fencing should enforce.
struct epoch_window_checker
  : serde::envelope<
      epoch_window_checker,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(_min_epoch, _max_epoch, _latest_offset);
    }

    // Assert if the epoch at this offset in the log is valid or not.
    void
    check_epoch(const model::ntp&, cluster_epoch epoch, model::offset offset);

    fmt::iterator format_to(fmt::iterator it) const;

    cluster_epoch _min_epoch;
    cluster_epoch _max_epoch;
    model::offset _latest_offset;
};

/// The STM that tracks current cluster epoch and LRO.
/// The goal is to guarantee that the cluster epoch is monotonic and
/// to provide the smallest cluster epoch available through the
/// underlying partition.
///
/// In order to provide this information the STM applies every L0
/// metadata batch to its in-memory state.
class ctp_stm final : public raft::persisted_stm<> {
    friend class ctp_stm_api;
    friend struct ctp_stm_accessor; // for tests

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

    void advance_max_seen_epoch(model::term_id term, cluster_epoch epoch) {
        _state.advance_max_seen_epoch(term, epoch);
    }

    ss::future<std::expected<cluster_epoch_fence, stale_cluster_epoch>>
    fence_epoch(cluster_epoch e);

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

    // The producer queue for this CTP.
    //
    // This is used to preserve ordering of concurrently uploading requests.
    l0::producer_queue& producer_queue();

    // Register this reader with the STM.
    //
    // By registering this reader, it's ensured that we don't GC any of the
    // data at the time this method is called. The state here will be filled in
    // by the STM for bookkeeping. The pointer to `state` must be kept stable
    // for it's entire lifetime.
    void register_reader(active_reader_state* state);

private:
    ss::future<> do_apply(const model::record_batch&) override;
    void apply_placeholder(const model::record_batch&);
    void apply_advance_reconciled_offset(model::record);
    void apply_set_start_offset(model::record);
    void apply_advance_epoch(model::record, model::offset base_offset);
    void apply_reset_state(model::record);

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

private:
    l0::producer_queue _producer_queue;
    intrusive_list<active_reader_state, &active_reader_state::hook>
      _active_readers;
    /// Lock to protect the state from concurrent access.
    /// When the new epoch is applied we need to acquire a write lock.
    /// Otherwise, we need to acquire a read lock.
    ss::semaphore _lock;
    // We only need one updater for the state's epoch at a time - the updater
    // still needs to obtain write lock units from `_lock`, but we can prevent
    // pessimizing with multiple waiters on write lock units by having a
    // separate lock for this purpose.
    ssx::mutex _epoch_update_lock{"ctp_stm::epoch_update_lock"};
    // Used to signal to waiters that the state's epoch has potentially been
    // updated by the holder of `_epoch_update_lock` (it is possible that the
    // lock holder may fail to update the epoch).
    ss::condition_variable _epoch_updated_cv;

    /// Current in-memory state of the STM
    ctp_stm_state _state;

    // The checker of the STM state.
    //
    // this is seperate from the state object itself because the assertion
    // is about the content of the log rather than the computed state. The state
    // is purely idempotent in terms of operations applied.
    epoch_window_checker _epoch_checker;

    // An abort source to stop the prefix truncation loop on stop.
    ss::condition_variable _lro_advanced;
    ss::abort_source _as;

    // The last point that we truncated to, so we can skip writing a raft
    // snapshot if needed. This is volatile state (which is fine).
    model::offset _last_truncation_point;
};

/// Offsets used to seed the re-created ctp_stm_state
struct ctp_stm_seed_offsets {
    kafka::offset start_offset;
    kafka::offset next_offset;
};

/// Create a bootstrap snapshot for ctp_stm with the given state.
/// This is used when bootstrapping a partition with a custom start offset
/// during cluster recovery. The caller is responsible for seeding the
/// initial state correctly.
///
/// \param work_directory The partition's work directory
/// \param offsets The initial STM state
/// \return A future that completes when the snapshot is persisted
ss::future<> create_ctp_stm_bootstrap_snapshot(
  const std::filesystem::path& work_directory, ctp_stm_seed_offsets offsets);

} // namespace cloud_topics
