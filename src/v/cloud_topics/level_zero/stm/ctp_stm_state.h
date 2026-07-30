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

#include "base/format_to.h"
#include "cloud_topics/level_zero/stm/size_estimator.h"
#include "cloud_topics/level_zero/stm/types.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <expected>
#include <variant>

namespace cloud_topics {

/// In-memory state of the cloud-topics state machine (ctp_stm).
///
class ctp_stm_state
  : public serde::
      envelope<ctp_stm_state, serde::version<1>, serde::compat_version<0>> {
    friend class ctp_stm_state_accessor;

public:
    ctp_stm_state() = default;

    /// Advance the applied max epoch.
    ///
    /// The update is idempotent. The method is invoked by the ctp_stm
    /// when the new placeholder batch is applied to the state.
    ///
    /// \note The advance_* methods are idempotent so the ctp_stm can
    /// start from the recent state and apply older log entries to it.
    /// This is needed to support the raft snapshotting mechanism. Raft
    /// requires taking snapshot at particular offset. The problem is that
    /// in order to provide this guarantee we need to be able to take the
    /// snapshot at any offset. In order to be able to do that we need the
    /// ctp_stm_state to be versioned. This will complicate the design and
    /// increase memory requirements. The alternative is to avoid versioning and
    /// rely on the idempotency. The snapshot can be based on any state with the
    /// insync_offset greater or equal to the snapshot offset. All log records
    /// can be applied to the state again without changing the state because of
    /// the idempotency.
    void advance_epoch(cluster_epoch epoch, model::offset offset);

    /// This is invoked in the write path before the batch with new
    /// epoch value is even replicated.
    void
    advance_max_seen_epoch(model::term_id term, cluster_epoch epoch) noexcept;

    /// Admit an epoch-carrying batch for submission to raft.
    ///
    /// The seen window doubles as the admission window: the floor
    /// (_previous_seen_epoch) is raised by fence-time bumps, by interior
    /// admissions and, until the first epoch batch applies, by every
    /// admission (whichever admitted epoch lands first collapses the log
    /// epoch window to [e, e]). Everything below the floor is rejected:
    /// two distinct epochs above `e` may land ahead of it in rising order
    /// and move the log epoch window past it, and applying `e` would then
    /// fire the epoch_window_checker vassert on every replica. The floor is
    /// the runner-up rather than the max because stragglers at the previous
    /// epoch are routine when shards observe the cluster epoch at different
    /// times, and a single epoch above `e` cannot move the log window past
    /// it. On a term change the window reseeds from the applied epochs, so
    /// a floor learned from submissions that never landed does not outlive
    /// its term.
    ///
    /// The caller (ctp_stm) must serialize calls with the raft enqueue of
    /// the admitted batch so that admission order equals log order.
    std::expected<std::monostate, stale_cluster_epoch>
    admit_epoch_enqueue(model::term_id term, cluster_epoch epoch) noexcept;

    // Set the new start offset for the partition.
    //
    // This method is idempotent in that it does never cause the start offset to
    // regress.
    void set_start_offset(kafka::offset new_offset) noexcept;

    // Get the start offset for this partition.
    kafka::offset start_offset() const noexcept;

    /// Find the maximum cluster epoch registered in the state.
    std::optional<cluster_epoch> get_max_applied_epoch() const noexcept;

    /// Return the previous epoch value.
    /// The request can be replicated only if its epoch is greater or
    /// equal to epoch returned by this method. If the method returns
    /// nullopt then the epoch wasn't advanced yet so there is no
    /// previous epoch.
    /// Note that this value is not necessary equal to estimate_min_epoch.
    std::optional<cluster_epoch> get_previous_applied_epoch() const noexcept;

    /// Return the max_seen_epoch epoch.
    ///
    /// The max_seen_epoch epoch is the maximum epoch that is expected to be
    /// registered in the state. It might be larger than the maximum epoch
    /// if the placeholder that contains this epoch is being replicated or
    /// not yet applied to the in-memory state.
    ///
    /// Once the max_seen_epoch epoch advances it's guaranteed that the cluster
    /// epoch is at least equal to the max_seen_epoch epoch.
    ///
    /// \return max_seen_epoch epoch.
    std::optional<cluster_epoch>
    get_max_seen_epoch(model::term_id term) const noexcept;

    /// Return the previous_seen_epoch epoch.
    std::optional<cluster_epoch>
      get_previous_seen_epoch(model::term_id) const noexcept;

    /// Estimate the minimum epoch referenced by this ctp_stm.
    /// \note This value might be stale.
    std::optional<cluster_epoch> estimate_min_epoch() const noexcept;

    /// Log offset of the current max_applied_epoch
    std::optional<model::offset> current_epoch_window_offset() const noexcept;

    /// Return true if the epoch can be replicated
    bool
    epoch_in_window(model::term_id term, cluster_epoch epoch) const noexcept;
    /// Return true if the epoch is above the current window
    bool
    epoch_above_window(model::term_id term, cluster_epoch epoch) const noexcept;

    /// Estimate inactive epoch
    std::optional<cluster_epoch> estimate_inactive_epoch() const noexcept;

    /// Record a placeholder's size contribution for the size estimator.
    void record_placeholder_size(model::offset offset, uint64_t size_bytes);

    /// Estimate the total bytes of cloud data addressable by this partition's
    /// level-zero log.
    ///
    /// Uses the LRO as the low watermark: data at or below the LRO has been
    /// reconciled to L1 and is no longer logically part of L0.
    uint64_t estimated_data_size() const noexcept;

    /// Access the size estimator directly (for testing and metrics).
    const size_estimator& get_size_estimator() const noexcept;

    /// Advance LRO and it's translated log offset counterpart.
    void advance_last_reconciled_offset(
      kafka::offset new_last_reconciled_offset,
      model::offset new_last_reconciled_log_offset) noexcept;

    /// Get last reconciled offset value
    std::optional<kafka::offset> get_last_reconciled_offset() const noexcept;
    std::optional<model::offset>
    get_last_reconciled_log_offset() const noexcept;

    /// Advance the min allowed local threshold floor.
    ///
    /// The floor is a kafka-offset lower bound advanced by L1 compaction: data
    /// below it has been compacted in L1 and is non-authoritative locally, so a
    /// separate path is free to prefix-truncate the raft log up to it. The
    /// floor is monotonic non-decreasing; values that do not advance it are
    /// ignored. The unset floor is kafka::offset::min().
    void set_min_allowed_local_threshold(kafka::offset) noexcept;

    /// Get the min allowed local threshold floor. Returns kafka::offset::min()
    /// when the floor is unset.
    kafka::offset get_min_allowed_local_threshold() const noexcept;

    auto serde_fields() {
        return std::tie(
          _max_applied_epoch,
          _last_reconciled_offset,
          _last_reconciled_log_offset,
          _current_epoch_window_offset,
          _min_epoch_lower_bound,
          _previous_applied_epoch,
          _start_offset,
          _size_estimator,
          _min_allowed_local_threshold);
    }

    /// Max collectible offset is defined by the LRO.
    ///
    /// The LRO is propagated using the advance_last_reconciled_offset
    /// method. It accepts the new LRO and its translated log offset.
    /// This translated log offset is used to determine the maximum
    /// collectible offset.
    ///
    /// Before the LRO is set the maximum collectible offset is
    /// set to min to prevent local log truncation.
    ///
    /// \return Max collectible offset.
    model::offset get_max_collectible_offset() const noexcept;

    fmt::iterator format_to(fmt::iterator) const;

private:
    /// The term at which the *_seen_epochs are for, due to the sliding window
    /// having the ability to diverge, we only track it within a single term,
    /// then reset the window to avoid nasty edge cases when leadership changes.
    model::term_id _seen_window_term;
    /// The max epoch after the current in flight requests are applied.
    ///
    /// This is required because of the pipelining of requests in the STM.
    /// If present, we don't allow any replicated requests to have an epoch
    /// that is lower than this value.
    std::optional<cluster_epoch> _max_seen_epoch;

    /// The previous epoch after the current in flight requests are applied.
    /// Requests with epochs below this value are fenced and not allowed to be
    /// applied to the STM.
    ///
    /// Doubles as the enqueue admission floor (see admit_epoch_enqueue):
    /// besides fence-time bumps it is raised by admitted interior epochs and,
    /// until the first epoch batch applies, by every admitted epoch.
    ///
    /// Not persisted with the snapshot because it reflects the state of
    /// in-flight requests.
    std::optional<cluster_epoch> _previous_seen_epoch;

    /// The maximum epoch of applied batches to the STM.
    ///
    /// We enforce no epochs applied or replicated are less than this value.
    std::optional<cluster_epoch> _max_applied_epoch;

    /// The previous max epoch applied to the STM state.
    /// Invariant:
    /// - _previous_epoch < _max_applied_epoch
    /// - _previous_epoch <= _previous_seen_epoch
    std::optional<cluster_epoch> _previous_applied_epoch;

    /// The offset at which the current applied epoch window was transitioned
    /// to.
    std::optional<model::offset> _current_epoch_window_offset;

    /// The epoch which is less or equal to the current epoch window referenced
    /// by the first record batch after the last reconciled offset.
    /// This epoch is not guaranteed to be "active" (from the point of
    /// view of the partition) but it's guaranteed that all epochs before
    /// this epoch are "inactive".
    std::optional<cluster_epoch> _min_epoch_lower_bound;

    /// The last offset that was uploaded to L1. This value may lag behind
    /// the value stored in the L1 metastore, but should never be ahead of
    /// what is stored in L1. Also known as LRO.
    std::optional<kafka::offset> _last_reconciled_offset;

    /// The LRO translated to the log offset.
    ///
    /// This is used to lookup the epoch that was last reconciled for
    /// L0 GC.
    std::optional<model::offset> _last_reconciled_log_offset;

    // The start offset for the partition (inclusive).
    //
    // This is the source of truth over what is in the local raft log, and is
    // inclusive of what is L1 as well. Any changes to start offset should go
    // through here, then be reconciled to L1.
    kafka::offset _start_offset = kafka::offset{0};

    // Estimates total cloud data bytes addressable by the surviving log.
    size_estimator _size_estimator;

    /// Min allowed local threshold floor advanced by L1 compaction.
    /// kafka::offset::min() means unset (no floor). Truncation is applied
    /// elsewhere.
    kafka::offset _min_allowed_local_threshold = kafka::offset::min();
};

}; // namespace cloud_topics
