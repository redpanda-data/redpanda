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
#include "cloud_topics/level_zero/common/producer_queue.h"
#include "cloud_topics/level_zero/stm/types.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "ssx/mutex.h"

#include <seastar/core/gate.hh>

#include <expected>

struct ctp_stm_api_accessor;
class prefix_logger;

namespace cloud_topics {

class ctp_stm;

enum class ctp_stm_api_errc : uint8_t {
    timeout,
    not_leader,
    shutdown,
    failure,
};

inline fmt::iterator format_to(ctp_stm_api_errc errc, fmt::iterator out) {
    switch (errc) {
    case ctp_stm_api_errc::timeout:
        return fmt::format_to(out, "timeout");
    case ctp_stm_api_errc::not_leader:
        return fmt::format_to(out, "not_leader");
    case ctp_stm_api_errc::shutdown:
        return fmt::format_to(out, "shutdown");
    case ctp_stm_api_errc::failure:
        return fmt::format_to(out, "failure");
    }
}

class ctp_stm_api {
    friend struct ::ctp_stm_api_accessor;

public:
    explicit ctp_stm_api(ss::shared_ptr<ctp_stm> stm);
    ctp_stm_api(const ctp_stm_api&) noexcept = delete;
    ctp_stm_api& operator=(const ctp_stm_api&) noexcept = delete;
    ctp_stm_api(ctp_stm_api&&) noexcept = delete;
    ctp_stm_api& operator=(ctp_stm_api&&) noexcept = delete;
    ~ctp_stm_api() noexcept = default;

public:
    /// Get the last reconciled offset from the ctp_stm state.
    kafka::offset get_last_reconciled_offset() const;

    /// Get the last reconciled log offset from the ctp_stm state
    model::offset get_last_reconciled_log_offset() const;

    /// Get threshold offset for local readers
    kafka::offset get_min_allowed_local_threshold() const;

    /// Replicate an advance_reconciled_offset_cmd, optionally combined with
    /// a set_min_allowed_local_threshold_cmd in the same record batch so
    /// both advance atomically. The min_allowed_local_threshold is used by
    /// the reconciler to move the local-read floor over placeholder-backed
    /// ranges it has reconciled into L1: once the floor passes them, reads
    /// are served from L1 and the placeholders' L0 objects may be safely
    /// garbage-collected. Values that do not advance the current floor are
    /// ignored.
    ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
    advance_reconciled_offset(
      kafka::offset last_reconciled_offset,
      model::timeout_clock::time_point deadline,
      ss::abort_source& as,
      std::optional<kafka::offset> min_allowed_local_threshold = std::nullopt);

    ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
    set_start_offset(
      kafka::offset new_start_offset,
      model::timeout_clock::time_point deadline,
      ss::abort_source& as);

    /// Replicate a set_min_allowed_local_threshold_cmd to advance the
    /// compaction floor consumed by the prefix-truncate loop (the tiered_cloud
    /// retention path). The floor is monotonic; replication is skipped when
    /// `value` does not advance it.
    ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
    set_min_allowed_local_threshold(
      kafka::offset value,
      model::timeout_clock::time_point deadline,
      ss::abort_source& as);

    /// Fence and replicate an advance_epoch_cmd if new_epoch > max_epoch.
    ss::future<std::expected<std::monostate, ctp_stm_api_errc>> advance_epoch(
      cluster_epoch new_epoch,
      model::timeout_clock::time_point deadline,
      ss::abort_source& as);

    /// Advance LRLO past any advance_epoch batches between current LRO and the
    /// next placeholder, allowing min_epoch_lower_bound to update.
    ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
    sync_to_next_placeholder(
      model::timeout_clock::time_point deadline, ss::abort_source& as);

    kafka::offset get_start_offset() const;

    /// Return the inactive epoch which is no longer referenced by this ctp_stm.
    /// This method is guaranteed to return precise value but it creates
    // a reader and scans the log for the minimum epoch.
    /// \note This method could return std::nullopt if the partition is empty
    ss::future<std::expected<std::optional<cluster_epoch>, ctp_stm_api_errc>>
    get_inactive_epoch() const;

    /// Return the inactive epoch which is no longer referenced by this ctp_stm.
    /// This method can return stale value but is guaranteed to eventually
    /// make forward progress.
    std::optional<cluster_epoch> estimate_inactive_epoch() const noexcept;

    /// Return the log offset at which the current max applied epoch was set.
    std::optional<model::offset> get_epoch_window_offset() const noexcept;

    /// Sync STM state with the log.
    ///
    /// Normal STM sync call only guaranteed that the in-memory state is
    /// consistent with the log messages replicated in previous terms.
    /// This method is used to ensure that the in-memory state is consistent
    /// with the log messages replicated in the current term.
    /// \return 'true' if the replica is a leader and the in-memory state of
    /// the STM is up-to-date. Otherwise, return 'false'.
    ss::future<bool>
    sync_in_term(model::timeout_clock::time_point deadline, ss::abort_source&);

    /// Fence writes. The `timeout` bounds the underlying ctp_stm raft sync;
    /// callers that have their own deadline (e.g. the bulk-replicate paths in
    /// the frontend) should pass it through so fence_epoch shares the same
    /// budget instead of using the stm's default.
    ss::future<std::expected<cluster_epoch_fence, stale_cluster_epoch>>
    fence_epoch(
      cluster_epoch e,
      model::timeout_clock::duration timeout = default_fence_epoch_timeout);

    /// Default deadline used by `fence_epoch` when callers do not supply one.
    static constexpr auto default_fence_epoch_timeout = std::chrono::seconds(
      10);

    /// Admit an epoch-carrying batch for replication through the enqueue
    /// gate (see ctp_stm::admit_epoch_enqueue). `term` must come from a
    /// fence acquired in the current term. The returned units must be held
    /// until the raft request_enqueued stage of the batch resolves.
    ss::future<std::expected<ssx::mutex::units, stale_cluster_epoch>>
    admit_epoch_enqueue(model::term_id term, cluster_epoch epoch);

    std::optional<cluster_epoch> get_max_epoch() const;

    std::optional<cluster_epoch> get_max_seen_epoch(model::term_id) const;

    l0::producer_queue& producer_queue();

    // Register this reader with the STM so that it's state isn't GC'd.
    //
    // The provided pointer must be kept *stable* during it's entire lifetime.
    void register_reader(active_reader_state*);

    /// Estimate the total bytes of cloud data addressable by the level-zero
    /// log for this partition.
    uint64_t estimated_data_size() const noexcept;

private:
    /// Replicate a record batch and wait for it to be applied to the ctp_stm.
    /// Returns the offset at which the batch was applied. If `gate_units`
    /// carries an enqueue-gate permit it is released once raft fixed the
    /// batch's position in the log.
    ss::future<std::expected<model::offset, ctp_stm_api_errc>> replicated_apply(
      model::record_batch&& batch,
      std::optional<model::term_id> expected_term,
      model::timeout_clock::time_point deadline,
      ss::abort_source&,
      ssx::mutex::units gate_units = {});

private:
    ss::shared_ptr<ctp_stm> _stm;
    const prefix_logger& _log;
};

} // namespace cloud_topics
