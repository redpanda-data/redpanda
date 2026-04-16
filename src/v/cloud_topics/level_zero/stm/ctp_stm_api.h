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
#include "cloud_topics/level_zero/stm/types.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"

#include <seastar/core/gate.hh>

#include <expected>
#include <ostream>

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

std::ostream& operator<<(std::ostream& o, ctp_stm_api_errc errc);

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

    ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
    advance_reconciled_offset(
      kafka::offset last_reconciled_offset,
      model::timeout_clock::time_point deadline,
      ss::abort_source& as);

    ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
    set_start_offset(
      kafka::offset new_start_offset,
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

    /// Fence writes
    ss::future<std::expected<cluster_epoch_fence, stale_cluster_epoch>>
    fence_epoch(cluster_epoch e);

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
    /// Returns the offset at which the batch was applied.
    ss::future<std::expected<model::offset, ctp_stm_api_errc>> replicated_apply(
      model::record_batch&& batch,
      std::optional<model::term_id> expected_term,
      model::timeout_clock::time_point deadline,
      ss::abort_source&);

private:
    ss::shared_ptr<ctp_stm> _stm;
    const prefix_logger& _log;
};

} // namespace cloud_topics
