/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/persisted_stm.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "outcome.h"
#include "raft/fwd.h"
#include "seastarx.h"
#include "storage/types.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/queue.hh>
#include <seastar/util/log.hh>

namespace cluster {

class consensus;

/**
 * Responsible for taking snapshots triggered by underlying log segments
 * eviction.
 *
 * The process goes like this: storage layer will send a "deletion notification"
 * - a request to evict log up to a certain offset. log_eviction_stm will then
 * adjust that offset with _stm_manager->max_collectible_offset(), write the
 * raft snapshot and notify the storage layer that log eviction can safely
 * proceed up to the adjusted offset.
 *
 * This class also initiates and responds to delete-records events. Call
 * truncate() pushes a special prefix_truncate batch onto the log for which this
 * stm will be searching for. Upon processing of this record a new snapshot will
 * be written which may also trigger deletion of data on disk.
 */
class log_eviction_stm final
  : public persisted_stm<kvstore_backed_stm_snapshot> {
public:
    using offset_result = result<model::offset, std::error_code>;
    log_eviction_stm(
      raft::consensus*, ss::logger&, ss::abort_source&, storage::kvstore&);

    ss::future<> start() override;

    ss::future<> stop() override;

    /// Truncate local log
    ///
    /// This method doesn't immediately delete the entries of the log below the
    /// requested offset but pushes a special record batch onto the log which
    /// when read by brokers, will invoke a routine that will perform the
    /// deletion
    ///
    /// Returns the offset at which the operation was replicated.
    ss::future<offset_result> truncate(
      model::offset rp_start_offset,
      kafka::offset kafka_start_offset,
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt);

    /// Return the offset up to which the storage layer would like to
    /// prefix truncate the log, if any.
    std::optional<model::offset> eviction_requested_offset() const {
        const auto requested_eviction_offset = std::max(
          _delete_records_eviction_offset, _storage_eviction_offset);
        if (!requested_eviction_offset) {
            return std::nullopt;
        }
        return requested_eviction_offset;
    }

    /// This class drives eviction, it works differently then other stms in this
    /// regard
    ///
    /// Override to ensure it never unnecessarily waits
    ss::future<> ensure_snapshot_exists(model::offset) override;

    /// The actual start offset of the log with the delta factored in
    model::offset effective_start_offset() const;

    /// Ensure followers have processed up until the most recent known version
    /// of the batch representing the start offset
    /// This only returns the start override, if one exists. It does not take
    /// into account local storage, and may not even point to an offset that
    /// exists in local storage (e.g. if we have locally truncated).
    ss::future<offset_result>
    sync_start_offset_override(model::timeout_clock::duration timeout);

    model::offset start_offset_override() const {
        if (_delete_records_eviction_offset == model::offset{}) {
            return model::offset{};
        }
        return model::next_offset(_delete_records_eviction_offset);
    }

protected:
    ss::future<> apply_snapshot(stm_snapshot_header, iobuf&&) override;

    ss::future<stm_snapshot> take_snapshot() override;

private:
    void increment_start_offset(model::offset);
    bool should_process_evict(model::offset);

    ss::future<> monitor_log_eviction();
    ss::future<> do_write_raft_snapshot(model::offset);
    ss::future<> write_raft_snapshots_in_background();
    ss::future<> apply(model::record_batch) override;
    ss::future<> handle_raft_snapshot() override;

    ss::future<offset_result> replicate_command(
      model::record_batch batch,
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> as);

private:
    using retry_event_until_success = ss::bool_class<struct wait_for_tag>;
    /**
     * Eviction event represents an eviction request coming either from storage
     * offset monitor notification or the delete records special batch.
     *
     * The event stores the trucation point offset and information if truncation
     * related should be retried. It may be the case that the truncation
     * may not yet be executed as max_collectible offsets of the partition
     * related STMs hasn't advanced yet.
     */
    struct eviction_event {
        model::offset prefix_truncate_offset;
        retry_event_until_success wait_for_success;
    };

    ss::future<>
      enqueue_eviction_event(model::offset, retry_event_until_success);

    void maybe_pop_queue();

    ss::abort_source& _as;
    model::offset _storage_eviction_offset;
    model::offset _delete_records_eviction_offset;
    /**
     * Since the ss::queue is a SPSC type of queue we use additional mutex to
     * hold the two producers i.e. apply fiber and log eviction monitoring fiber
     */
    static constexpr size_t max_event_queue_size = 4;
    mutex _queue_mutex;
    ss::queue<eviction_event> _queue;
};

} // namespace cluster
