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

#include "base/outcome.h"
#include "raft/fwd.h"
#include "raft/recovery_client_protocol.h"
#include "raft/recovery_memory_quota.h"
#include "raft/types.h"
#include "rpc/connection_cache.h"
#include "storage/snapshot.h"
#include "utils/prefix_logger.h"

#include <vector>

namespace raft {

class recovery_stm {
public:
    recovery_stm(
      consensus*,
      vnode,
      recovery_client_protocol&,
      scheduling_config,
      recovery_memory_quota&);
    ss::future<> apply();

private:
    /**
     * Indicates if a snapshot is required for follower to recover, the recovery
     * process will either not require a snapshot, create an on demand snapshot
     * or will use a currently available snapshot.
     */
    enum class required_snapshot_type {
        none,
        current,
        on_demand,
    };
    /**
     * Holder for on demand snapshot output stream. Right now Redpanda uses
     * simple empty snapshots for all data partition STMs hence there is no much
     * overhead in creating a separate snapshot for each follower even if it has
     * the same content.
     *
     * In future it may be worth to cache on demand snapshots if more than one
     * learner is being recovered.
     */
    struct on_demand_snapshot_reader {
        ss::input_stream<char>& input() { return stream; }
        ss::future<> close() { return stream.close(); }

        ss::input_stream<char> stream;
    };
    // variant encapsulating two different reader types
    using snapshot_reader_t
      = std::variant<storage::snapshot_reader, on_demand_snapshot_reader>;
    // Recovery of compacted topics needs special care and attention due to
    // possible races with removed tombstones and transactional
    // control batches. The following functions deal with making this process
    // safe.
    //
    // Returns true iff the log being recovered is from a compacted topic, and
    // if recovery safety checks are not disabled.
    bool needs_recovery_checks() const;

    // Issues an RPC to reset the follower node by invoking
    // `consensus::clear_state()`. This suffix truncates the log and removes any
    // snapshots/persistent state on the follower. Locally, it sets
    // `_stop_requested` to `true` to allow recovery to restart organically per
    // the follower's new state.
    ss::future<> reset_follower(std::string_view ctx);

    // We may need to reset the learner of a compacted topic if
    // the learner is continuing (i.e it has performed a partial recovery
    // from a previous leader, and is now reading another portion of the log
    // from this leader) its recovery below the current log's
    // max_clean_and_removable_offset().
    bool needs_initial_reset();

    // Issues an RPC via `reset_follower()` depending if recovery is un-safe and
    // the follower needs resetting per the result of `needs_initial_reset()`.
    // This check is only performed once, at the beginning of the `recovery_stm`
    // lifecycle.
    ss::future<> maybe_initial_reset_follower();

    // Returns true iff the current time to recover has exceeded the log's
    // configured `delete.retention.ms`. Always returns `false` if recovery
    // checks are disabled.
    bool recovery_time_exceeds_delete_retention_ms();

    // Issues an RPC via `reset_follower()` iff
    // `recovery_time_exceeds_delete_retention_ms()` has returned true. This
    // check is performed on every invocation of `do_recover()` as a check that
    // the current recovery is still safe from divergence as a result of removed
    // state in the current log.
    ss::future<> maybe_reset_follower();

    ss::future<> recover();
    ss::future<> do_recover();
    ss::future<
      std::optional<std::tuple<chunked_vector<model::record_batch>, size_t>>>
    read_range_for_recovery(model::offset, bool, size_t);

    ss::future<> replicate(
      chunked_vector<model::record_batch> batches,
      flush_after_append request_flush_after_append,
      ssx::semaphore_units recovery_memory_units,
      size_t batches_size);

    ss::future<result<append_entries_reply>> dispatch_append_entries(
      append_entries_request&&, std::vector<ssx::semaphore_units>);
    std::optional<follower_index_metadata*> get_follower_meta();
    clock_type::time_point append_entries_timeout();

    ss::future<> install_snapshot(required_snapshot_type);
    ss::future<> send_install_snapshot_request();
    ss::future<> handle_install_snapshot_reply(result<install_snapshot_reply>);
    ss::future<> open_current_snapshot();
    ss::future<> take_on_demand_snapshot(model::offset);
    ss::future<iobuf> read_snapshot_chunk();
    ss::future<> close_snapshot_reader();
    required_snapshot_type get_required_snapshot_type(
      const follower_index_metadata& follower_metadata) const;
    bool is_recovery_finished();
    flush_after_append should_flush(model::offset) const;
    bool is_snapshot_at_offset_supported() const;
    consensus* _ptr;
    vnode _node_id;
    model::offset _base_batch_offset;
    model::offset _last_batch_offset;
    model::offset _committed_offset;
    /**
     * Now the snapshot delivered to the follower may have offset different than
     * the one indicated by `consensus::last_snapshot_index()`, we need to cache
     * it in recovery_stm to correctly update follower state after the snapshot
     * is delivered.
     */
    model::offset _inflight_snapshot_last_included_index;
    model::term_id _term;
    recovery_client_protocol& _recovery_rpc;
    scheduling_config _scheduling;
    prefix_logger _ctxlog;

    std::unique_ptr<snapshot_reader_t> _snapshot_reader;
    size_t _sent_snapshot_bytes = 0;
    size_t _snapshot_size = 0;
    // needed to early exit. (node down)
    bool _stop_requested = false;
    recovery_memory_quota& _memory_quota;
    size_t _recovered_bytes_since_flush = 0;
};

} // namespace raft
