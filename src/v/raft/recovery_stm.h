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
#include "raft/recovery_memory_quota.h"
#include "raft/types.h"
#include "storage/snapshot.h"
#include "utils/prefix_logger.h"

#include <vector>

namespace raft {

class recovery_stm {
public:
    recovery_stm(consensus*, vnode, scheduling_config, recovery_memory_quota&);
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
    ss::future<> recover();
    ss::future<> do_recover(ss::io_priority_class);
    ss::future<std::optional<model::record_batch_reader>>
    read_range_for_recovery(model::offset, ss::io_priority_class, bool, size_t);

    ss::future<> replicate(
      model::record_batch_reader&&, flush_after_append, ssx::semaphore_units);
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
