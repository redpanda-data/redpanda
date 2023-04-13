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

#include "model/metadata.h"
#include "outcome.h"
#include "raft/logger.h"
#include "raft/recovery_memory_quota.h"
#include "storage/snapshot.h"
#include "utils/prefix_logger.h"

#include <vector>

namespace raft {

class recovery_stm {
public:
    recovery_stm(consensus*, vnode, scheduling_config, recovery_memory_quota&);
    ss::future<> apply();

private:
    ss::future<> recover();
    ss::future<> do_recover(ss::io_priority_class);
    ss::future<std::optional<model::record_batch_reader>>
    read_range_for_recovery(model::offset, ss::io_priority_class, bool, size_t);

    ss::future<> replicate(
      model::record_batch_reader&&,
      append_entries_request::flush_after_append,
      ssx::semaphore_units);
    ss::future<result<append_entries_reply>> dispatch_append_entries(
      append_entries_request&&, std::vector<ssx::semaphore_units>);
    std::optional<follower_index_metadata*> get_follower_meta();
    clock_type::time_point append_entries_timeout();

    ss::future<> install_snapshot();
    ss::future<> send_install_snapshot_request();
    ss::future<> handle_install_snapshot_reply(result<install_snapshot_reply>);
    ss::future<> open_snapshot_reader();
    ss::future<> close_snapshot_reader();
    bool state_changed();
    bool is_recovery_finished();
    append_entries_request::flush_after_append
      should_flush(model::offset) const;
    consensus* _ptr;
    vnode _node_id;
    model::offset _base_batch_offset;
    model::offset _last_batch_offset;
    model::offset _committed_offset;
    model::term_id _term;
    scheduling_config _scheduling;
    prefix_logger _ctxlog;
    // tracking follower snapshot delivery
    std::unique_ptr<storage::snapshot_reader> _snapshot_reader;
    size_t _sent_snapshot_bytes = 0;
    size_t _snapshot_size = 0;
    // needed to early exit. (node down)
    bool _stop_requested = false;
    recovery_memory_quota& _memory_quota;
    size_t _recovered_bytes_since_flush = 0;
};

} // namespace raft
