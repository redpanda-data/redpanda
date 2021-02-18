/*
 * Copyright 2020 Vectorized, Inc.
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
#include "storage/snapshot.h"

namespace raft {

class recovery_stm {
public:
    recovery_stm(consensus*, vnode, ss::io_priority_class);
    ss::future<> apply();

private:
    ss::future<> do_recover();
    ss::future<>
      read_range_for_recovery(model::offset, model::offset, model::offset);
    ss::future<> replicate(
      model::record_batch_reader&&, append_entries_request::flush_after_append);
    ss::future<result<append_entries_reply>>
    dispatch_append_entries(append_entries_request&&);
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
    ss::io_priority_class _prio;
    ctx_log _ctxlog;
    // tracking follower snapshot delivery
    std::unique_ptr<storage::snapshot_reader> _snapshot_reader;
    size_t _sent_snapshot_bytes = 0;
    size_t _snapshot_size = 0;
    // needed to early exit. (node down)
    bool _stop_requested = false;
};

} // namespace raft
