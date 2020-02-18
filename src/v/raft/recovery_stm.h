#pragma once

#include "model/metadata.h"
#include "raft/consensus.h"

namespace raft {

class recovery_stm {
public:
    recovery_stm(consensus*, model::node_id, ss::io_priority_class);

    ss::future<> apply();

private:
    ss::future<> do_one_read();
    ss::future<> replicate(model::record_batch_reader&&);
    ss::future<result<append_entries_reply>>
    dispatch_append_entries(append_entries_request&&);
    std::optional<follower_index_metadata*> get_follower_meta();

    bool is_recovery_finished();

    consensus* _ptr;
    model::node_id _node_id;
    model::offset _base_batch_offset;
    model::offset _last_batch_offset;
    ss::io_priority_class _prio;
    raft_ctx_log _ctxlog;
    // needed to early exit. (node down)
    bool _stop_requested = false;
};

} // namespace raft
