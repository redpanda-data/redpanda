#pragma once

#include "model/metadata.h"
#include "raft/consensus.h"

namespace raft {

class recovery_stm {
public:
    recovery_stm(consensus*, model::node_id, ss::io_priority_class);
    ss::future<> apply();

private:
    ss::future<> do_recover();
    ss::future<> read_range_for_recovery(model::offset, model::offset);
    ss::future<> replicate(model::record_batch_reader&&);
    ss::future<result<append_entries_reply>>
    dispatch_append_entries(append_entries_request&&);
    std::optional<follower_index_metadata*> get_follower_meta();
    clock_type::time_point append_entries_timeout();

    ss::future<> install_snapshot();
    ss::future<> send_install_snapshot_request();
    ss::future<> handle_install_snapshot_reply(result<install_snapshot_reply>);
    ss::future<> open_snapshot_reader();
    ss::future<> close_snapshot_reader();

    bool is_recovery_finished();

    consensus* _ptr;
    model::node_id _node_id;
    model::offset _base_batch_offset;
    model::offset _last_batch_offset;
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
