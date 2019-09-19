#pragma once

#include "raft/client_cache.h"
#include "seastarx.h"
#include "storage/log.h"
#include "storage/log_manager.h"

#include <seastar/core/sharded.hh>

namespace raft {
/// consensus for one raft group
class consensus {
public:
    enum class vote_state { follower, candidate, leader };
    consensus(
      model::node_id,
      protocol_metadata,
      group_configuration,
      storage::log&,
      storage::log_append_config::fsync should_fsync,
      io_priority_class io_priority,
      model::timeout_clock::duration disk_timeout,
      sharded<client_cache>&);

    future<vote_reply> vote(vote_request);

    future<append_entries_reply> append_entries(append_entries_request);

    /// \brief currently dispatches it ASAP since each entry is readly a
    /// record_batch; In the future we can batch a little more
    /// before sending down the wire as an optimization
    future<> replicate(std::unique_ptr<entry>);

    bool is_leader() const {
        return _voted_for == _self;
    }

    const protocol_metadata& meta() const {
        return _meta;
    }

    const group_configuration& config() const {
        return _conf;
    }

private:
    model::node_id _self;
    protocol_metadata _meta;
    group_configuration _conf;
    storage::log& _log;
    storage::log_append_config::fsync _should_fsync;
    io_priority_class _io_priority;
    model::timeout_clock::duration _disk_timeout;
    sharded<client_cache>& _clients;

    // FIXME(agallego) - needs to integrate with log replay
    model::node_id _voted_for;

    clock_type::time_point _hbeat = clock_type::now();
    vote_state _vstate = vote_state::follower;
    /// \brief all raft operations must happen exclusively since the common case
    /// is for the operation to touch the disk
    semaphore _op_sem{1};
};

} // namespace raft
