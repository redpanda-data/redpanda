#pragma once

#include "raft/client_cache.h"
#include "seastarx.h"
#include "storage/log.h"

#include <seastar/core/sharded.hh>

namespace raft {
struct append_entries_proto_hook {
    using entries = std::vector<std::unique_ptr<entry>>;
    virtual ~append_entries_proto_hook() = default;
    virtual void pre_commit(model::offset begin, const entries&) = 0;
    virtual void abort(model::offset begin) = 0;
    virtual void commit(model::offset begin, model::offset committed) = 0;
};

/// consensus for one raft group
class consensus {
public:
    struct voted_for_configuration {
        model::node_id voted_for;
        // for term it doesn't make sense to use numeric_limits<>::min
        model::term_id term = 0;
    };
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

    /// \brief must be initial call.
    /// allow for internal state recovery
    future<> start();

    future<vote_reply> vote(vote_request r) {
        return with_semaphore(_op_sem, 1, [this, r = std::move(r)]() mutable {
            return do_vote(std::move(r));
        });
    }
    future<append_entries_reply> append_entries(append_entries_request r) {
        return with_semaphore(_op_sem, 1, [this, r = std::move(r)]() mutable {
            return do_append_entries(std::move(r));
        });
    }

    void register_hook(append_entries_proto_hook* fn) {
        _hooks.push_back(fn);
    }

    /// \brief currently dispatches it ASAP since each entry is already a
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
    // all these private functions assume that we are under exclusive operations
    // via the _op_sem
    void step_down();
    future<vote_reply> do_vote(vote_request);
    future<append_entries_reply> do_append_entries(append_entries_request);
    future<std::vector<storage::log::append_result>>
      disk_append(std::vector<std::unique_ptr<entry>>);
    sstring voted_for_filename() const {
        return _log.base_directory() + "/voted_for";
    }

    // args
    model::node_id _self;
    protocol_metadata _meta;
    group_configuration _conf;
    storage::log& _log;
    storage::log_append_config::fsync _should_fsync;
    io_priority_class _io_priority;
    model::timeout_clock::duration _disk_timeout;
    sharded<client_cache>& _clients;

    // read at `future<> start()`
    model::node_id _voted_for;
    clock_type::time_point _hbeat = clock_type::now();
    vote_state _vstate = vote_state::follower;
    /// \brief all raft operations must happen exclusively since the common case
    /// is for the operation to touch the disk
    semaphore _op_sem{1};

    std::vector<append_entries_proto_hook*> _hooks;
};

} // namespace raft
