#pragma once

#include "raft/client_cache.h"
#include "raft/probe.h"
#include "raft/timeout_jitter.h"
#include "seastarx.h"
#include "storage/log.h"

#include <seastar/core/sharded.hh>

namespace raft {
/// consensus for one raft group
class consensus {
public:
    struct voted_for_configuration {
        model::node_id voted_for;
        // for term it doesn't make sense to use numeric_limits<>::min
        model::term_id term{0};
    };
    enum class vote_state { follower, candidate, leader };
    using vote_request_ptr = foreign_ptr<std::unique_ptr<vote_request>>;
    using vote_reply_ptr = foreign_ptr<std::unique_ptr<vote_reply>>;
    using leader_cb_t = noncopyable_function<void(group_id)>;
    using append_entries_cb_t
      = noncopyable_function<void(std::vector<entry>&&)>;

    consensus(
      model::node_id,
      group_id,
      timeout_jitter,
      storage::log&,
      storage::log_append_config::fsync should_fsync,
      io_priority_class io_priority,
      model::timeout_clock::duration disk_timeout,
      sharded<client_cache>&,
      leader_cb_t);

    /// Initial call. Allow for internal state recovery
    future<> start();

    /// Stop all communications.
    future<> stop();

    future<vote_reply> vote(vote_request&& r) {
        return with_gate(_bg, [this, r = std::move(r)]() mutable {
            return with_semaphore(
              _op_sem, 1, [this, r = std::move(r)]() mutable {
                  return do_vote(std::move(r));
              });
        });
    }
    future<append_entries_reply> append_entries(append_entries_request&& r) {
        return with_gate(_bg, [this, r = std::move(r)]() mutable {
            return with_semaphore(
              _op_sem, 1, [this, r = std::move(r)]() mutable {
                  return do_append_entries(std::move(r));
              });
        });
    }

    void register_hook(append_entries_cb_t fn) {
        if (_append_entries_notification) {
            throw std::runtime_error(
              "Raft entries already has append_entries hook");
        }
        _append_entries_notification = std::move(fn);
    }

    bool is_leader() const {
        return _vstate == vote_state::leader;
    }

    const protocol_metadata& meta() const {
        return _meta;
    }

    const group_configuration& config() const {
        return _conf;
    }
    const model::ntp& ntp() const {
        return _log.ntp();
    }

    clock_type::time_point last_heartbeat() const {
        return _hbeat;
    };

    void process_heartbeat(append_entries_reply&&) {
    }

private:
    // all these private functions assume that we are under exclusive operations
    // via the _op_sem
    void step_down();
    future<vote_reply> do_vote(vote_request&&);
    future<append_entries_reply> do_append_entries(append_entries_request&&);

    future<append_entries_reply> success_case_append_entries(
      protocol_metadata sender, std::vector<storage::log::append_result>);

    future<std::vector<storage::log::append_result>>
    disk_append(std::vector<entry>&&);

    sstring voted_for_filename() const {
        return _log.base_directory() + "/voted_for";
    }

    void dispatch_vote();

    future<> do_dispatch_vote(clock_type::time_point);

    future<std::vector<vote_reply_ptr>>
      send_vote_requests(clock_type::time_point);

    future<> process_vote_replies(std::vector<vote_reply_ptr>);
    future<> replicate_config_as_new_leader();

    /// After we append on disk, we must consume the entries
    /// to update our leader_id, nodes & learners configuration
    future<> process_configurations(std::vector<entry>&&);

    // args
    model::node_id _self;
    timeout_jitter _jit;
    storage::log& _log;
    storage::log_append_config::fsync _should_fsync;
    io_priority_class _io_priority;
    model::timeout_clock::duration _disk_timeout;
    sharded<client_cache>& _clients;
    leader_cb_t _leader_notification;

    // read at `future<> start()`
    model::node_id _voted_for;
    protocol_metadata _meta;
    group_configuration _conf;

    /// useful for when we are not the leader
    clock_type::time_point _hbeat = clock_type::now();
    /// used to keep track if we are a leader, or transitioning
    vote_state _vstate = vote_state::follower;
    /// used for votes only. heartbeats are done by heartbeat_manager
    timer_type _vote_timeout;
    /// used to wait for background ops before shutting down
    gate _bg;

    /// all raft operations must happen exclusively since the common case
    /// is for the operation to touch the disk
    semaphore _op_sem{1};
    /// used for notifying when commits happened to log
    append_entries_cb_t _append_entries_notification;
    probe _probe;
};

} // namespace raft
