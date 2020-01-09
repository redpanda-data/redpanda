#pragma once

#include "raft/probe.h"
#include "raft/timeout_jitter.h"
#include "rpc/connection_cache.h"
#include "seastarx.h"
#include "storage/log.h"

#include <seastar/core/sharded.hh>

#include <boost/container/flat_map.hpp>

namespace raft {
class replicate_entries_stm;
class vote_stm;
class recovery_stm;
/// consensus for one raft group
class consensus {
public:
    struct voted_for_configuration {
        model::node_id voted_for;
        // for term it doesn't make sense to use numeric_limits<>::min
        model::term_id term{0};
    };
    enum class vote_state { follower, candidate, leader };
    using leader_cb_t = ss::noncopyable_function<void(group_id)>;
    using append_entries_cb_t
      = ss::noncopyable_function<void(std::vector<entry>&&)>;

    consensus(
      model::node_id,
      group_id,
      group_configuration,
      timeout_jitter,
      storage::log&,
      storage::log_append_config::fsync should_fsync,
      ss::io_priority_class io_priority,
      model::timeout_clock::duration disk_timeout,
      ss::sharded<rpc::connection_cache>&,
      leader_cb_t);

    /// Initial call. Allow for internal state recovery
    ss::future<> start();

    /// Stop all communications.
    ss::future<> stop();

    ss::future<vote_reply> vote(vote_request&& r) {
        return with_gate(_bg, [this, r = std::move(r)]() mutable {
            return with_semaphore(
              _op_sem, 1, [this, r = std::move(r)]() mutable {
                  return do_vote(std::move(r));
              });
        });
    }
    ss::future<append_entries_reply>
    append_entries(append_entries_request&& r) {
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

    /// This method adds a member to the group and performs configuration update
    ss::future<> add_group_member(model::broker node);

    bool is_leader() const { return _vstate == vote_state::leader; }
    model::node_id self() const { return _self; }
    const protocol_metadata& meta() const { return _meta; }
    const group_configuration& config() const { return _conf; }
    const model::ntp& ntp() const { return _log.ntp(); }
    clock_type::time_point last_heartbeat() const { return _hbeat; };

    void process_heartbeat(model::node_id, result<append_entries_reply>);
    ss::future<result<replicate_result>> replicate(raft::entry&&);
    ss::future<result<replicate_result>> replicate(std::vector<raft::entry>&&);

private:
    friend replicate_entries_stm;
    friend vote_stm;
    friend recovery_stm;
    // all these private functions assume that we are under exclusive operations
    // via the _op_sem
    void step_down();
    ss::future<vote_reply> do_vote(vote_request&&);
    ss::future<append_entries_reply>
    do_append_entries(append_entries_request&&);

    /// advances our pointer in the log
    append_entries_reply make_append_entries_reply(
      protocol_metadata, std::vector<storage::log::append_result>);

    ss::future<append_entries_reply>
      commit_entries(std::vector<entry>, append_entries_reply);

    ss::future<result<replicate_result>>
    do_replicate(std::vector<raft::entry>&&);

    ss::future<std::vector<storage::log::append_result>>
    disk_append(std::vector<entry>&&);

    ss::sstring voted_for_filename() const;

    /// used for timer callback to dispatch the vote_stm
    void dispatch_vote();
    /// Replicates configuration to other nodes,
    //  it have to be called under ops semaphore
    ss::future<> replicate_configuration(group_configuration);
    /// After we append on disk, we must consume the entries
    /// to update our leader_id, nodes & learners configuration
    ss::future<> process_configurations(std::vector<entry>&&);

    void arm_vote_timeout();

    // args
    model::node_id _self;
    timeout_jitter _jit;
    storage::log& _log;
    storage::log_append_config::fsync _should_fsync;
    ss::io_priority_class _io_priority;
    model::timeout_clock::duration _disk_timeout;
    ss::sharded<rpc::connection_cache>& _clients;
    leader_cb_t _leader_notification;

    // _conf is set *both* in ctor with initial configuration
    // and it is overriden to the last one found the in the last log segment
    group_configuration _conf;

    // read at `ss::future<> start()`
    model::node_id _voted_for;
    protocol_metadata _meta;

    /// useful for when we are not the leader
    clock_type::time_point _hbeat = clock_type::now();
    /// used to keep track if we are a leader, or transitioning
    vote_state _vstate = vote_state::follower;
    /// used for votes only. heartbeats are done by heartbeat_manager
    timer_type _vote_timeout;

    /// used for keepint tally on followers
    boost::container::flat_map<model::node_id, follower_index_metadata>
      _follower_stats;

    /// used to wait for background ops before shutting down
    ss::gate _bg;

    /// all raft operations must happen exclusively since the common case
    /// is for the operation to touch the disk
    ss::semaphore _op_sem{1};
    /// used for notifying when commits happened to log
    append_entries_cb_t _append_entries_notification;
    probe _probe;
};

} // namespace raft
