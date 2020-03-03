#pragma once

#include "model/fundamental.h"
#include "raft/consensus_client_protocol.h"
#include "raft/follower_stats.h"
#include "raft/logger.h"
#include "raft/probe.h"
#include "raft/replicate_batcher.h"
#include "raft/timeout_jitter.h"
#include "rpc/connection_cache.h"
#include "seastarx.h"
#include "storage/log.h"

#include <seastar/core/sharded.hh>
#include <seastar/util/bool_class.hh>

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
    using leader_cb_t = ss::noncopyable_function<void(leadership_status)>;
    using append_entries_cb_t
      = ss::noncopyable_function<void(model::record_batch_reader&&)>;

    consensus(
      model::node_id,
      group_id,
      group_configuration,
      timeout_jitter,
      storage::log,
      storage::log_append_config::fsync should_fsync,
      ss::io_priority_class io_priority,
      model::timeout_clock::duration disk_timeout,
      consensus_client_protocol,
      leader_cb_t,
      std::optional<append_entries_cb_t>&& = std::nullopt);

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

    /// This method adds a member to the group and performs configuration update
    ss::future<> add_group_member(model::broker node);

    bool is_leader() const { return _vstate == vote_state::leader; }
    std::optional<model::node_id> get_leader_id() const { return _leader_id; }
    model::node_id self() const { return _self; }
    const protocol_metadata& meta() const { return _meta; }
    const group_configuration& config() const { return _conf; }
    const model::ntp& ntp() const { return _log.ntp(); }
    clock_type::time_point last_heartbeat() const { return _hbeat; };

    clock_type::time_point last_hbeat_timestamp(model::node_id);

    void
      process_heartbeat_response(model::node_id, result<append_entries_reply>);

    ss::future<result<replicate_result>>
    replicate(model::record_batch_reader&&);

    model::record_batch_reader make_reader(storage::log_reader_config config) {
        config.max_offset = std::min(
          config.max_offset, model::offset(_meta.commit_index));
        return _log.make_reader(std::move(config));
    }

    model::offset committed_offset() const {
        return model::offset(_meta.commit_index);
    }

    ss::future<> step_down() {
        if (is_leader()) {
            _ctxlog.trace("Resigned from leadership");
            return seastar::with_semaphore(
              _op_sem, 1, [this] { do_step_down(); });
        }
        return ss::make_ready_future<>();
    }

    void remove_append_entries_callback() {
        _append_entries_notification.reset();
    }
    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg) {
        return _log.timequery(cfg);
    }

private:
    friend replicate_entries_stm;
    friend vote_stm;
    friend recovery_stm;
    friend replicate_batcher;

    // all these private functions assume that we are under exclusive operations
    // via the _op_sem
    void do_step_down();
    ss::future<vote_reply> do_vote(vote_request&&);
    ss::future<append_entries_reply>
    do_append_entries(append_entries_request&&);

    append_entries_reply make_append_entries_reply(storage::append_result);

    ss::future<> notify_entries_commited(
      model::offset start_offset, model::offset end_offset);

    ss::future<result<replicate_result>>
    do_replicate(model::record_batch_reader&&);

    ss::future<storage::append_result>
    disk_append(model::record_batch_reader&&);

    using success_reply = ss::bool_class<struct successfull_reply_tag>;

    success_reply
      process_append_reply(model::node_id, result<append_entries_reply>);
    void successfull_append_entries_reply(
      follower_index_metadata&, append_entries_reply);
    void dispatch_recovery(follower_index_metadata&, append_entries_reply);
    void maybe_update_leader_commit_idx();
    ss::future<> do_maybe_update_leader_commit_idx();

    model::term_id get_term(model::offset);

    ss::sstring voted_for_filename() const;

    /// used for timer callback to dispatch the vote_stm
    void dispatch_vote();
    /// Replicates configuration to other nodes,
    //  it have to be called under ops semaphore
    ss::future<> replicate_configuration(group_configuration);
    /// After we append on disk, we must consume the entries
    /// to update our leader_id, nodes & learners configuration
    ss::future<>
    process_configurations(model::record_batch_reader&&, model::offset);

    ss::future<> maybe_update_follower_commit_idx(model::offset);

    void arm_vote_timeout();
    void update_node_hbeat_timestamp(model::node_id);

    void update_follower_stats(const group_configuration&);
    void trigger_leadership_notification();
    // args
    model::node_id _self;
    timeout_jitter _jit;
    storage::log _log;
    storage::log_append_config::fsync _should_fsync;
    ss::io_priority_class _io_priority;
    model::timeout_clock::duration _disk_timeout;
    consensus_client_protocol _client_protocol;
    leader_cb_t _leader_notification;
    model::offset _last_seen_config_offset;
    // _conf is set *both* in ctor with initial configuration
    // and it is overriden to the last one found the in the last log segment
    group_configuration _conf;

    // read at `ss::future<> start()`
    model::node_id _voted_for;
    protocol_metadata _meta;
    std::optional<model::node_id> _leader_id;

    /// useful for when we are not the leader
    clock_type::time_point _hbeat = clock_type::now();
    /// used to keep track if we are a leader, or transitioning
    vote_state _vstate = vote_state::follower;
    /// used for votes only. heartbeats are done by heartbeat_manager
    timer_type _vote_timeout;

    /// used for keepint tally on followers
    follower_stats _fstats;

    replicate_batcher _batcher;

    /// used to wait for background ops before shutting down
    ss::gate _bg;

    /// all raft operations must happen exclusively since the common case
    /// is for the operation to touch the disk
    ss::semaphore _op_sem{1};
    /// used for notifying when commits happened to log
    std::optional<append_entries_cb_t> _append_entries_notification;
    probe _probe;
    raft_ctx_log _ctxlog;
};

} // namespace raft
