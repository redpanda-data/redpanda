#pragma once

#include "hashing/crc32c.h"
#include "model/fundamental.h"
#include "raft/consensus_client_protocol.h"
#include "raft/event_manager.h"
#include "raft/follower_stats.h"
#include "raft/logger.h"
#include "raft/probe.h"
#include "raft/replicate_batcher.h"
#include "raft/timeout_jitter.h"
#include "raft/types.h"
#include "rpc/connection_cache.h"
#include "seastarx.h"
#include "storage/log.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
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

        uint32_t crc() const {
            crc32 c;
            c.extend(voted_for());
            c.extend(term());
            return c.value();
        }
    };
    enum class vote_state { follower, candidate, leader };
    using leader_cb_t = ss::noncopyable_function<void(leadership_status)>;

    consensus(
      model::node_id,
      group_id,
      group_configuration,
      timeout_jitter,
      storage::log,
      ss::io_priority_class io_priority,
      model::timeout_clock::duration disk_timeout,
      consensus_client_protocol,
      leader_cb_t);

    /// Initial call. Allow for internal state recovery
    ss::future<> start();

    /// Stop all communications.
    ss::future<> stop();

    ss::future<vote_reply> vote(vote_request&& r);
    ss::future<append_entries_reply> append_entries(append_entries_request&& r);
    ss::future<install_snapshot_reply>
    install_snapshot(install_snapshot_request&& r);

    /// This method adds a member to the group and performs configuration update
    ss::future<> add_group_member(model::broker node);

    bool is_leader() const { return _vstate == vote_state::leader; }
    std::optional<model::node_id> get_leader_id() const { return _leader_id; }
    model::node_id self() const { return _self; }
    protocol_metadata meta() const {
        auto lstats = _log.offsets();
        return protocol_metadata{.group = _group,
                                 .commit_index = _commit_index,
                                 .term = _term,
                                 .prev_log_index = lstats.dirty_offset,
                                 .prev_log_term = lstats.dirty_offset_term};
    }
    raft::group_id group() const { return _group; }
    model::term_id term() const { return _term; }
    const group_configuration& config() const { return _conf; }
    const model::ntp& ntp() const { return _log.config().ntp; }
    clock_type::time_point last_heartbeat() const { return _hbeat; };

    clock_type::time_point last_hbeat_timestamp(model::node_id);

    /// Increment and returns next append_entries order tracking sequence for
    /// follower with given node id
    follower_req_seq next_follower_sequence(model::node_id);

    void process_append_entries_reply(
      model::node_id, result<append_entries_reply>, follower_req_seq);

    ss::future<result<replicate_result>>
    replicate(model::record_batch_reader&&, replicate_options);

    ss::future<model::record_batch_reader>
    make_reader(storage::log_reader_config config);

    model::offset committed_offset() const { return _commit_index; }

    ss::future<> step_down(model::term_id term) {
        return _op_lock.with([this, term] {
            _term = term;
            do_step_down();
        });
    }

    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg) {
        return _log.timequery(cfg);
    }

    model::offset start_offset() const { return _log.offsets().start_offset; }

    event_manager& events() { return _event_manager; }

private:
    friend replicate_entries_stm;
    friend vote_stm;
    friend recovery_stm;
    friend replicate_batcher;
    friend event_manager;

    // all these private functions assume that we are under exclusive operations
    // via the _op_sem
    void do_step_down();
    ss::future<vote_reply> do_vote(vote_request&&);
    ss::future<append_entries_reply>
    do_append_entries(append_entries_request&&);
    ss::future<install_snapshot_reply>
    do_install_snapshot(install_snapshot_request&& r);
    append_entries_reply make_append_entries_reply(storage::append_result);

    ss::future<> notify_entries_commited(
      model::offset start_offset, model::offset end_offset);

    ss::future<result<replicate_result>>
    do_replicate(model::record_batch_reader&&);

    ss::future<storage::append_result>
    disk_append(model::record_batch_reader&&);

    using success_reply = ss::bool_class<struct successfull_reply_tag>;

    success_reply update_follower_index(
      model::node_id, result<append_entries_reply>, follower_req_seq seq_id);
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
    //  caller have to pass in _op_sem semaphore units
    ss::future<>
    replicate_configuration(ss::semaphore_units<> u, group_configuration);
    /// After we append on disk, we must consume the entries
    /// to update our leader_id, nodes & learners configuration
    ss::future<>
    process_configurations(model::record_batch_reader&&, model::offset);

    ss::future<> maybe_update_follower_commit_idx(model::offset);

    void arm_vote_timeout();
    void update_node_hbeat_timestamp(model::node_id);

    void update_follower_stats(const group_configuration&);
    void trigger_leadership_notification();

    /// \brief _does not_ hold the lock.
    ss::future<> flush_log();
    /// \brief called by the vote timer, to dispatch a write under
    /// the ops semaphore
    void dispatch_flush_with_lock();

    absl::flat_hash_map<model::node_id, follower_req_seq>
    next_followers_request_seq();

    bool should_skip_vote();
    void setup_metrics();
    // args
    model::node_id _self;
    raft::group_id _group;
    timeout_jitter _jit;
    storage::log _log;
    ss::io_priority_class _io_priority;
    model::timeout_clock::duration _disk_timeout;
    consensus_client_protocol _client_protocol;
    leader_cb_t _leader_notification;
    model::offset _last_seen_config_offset;
    // _conf is set *both* in ctor with initial configuration
    // and it is overriden to the last one found the in the last log segment
    group_configuration _conf;

    // consensus state
    model::offset _commit_index;
    model::term_id _term;

    // read at `ss::future<> start()`
    model::node_id _voted_for;
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
    bool _has_pending_flushes{false};

    /// used to wait for background ops before shutting down
    ss::gate _bg;

    /// all raft operations must happen exclusively since the common case
    /// is for the operation to touch the disk
    mutex _op_lock;
    /// used for notifying when commits happened to log
    event_manager _event_manager;
    probe _probe;
    ctx_log _ctxlog;
    ss::condition_variable _commit_index_updated;

    consistency_level _last_replicate_consistency{
      consistency_level::quorum_ack};
    std::chrono::milliseconds _replicate_append_timeout;
    std::chrono::milliseconds _recovery_append_timeout;
    ss::metrics::metric_groups _metrics;
    ss::abort_source _as;

    friend std::ostream& operator<<(std::ostream&, const consensus&);
};

} // namespace raft
