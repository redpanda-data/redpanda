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

#include "hashing/crc32c.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/append_entries_buffer.h"
#include "raft/configuration_manager.h"
#include "raft/consensus_client_protocol.h"
#include "raft/event_manager.h"
#include "raft/follower_stats.h"
#include "raft/group_configuration.h"
#include "raft/logger.h"
#include "raft/mutex_buffer.h"
#include "raft/prevote_stm.h"
#include "raft/probe.h"
#include "raft/replicate_batcher.h"
#include "raft/timeout_jitter.h"
#include "raft/types.h"
#include "rpc/connection_cache.h"
#include "seastarx.h"
#include "storage/api.h"
#include "storage/log.h"
#include "storage/snapshot.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/bool_class.hh>

#include <optional>

namespace raft {
class replicate_entries_stm;
class vote_stm;
class prevote_stm;
class recovery_stm;
/// consensus for one raft group
class consensus {
public:
    // we maintain this for backward compatibility, will be removed in future
    // versions.
    struct voted_for_configuration_old {
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
    struct voted_for_configuration {
        vnode voted_for;
        // for term it doesn't make sense to use numeric_limits<>::min
        model::term_id term{0};

        uint32_t crc() const {
            crc32 c;
            c.extend(voted_for.id()());
            c.extend(voted_for.revision()());
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
      leader_cb_t,
      storage::api&);

    /// Initial call. Allow for internal state recovery
    ss::future<> start();

    /// Stop all communications.
    ss::future<> stop();

    /// Stop consensus instance from accepting requests
    void shutdown_input();

    ss::future<vote_reply> vote(vote_request&& r);
    ss::future<append_entries_reply> append_entries(append_entries_request&& r);
    ss::future<install_snapshot_reply>
    install_snapshot(install_snapshot_request&& r);

    ss::future<timeout_now_reply> timeout_now(timeout_now_request&& r);

    /// This method adds multiple members to the group and performs
    /// configuration update
    ss::future<std::error_code>
      add_group_members(std::vector<model::broker>, model::revision_id);
    /// Updates given member configuration
    ss::future<std::error_code> update_group_member(model::broker);
    // Removes members from group
    ss::future<std::error_code>
      remove_members(std::vector<model::node_id>, model::revision_id);
    // Replace configuration of raft group with given set of nodes
    ss::future<std::error_code>
      replace_configuration(std::vector<model::broker>, model::revision_id);

    bool is_leader() const { return _vstate == vote_state::leader; }
    bool is_candidate() const { return _vstate == vote_state::candidate; }
    std::optional<model::node_id> get_leader_id() const {
        return _leader_id ? std::make_optional(_leader_id->id()) : std::nullopt;
    }

    vnode self() const { return _self; }
    protocol_metadata meta() const {
        auto lstats = _log.offsets();
        return protocol_metadata{
          .group = _group,
          .commit_index = _commit_index,
          .term = _term,
          .prev_log_index = lstats.dirty_offset,
          .prev_log_term = lstats.dirty_offset_term,
          .last_visible_index = last_visible_index()};
    }
    raft::group_id group() const { return _group; }
    model::term_id term() const { return _term; }
    group_configuration config() const;
    const model::ntp& ntp() const { return _log.config().ntp(); }
    clock_type::time_point last_heartbeat() const { return _hbeat; };

    clock_type::time_point last_append_timestamp(vnode);
    /**
     * \brief Persist snapshot with given data and start offset
     *
     * The write snaphot API is called by the state machine implementation
     * whenever it decides to take a snapshot. Write snapshot is executed under
     * consensus operations lock.
     */
    ss::future<> write_snapshot(write_snapshot_cfg);

    /// Increment and returns next append_entries order tracking sequence for
    /// follower with given node id
    follower_req_seq next_follower_sequence(vnode);

    void process_append_entries_reply(
      model::node_id,
      result<append_entries_reply>,
      follower_req_seq,
      model::offset);

    ss::future<result<replicate_result>>
    replicate(model::record_batch_reader&&, replicate_options);

    /**
     * Replication happens only when expected_term matches the current _term
     * otherwise consensus returns not_leader. This feature is needed to keep
     * ingestion-time state machine in sync with the log. The conventional
     * state machines running on top on the log are optimistic: to execute a
     * command a user should add a command to a log (replicate) then continue
     * reading the commands from the log and executing them one after another.
     * When the commands are conditional the conventional approach is wasteful
     * because we even when a condition resolves to false we still pay the
     * replication costs. An alternative approach is to check the conditions
     * before replication but in this case there is a risk of divergence between
     * the log and the state (e.g. a leadership moves to an another broker, it
     * adds messages then the leadership moves back). The expected_term
     * prevents this situation. The expected use case is:
     *   1. when a cached term matches consensus.term() call replicate using
     *      the cached term as expected_term
     *   2. otherwise:
     *      a. abrt all incoming requests
     *      b. call consensus meta() to get the latest offset and a term
     *      c. wait until the state caches up with the latest offset
     *      d. cache the term
     *      e. continue with step #1
     */
    ss::future<result<replicate_result>>
    replicate(model::term_id, model::record_batch_reader&&, replicate_options);

    ss::future<model::record_batch_reader> make_reader(
      storage::log_reader_config,
      std::optional<clock_type::time_point> = std::nullopt);

    model::offset get_latest_configuration_offset() const;
    model::offset committed_offset() const { return _commit_index; }
    model::offset last_stable_offset() const;

    /**
     * Last visible index is an offset that is safe to be fetched by the
     * consumers. This is similar to Kafka's HighWatermark. Last visible
     * offset depends on the consistency level of messages replicated by the
     * consensus instance and state of the log. Last visible offset similarly
     * to HighWaterMark is monotonic and can never move backward. Last visible
     * offset is always greater than log start offset and smaller than log dirty
     * offset.
     *
     * Last visible offset is updated in two scenarios
     *
     * - commited offset is updated (consistency_level=quorum)
     * - when batch that was appendend to the leader log is safely replicated on
     *   majority of nodes
     *
     * We always update last visible index with std::max(prev,
     * possible_value) to guarantee its monotonicity.
     *
     */
    model::offset last_visible_index() const {
        return std::min(
          _majority_replicated_index, _visibility_upper_bound_index);
    };

    ss::future<offset_configuration>
    wait_for_config_change(model::offset last_seen, ss::abort_source& as) {
        return _configuration_manager.wait_for_change(last_seen, as);
    }

    ss::future<> step_down(model::term_id term) {
        return _op_lock.with([this, term] {
            // check again under op_lock semaphore, make sure we do not move
            // term backward
            if (term > _term) {
                _term = term;
                _voted_for = {};
                do_step_down();
            }
        });
    }

    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg) {
        return _log.timequery(cfg);
    }

    model::offset start_offset() const { return _log.offsets().start_offset; }

    event_manager& events() { return _event_manager; }

    ss::future<model::offset> monitor_log_eviction(ss::abort_source& as) {
        return _log.monitor_eviction(as);
    }

    const storage::ntp_config& log_config() const { return _log.config(); }

    /*
     * Attempt to transfer leadership to another node in this raft group. If no
     * node is specified, the most up-to-date node will be selected.
     */
    ss::future<std::error_code>
      transfer_leadership(std::optional<model::node_id>);

    ss::future<> remove_persistent_state();

    /**
     *  Methods exposed for the state machine to speed up recovery
     */
    ss::future<> write_last_applied(model::offset);

    model::offset read_last_applied() const;

    // yields a node scoped unique number on each invocation
    ss::future<model::run_id> get_run_id();

    probe& get_probe() { return _probe; };

    storage::log& log() { return _log; }

    bool are_heartbeats_suppressed(vnode) const;

    void suppress_heartbeats(vnode, follower_req_seq, bool);

private:
    friend replicate_entries_stm;
    friend vote_stm;
    friend prevote_stm;
    friend recovery_stm;
    friend replicate_batcher;
    friend event_manager;
    friend append_entries_buffer;
    using update_last_quorum_index
      = ss::bool_class<struct update_last_quorum_index>;
    // all these private functions assume that we are under exclusive operations
    // via the _op_sem
    void do_step_down();
    ss::future<vote_reply> do_vote(vote_request&&);
    ss::future<append_entries_reply>
    do_append_entries(append_entries_request&&);
    ss::future<install_snapshot_reply>
    do_install_snapshot(install_snapshot_request&& r);
    /**
     * Hydrate the consensus state with the data from the snapshot
     */
    ss::future<> hydrate_snapshot();
    ss::future<> do_hydrate_snapshot(storage::snapshot_reader&);

    /**
     * Truncates the log up the last offset stored in the snapshot
     */
    ss::future<> truncate_to_latest_snapshot();
    ss::future<install_snapshot_reply>
      finish_snapshot(install_snapshot_request, install_snapshot_reply);

    ss::future<> do_write_snapshot(model::offset, iobuf&&);
    append_entries_reply
      make_append_entries_reply(vnode, storage::append_result);

    ss::future<result<replicate_result>> do_replicate(
      std::optional<model::term_id>,
      model::record_batch_reader&&,
      replicate_options);

    ss::future<storage::append_result>
    disk_append(model::record_batch_reader&&, update_last_quorum_index);

    using success_reply = ss::bool_class<struct successfull_reply_tag>;

    success_reply update_follower_index(
      model::node_id,
      const result<append_entries_reply>&,
      follower_req_seq seq_id,
      model::offset);

    void successfull_append_entries_reply(
      follower_index_metadata&, append_entries_reply);

    bool needs_recovery(const follower_index_metadata&, model::offset);
    void dispatch_recovery(follower_index_metadata&);
    void maybe_update_leader_commit_idx();
    ss::future<> do_maybe_update_leader_commit_idx(ss::semaphore_units<>);

    model::term_id get_term(model::offset);

    /*
     * Start an election. When leadership transfer is requested, the election is
     * started immediately, and the vote request will contain a flag that
     * requests stable leadership optimization to be ignored.
     */
    void dispatch_vote(bool leadership_transfer);
    ss::future<bool> dispatch_prevote(bool leadership_transfer);
    bool should_skip_vote(bool ignore_heartbeat);

    /// Replicates configuration to other nodes,
    //  caller have to pass in _op_sem semaphore units
    ss::future<std::error_code>
    replicate_configuration(ss::semaphore_units<> u, group_configuration);

    ss::future<> maybe_update_follower_commit_idx(model::offset);

    void arm_vote_timeout();
    void update_node_append_timestamp(vnode);
    void update_node_hbeat_timestamp(vnode);

    void update_follower_stats(const group_configuration&);
    void trigger_leadership_notification();

    /// \brief _does not_ hold the lock.
    ss::future<> flush_log();
    /// \brief called by the vote timer, to dispatch a write under
    /// the ops semaphore
    void dispatch_flush_with_lock();

    void maybe_step_down();

    absl::flat_hash_map<vnode, follower_req_seq> next_followers_request_seq();

    void setup_metrics();

    bytes voted_for_key() const;
    void read_voted_for();
    ss::future<> write_voted_for(consensus::voted_for_configuration);
    model::term_id get_last_entry_term(const storage::offset_stats&) const;

    template<typename Func>
    ss::future<std::error_code> change_configuration(Func&&);

    ss::future<> maybe_commit_configuration(ss::semaphore_units<>);
    void maybe_promote_to_voter(vnode);

    ss::future<model::record_batch_reader>
      do_make_reader(storage::log_reader_config);

    bytes last_applied_key() const;

    void maybe_update_last_visible_index(model::offset);
    void maybe_update_majority_replicated_index();

    void start_dispatching_disk_append_events();

    voter_priority next_target_priority();
    voter_priority get_node_priority(vnode) const;

    /**
     * Return true if there is no state backing this consensus group i.e. there
     * is no snapshot and log is empty
     */
    bool is_initial_state() const {
        static constexpr model::offset not_initialized{};
        auto lstats = _log.offsets();
        return _log.segment_count() == 0
               && lstats.dirty_offset == not_initialized
               && lstats.start_offset == not_initialized
               && _last_snapshot_index == not_initialized;
    }

    template<typename Reply>
    result<Reply> validate_reply_target_node(
      std::string_view request, result<Reply>&& reply) {
        if (unlikely(reply && reply.value().target_node_id != self())) {
            vlog(
              _ctxlog.warn,
              "received {} reply addressed to different node: {}, current "
              "node: {}",
              request,
              reply.value().target_node_id,
              _self);
            return result<Reply>(errc::invalid_target_node);
        }
        return std::move(reply);
    }

    template<typename Request>
    bool is_request_target_node_invalid(
      std::string_view request_name, const Request& request) {
        auto target = request.target_node();
        if (unlikely(target != _self)) {
            vlog(
              _ctxlog.warn,
              "received {} request addressed to different node: {}, current "
              "node: {}",
              request_name,
              target,
              _self);
            return true;
        }
        return false;
    }
    // args
    vnode _self;
    raft::group_id _group;
    timeout_jitter _jit;
    storage::log _log;
    ss::io_priority_class _io_priority;
    model::timeout_clock::duration _disk_timeout;
    consensus_client_protocol _client_protocol;
    leader_cb_t _leader_notification;

    // consensus state
    model::offset _commit_index;
    model::term_id _term;

    // read at `ss::future<> start()`
    vnode _voted_for;
    std::optional<vnode> _leader_id;
    bool _transferring_leadership{false};

    /// useful for when we are not the leader
    clock_type::time_point _hbeat = clock_type::now();
    clock_type::time_point _became_leader_at = clock_type::now();
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

    std::chrono::milliseconds _replicate_append_timeout;
    std::chrono::milliseconds _recovery_append_timeout;
    ss::metrics::metric_groups _metrics;
    ss::abort_source _as;
    storage::api& _storage;
    storage::snapshot_manager _snapshot_mgr;
    std::optional<storage::snapshot_writer> _snapshot_writer;
    model::offset _last_snapshot_index;
    model::term_id _last_snapshot_term;
    configuration_manager _configuration_manager;
    model::offset _majority_replicated_index;
    model::offset _visibility_upper_bound_index;
    voter_priority _target_priority = voter_priority::max();
    /**
     * We keep an idex of the most recent entry replicated with quorum
     * consistency level to make sure that all requests replicated with quorum
     * consistency level will not be visible before they are committed by
     * majority.
     */
    model::offset _last_quorum_replicated_index;
    consistency_level _last_write_consistency_level;
    offset_monitor _consumable_offset_monitor;
    ss::condition_variable _disk_append;
    append_entries_buffer _append_requests_buffer;
    friend std::ostream& operator<<(std::ostream&, const consensus&);
};

} // namespace raft
