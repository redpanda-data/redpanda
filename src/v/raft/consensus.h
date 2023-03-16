/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "features/feature_table.h"
#include "hashing/crc32c.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/append_entries_buffer.h"
#include "raft/configuration_manager.h"
#include "raft/consensus_client_protocol.h"
#include "raft/consensus_utils.h"
#include "raft/event_manager.h"
#include "raft/follower_stats.h"
#include "raft/group_configuration.h"
#include "raft/logger.h"
#include "raft/mutex_buffer.h"
#include "raft/offset_translator.h"
#include "raft/prevote_stm.h"
#include "raft/probe.h"
#include "raft/recovery_memory_quota.h"
#include "raft/recovery_throttle.h"
#include "raft/replicate_batcher.h"
#include "raft/timeout_jitter.h"
#include "raft/types.h"
#include "seastarx.h"
#include "ssx/semaphore.h"
#include "storage/fwd.h"
#include "storage/log.h"
#include "storage/snapshot.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/bool_class.hh>

#include <optional>
#include <string_view>

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
            crc::crc32c c;
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
            crc::crc32c c;
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
      scheduling_config,
      config::binding<std::chrono::milliseconds> disk_timeout,
      consensus_client_protocol,
      leader_cb_t,
      storage::api&,
      std::optional<std::reference_wrapper<recovery_throttle>>,
      recovery_memory_quota&,
      features::feature_table&,
      std::optional<voter_priority> = std::nullopt,
      keep_snapshotted_log = keep_snapshotted_log::no);

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

    /// This method adds member to a group
    ss::future<std::error_code>
      add_group_member(model::broker, model::revision_id);
    /// Updates given member configuration
    ss::future<std::error_code> update_group_member(model::broker);
    // Removes member from group
    ss::future<std::error_code>
      remove_member(model::node_id, model::revision_id);
    /**
     * Replace configuration, uses revision provided with brokers
     */
    ss::future<std::error_code>
      replace_configuration(std::vector<broker_revision>, model::revision_id);

    /**
     * New simplified configuration change API, accepting only vnode instead of
     * full broker object
     */
    ss::future<std::error_code> add_group_member(vnode, model::revision_id);
    ss::future<std::error_code> remove_member(vnode, model::revision_id);
    ss::future<std::error_code>
      replace_configuration(std::vector<vnode>, model::revision_id);

    // Abort ongoing configuration change - may cause data loss
    ss::future<std::error_code> abort_configuration_change(model::revision_id);
    // Revert current configuration change - this is safe and will never cause
    // data loss
    ss::future<std::error_code> cancel_configuration_change(model::revision_id);
    bool is_elected_leader() const { return _vstate == vote_state::leader; }
    // The node won the elections and made sure that the records written in
    // previous term are behind committed index
    bool is_leader() const {
        return is_elected_leader() && _term == _confirmed_term;
    }
    bool is_candidate() const { return _vstate == vote_state::candidate; }
    std::optional<model::node_id> get_leader_id() const {
        return _leader_id ? std::make_optional(_leader_id->id()) : std::nullopt;
    }

    /**
     * On leader, return the number of under replicated followers.  On
     * followers, return nullopt
     */
    std::optional<uint8_t> get_under_replicated() const;

    /**
     * Sends a round of heartbeats to followers, when majority of followers
     * replied with success to either this of any following request all reads up
     * to returned offsets are linearizable. (i.e. majority of followers have
     * updated their commit indices to at least reaturned offset). For more
     * details see paragraph 6.4 of Raft protocol dissertation.
     */
    ss::future<result<model::offset>> linearizable_barrier();

    vnode self() const { return _self; }
    protocol_metadata meta() const;
    raft::group_id group() const { return _group; }
    model::term_id term() const { return _term; }
    group_configuration config() const;
    const model::ntp& ntp() const { return _log.config().ntp(); }
    clock_type::time_point last_heartbeat() const { return _hbeat; };
    clock_type::time_point became_leader_at() const {
        return _became_leader_at;
    };

    clock_type::time_point last_sent_append_entries_req_timestamp(vnode);
    /**
     * \brief Persist snapshot with given data and start offset
     *
     * The write snaphot API is called by the state machine implementation
     * whenever it decides to take a snapshot. Write snapshot is executed under
     * consensus operations lock.
     */
    ss::future<> write_snapshot(write_snapshot_cfg);

    struct opened_snapshot {
        raft::snapshot_metadata metadata;
        storage::snapshot_reader reader;

        ss::future<> close() { return reader.close(); }
    };

    // Open the current snapshot for reading (if present)
    ss::future<std::optional<opened_snapshot>> open_snapshot();

    std::filesystem::path get_snapshot_path() const {
        return _snapshot_mgr.snapshot_path();
    }

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
    replicate_stages
    replicate_in_stages(model::record_batch_reader&&, replicate_options);
    uint64_t get_snapshot_size() const { return _snapshot_size; }

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
    replicate_stages replicate_in_stages(
      model::term_id, model::record_batch_reader&&, replicate_options);
    ss::future<model::record_batch_reader> make_reader(
      storage::log_reader_config,
      std::optional<clock_type::time_point> = std::nullopt);

    model::offset get_latest_configuration_offset() const;
    model::offset committed_offset() const { return _commit_index; }
    model::offset flushed_offset() const { return _flushed_offset; }
    model::offset last_quorum_replicated_index() const {
        return _last_quorum_replicated_index;
    }
    model::offset majority_replicated_index() const {
        return _majority_replicated_index;
    }
    model::offset visibility_upper_bound_index() const {
        return _visibility_upper_bound_index;
    }
    model::term_id confirmed_term() const { return _confirmed_term; }

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

    ss::future<> step_down(model::term_id term, std::string_view ctx) {
        return _op_lock.with([this, term, ctx] {
            // check again under op_lock semaphore, make sure we do not move
            // term backward
            if (term > _term) {
                _term = term;
                _voted_for = {};
                do_step_down(fmt::format(
                  "external_stepdown with term {} - {}", term, ctx));
            }
        });
    }

    ss::future<> step_down(std::string_view ctx) {
        return _op_lock.with([this, ctx] {
            do_step_down(fmt::format("external_stepdown - {}", ctx));
            if (_leader_id) {
                _leader_id = std::nullopt;
                trigger_leadership_notification();
            }
        });
    }

    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg);

    model::offset last_snapshot_index() const { return _last_snapshot_index; }
    model::term_id last_snapshot_term() const { return _last_snapshot_term; }
    model::offset received_snapshot_index() const {
        return _received_snapshot_index;
    }
    size_t received_snapshot_bytes() const { return _received_snapshot_bytes; }
    bool has_pending_flushes() const { return _has_pending_flushes; }

    model::offset start_offset() const {
        return model::next_offset(_last_snapshot_index);
    }

    model::offset dirty_offset() const { return _log.offsets().dirty_offset; }

    ss::condition_variable& commit_index_updated() {
        return _commit_index_updated;
    }

    event_manager& events() { return _event_manager; }

    ss::future<model::offset> monitor_log_eviction(ss::abort_source& as) {
        return _log.monitor_eviction(as);
    }

    const storage::ntp_config& log_config() const { return _log.config(); }

    /*
     * Attempt to transfer leadership to another node in this raft group. If no
     * node is specified, the most up-to-date node will be selected.
     */
    ss::future<transfer_leadership_reply>
      transfer_leadership(transfer_leadership_request);
    ss::future<std::error_code>
      prepare_transfer_leadership(vnode, transfer_leadership_options);
    ss::future<std::error_code>
      do_transfer_leadership(transfer_leadership_request);

    ss::future<> remove_persistent_state();

    /**
     *  Methods exposed for the state machine to speed up recovery
     */
    ss::future<> write_last_applied(model::offset);

    model::offset read_last_applied() const;

    probe& get_probe() { return _probe; };

    storage::log& log() { return _log; }

    ss::lw_shared_ptr<const storage::offset_translator_state>
    get_offset_translator_state() {
        return _offset_translator.state();
    }

    /**
     * In our raft implementation heartbeats are sent outside of the consensus
     * lock. In order to prevent reordering and do not flood followers with
     * heartbeats that they will not be able to respond to we suppress sending
     * heartbeats when other append entries request or heartbeat request is in
     * flight.
     */
    heartbeats_suppressed are_heartbeats_suppressed(vnode) const;

    void update_suppress_heartbeats(
      vnode, follower_req_seq, heartbeats_suppressed);

    void update_heartbeat_status(vnode, bool);

    bool should_reconnect_follower(vnode);

    std::vector<follower_metrics> get_follower_metrics() const;
    result<follower_metrics> get_follower_metrics(model::node_id) const;
    size_t get_follower_count() const;
    bool has_followers() const { return _fstats.size() > 0; }

    offset_monitor& visible_offset_monitor() {
        return _consumable_offset_monitor;
    }

    ss::future<> refresh_commit_index();

    model::term_id get_term(model::offset) const;

    /*
     * Prevent the current node from becoming a leader for this group. If the
     * node is the leader then this only takes affect if leadership is lost.
     */
    void block_new_leadership() {
        _node_priority_override = raft::zero_voter_priority;
    }

    /**
     * Resets node priority only if it was not blocked
     */
    void reset_node_priority() {
        if (_node_priority_override == raft::min_voter_priority) {
            unblock_new_leadership();
        }
    }
    /*
     * Allow the current node to become a leader for this group.
     */
    void unblock_new_leadership() { _node_priority_override.reset(); }

    const follower_stats& get_follower_stats() const { return _fstats; }

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
    void do_step_down(std::string_view);
    ss::future<vote_reply> do_vote(vote_request&&);
    ss::future<append_entries_reply>
    do_append_entries(append_entries_request&&);
    ss::future<install_snapshot_reply>
    do_install_snapshot(install_snapshot_request r);
    ss::future<> do_start();

    ss::future<result<replicate_result>> dispatch_replicate(
      append_entries_request,
      std::vector<ssx::semaphore_units>,
      absl::flat_hash_map<vnode, follower_req_seq>);
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

    replicate_stages do_replicate(
      std::optional<model::term_id>,
      model::record_batch_reader&&,
      replicate_options);

    ss::future<result<replicate_result>> chain_stages(replicate_stages);

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
    ss::future<> do_maybe_update_leader_commit_idx(ssx::semaphore_units);

    clock_type::time_point majority_heartbeat() const;
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
    replicate_configuration(ssx::semaphore_units u, group_configuration);

    ss::future<> maybe_update_follower_commit_idx(model::offset);

    void arm_vote_timeout();
    void update_node_append_timestamp(vnode);
    void update_node_reply_timestamp(vnode);
    void maybe_update_node_reply_timestamp(vnode);

    void update_follower_stats(const group_configuration&);
    void trigger_leadership_notification();

    /// \brief _does not_ hold the lock.
    ss::future<> flush_log();

    void maybe_step_down();

    absl::flat_hash_map<vnode, follower_req_seq> next_followers_request_seq();

    void setup_metrics();
    void setup_public_metrics();

    bytes voted_for_key() const {
        return raft::details::serialize_group_key(
          _group, metadata_key::voted_for);
    }
    void read_voted_for();
    ss::future<> write_voted_for(consensus::voted_for_configuration);
    model::term_id get_last_entry_term(const storage::offset_stats&) const;

    template<typename Func>
    ss::future<std::error_code> change_configuration(Func&&);

    template<typename Func>
    ss::future<std::error_code>
      interrupt_configuration_change(model::revision_id, Func);

    ss::future<> maybe_commit_configuration(ssx::semaphore_units);
    void maybe_promote_to_voter(vnode);

    ss::future<model::record_batch_reader>
      do_make_reader(storage::log_reader_config);

    bytes last_applied_key() const {
        return raft::details::serialize_group_key(
          _group, metadata_key::last_applied_offset);
    }

    void maybe_update_last_visible_index(model::offset);
    void maybe_update_majority_replicated_index();

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
      std::string_view request,
      result<Reply> reply,
      model::node_id requested_node_id) {
        if (reply) {
            // since we are not going to introduce the node in ADL versions of
            // replies it may be not initialzed, in this case just ignore the
            // check
            if (unlikely(
                  reply.value().node_id != vnode{}
                  && reply.value().node_id.id() != requested_node_id)) {
                vlog(
                  _ctxlog.warn,
                  "received {} reply from a node that id does not match the "
                  "requested one. Received: {}, requested: {}",
                  request,
                  reply.value().node_id.id(),
                  requested_node_id);
                return result<Reply>(errc::invalid_target_node);
            }
            if (unlikely(reply.value().target_node_id != self())) {
                /**
                 * if received node_id is not initialized it means that there
                 * were no raft group instance on target node to handle the
                 * request.
                 */
                if (reply.value().target_node_id.id() == model::node_id{}) {
                    // A grace period, where perhaps it is okay that a peer
                    // hasn't seen the controller log message that created
                    // this partition yet.
                    static constexpr clock_type::duration grace = 30s;
                    bool instantiated_recently = (clock_type::now()
                                                  - _instantiated_at)
                                                 < grace;
                    if (!instantiated_recently) {
                        vlog(
                          _ctxlog.warn,
                          "received {} reply from the node where ntp {} does "
                          "not "
                          "exists",
                          request,
                          _log.config().ntp());
                    }
                    return result<Reply>(errc::group_not_exists);
                }

                vlog(
                  _ctxlog.warn,
                  "received {} reply addressed to different node: {}, current "
                  "node: {}",
                  request,
                  reply.value().target_node_id,
                  _self);
                return result<Reply>(errc::invalid_target_node);
            }
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
              "node: {}, source: {}",
              request_name,
              target,
              _self,
              request.source_node());
            return true;
        }
        return false;
    }

    void maybe_upgrade_configuration_to_v4(group_configuration&);

    void update_confirmed_term();
    // args
    vnode _self;
    raft::group_id _group;
    timeout_jitter _jit;
    storage::log _log;
    offset_translator _offset_translator;
    scheduling_config _scheduling;
    config::binding<std::chrono::milliseconds> _disk_timeout;
    consensus_client_protocol _client_protocol;
    leader_cb_t _leader_notification;

    // consensus state
    model::offset _commit_index;
    model::term_id _term;
    // It's common to use raft log as a foundation for state machines:
    // when a node becomes a leader it replays the log, reconstructs
    // the state and becomes ready to serve the requests. However it is
    // not enough for a node to become a leader, it should successfully
    // replicate a new record to be sure that older records stored in
    // the local log were actually replicated and do not constitute an
    // artifact of the previously crashed leader. Redpanda uses a confi-
    // guration batch for the initial replication to gain certainty. When
    // commit index moves past the configuration batch _confirmed_term
    // gets updated. So when _term==_confirmed_term it's safe to use
    // local log to reconstruct the state.
    model::term_id _confirmed_term;
    model::offset _flushed_offset{};

    // read at `ss::future<> start()`
    vnode _voted_for;
    std::optional<vnode> _leader_id;
    bool _transferring_leadership{false};

    /// useful for when we are not the leader
    clock_type::time_point _hbeat = clock_type::now();
    clock_type::time_point _became_leader_at = clock_type::now();
    clock_type::time_point _instantiated_at = clock_type::now();

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
    size_t _heartbeat_disconnect_failures;
    ss::metrics::metric_groups _metrics;
    ss::abort_source _as;
    storage::api& _storage;
    std::optional<std::reference_wrapper<recovery_throttle>> _recovery_throttle;
    recovery_memory_quota& _recovery_mem_quota;
    features::feature_table& _features;
    storage::simple_snapshot_manager _snapshot_mgr;
    uint64_t _snapshot_size{0};
    std::optional<storage::snapshot_writer> _snapshot_writer;
    model::offset _last_snapshot_index;
    model::term_id _last_snapshot_term;
    configuration_manager _configuration_manager;
    model::offset _majority_replicated_index;
    model::offset _visibility_upper_bound_index;
    voter_priority _target_priority = voter_priority::max();
    std::optional<voter_priority> _node_priority_override;
    keep_snapshotted_log _keep_snapshotted_log;

    // used to track currently installed snapshot
    model::offset _received_snapshot_index;
    size_t _received_snapshot_bytes = 0;

    /**
     * We keep an idex of the most recent entry replicated with quorum
     * consistency level to make sure that all requests replicated with quorum
     * consistency level will not be visible before they are committed by
     * majority.
     */
    model::offset _last_quorum_replicated_index;
    consistency_level _last_write_consistency_level;
    offset_monitor _consumable_offset_monitor;
    ss::condition_variable _follower_reply;
    append_entries_buffer _append_requests_buffer;
    friend std::ostream& operator<<(std::ostream&, const consensus&);
};

} // namespace raft
