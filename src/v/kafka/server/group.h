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
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/partition.h"
#include "cluster/tx_utils.h"
#include "kafka/group_probe.h"
#include "kafka/protocol/fwd.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/logger.h"
#include "kafka/server/member.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "seastarx.h"
#include "utils/mutex.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/log.hh>

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

#include <iosfwd>
#include <optional>
#include <vector>

namespace config {
struct configuration;
}

namespace kafka {
struct group_log_group_metadata;

/**
 * \defgroup kafka-groups Kafka group membership API
 *
 * The Kafka API defines a group membership protocol for distributing and
 * synchronizing state across a set of clients. The primary use case for the
 * group membership API is the implementation of consumer groups which is a
 * feature in Kafka for distributing the work of consuming a topic across the
 * members in the group.
 *
 * The group membership API is generic and can be used by Kafka clients to
 * build other group membership-based features. For instance, in addition to
 * consumer groups, the group membership API is used to implement Kafka Connect
 * that aids in connecting Kafka with external data sources.
 *
 * A note on naming. Some of the names used in the group membership API
 * implementation are not ideal. However, most of the names directly correspond
 * to their counterparts in the Kafka implementation. This equivalence has
 * proven generally useful when comparing implementations.
 *
 * join_timer: the group contains a timer called the join timer. this timer
 * controls group state transitions in a couple scenarios. for a new group it
 * delays transition as long as members continue to join within in a time bound.
 * this delay implements a debouncing optimization. the new_member_added flag
 * tracks this scenario and is inspected in the timer callback. the delay is
 * also used to wait for all members to join before either rebalancing or
 * removing inactive members.
 *
 * \addtogroup kafka-groups
 * @{
 */

/**
 * \brief Group states.
 */
enum class group_state {
    /// The group currently has no members.
    empty,

    /// The group is preparing to rebalance.
    preparing_rebalance,

    /// The group is waiting on the leader to provide assignments.
    completing_rebalance,

    /// The group is stable.
    stable,

    /// Transient state as the group is being removed.
    dead,
};

using enable_group_metrics = ss::bool_class<struct enable_gr_metrics_tag>;

std::ostream& operator<<(std::ostream&, group_state gs);

ss::sstring group_state_to_kafka_name(group_state);
cluster::begin_group_tx_reply make_begin_tx_reply(cluster::tx_errc);
cluster::prepare_group_tx_reply make_prepare_tx_reply(cluster::tx_errc);
cluster::commit_group_tx_reply make_commit_tx_reply(cluster::tx_errc);
cluster::abort_group_tx_reply make_abort_tx_reply(cluster::tx_errc);
kafka::error_code map_store_offset_error_code(std::error_code);

/// \brief A Kafka group.
///
/// Container of members.
class group {
public:
    using clock_type = ss::lowres_clock;
    using duration_type = clock_type::duration;

    static constexpr int8_t fence_control_record_version{0};
    static constexpr int8_t prepared_tx_record_version{0};
    static constexpr int8_t commit_tx_record_version{0};
    static constexpr int8_t aborted_tx_record_version{0};

    struct offset_commit_stages {
        explicit offset_commit_stages(offset_commit_response resp)
          : dispatched(ss::now())
          , committed(
              ss::make_ready_future<offset_commit_response>(std::move(resp))) {}

        offset_commit_stages(
          ss::future<> dispatched, ss::future<offset_commit_response> resp)
          : dispatched(std::move(dispatched))
          , committed(std::move(resp)) {}

        ss::future<> dispatched;
        ss::future<offset_commit_response> committed;
    };

    struct offset_metadata {
        model::offset log_offset;
        model::offset offset;
        ss::sstring metadata;
        kafka::leader_epoch committed_leader_epoch;
    };

    struct offset_metadata_with_probe {
        offset_metadata metadata;
        group_offset_probe probe;

        offset_metadata_with_probe(
          offset_metadata _metadata,
          const kafka::group_id& group_id,
          const model::topic_partition& tp,
          enable_group_metrics enable_metrics)
          : metadata(std::move(_metadata))
          , probe(metadata.offset) {
            if (enable_metrics) {
                probe.setup_metrics(group_id, tp);
            }
        }
    };

    struct prepared_tx {
        model::producer_identity pid;
        model::tx_seq tx_seq;
        absl::node_hash_map<model::topic_partition, offset_metadata> offsets;
    };

    group(
      kafka::group_id id,
      group_state s,
      config::configuration& conf,
      ss::lw_shared_ptr<cluster::partition> partition,
      group_metadata_serializer,
      enable_group_metrics);

    // constructor used when loading state from log
    group(
      kafka::group_id id,
      group_metadata_value& md,
      config::configuration& conf,
      ss::lw_shared_ptr<cluster::partition> partition,
      group_metadata_serializer,
      enable_group_metrics);

    /// Get the group id.
    const kafka::group_id& id() const { return _id; }

    /// Return the group state.
    group_state state() const { return _state; }

    /// Check if the group is in a given state.
    bool in_state(group_state s) const { return _state == s; }

    /**
     * \brief Transition the group to a new state.
     *
     * Returns the previous state.
     */
    group_state set_state(group_state s);

    /// Return the generation of the group.
    kafka::generation_id generation() const { return _generation; }

    /**
     * \brief Access a group member.
     *
     * \throws std::out_of_range if member is not found.
     */
    member_ptr get_member(const kafka::member_id& id) const {
        return _members.at(id);
    }

    /// Check if the group contains a member.
    bool contains_member(const kafka::member_id& member_id) const {
        return _members.find(member_id) != _members.end();
    }

    /// Check if the group has members.
    bool has_members() const { return !_members.empty(); }

    /// Check if all members have joined.
    bool all_members_joined() const {
        vassert(
          _num_members_joining >= 0,
          "invalid number of joining members: {}",
          _num_members_joining);
        return (_members.size() == (size_t)_num_members_joining)
               && _pending_members.empty();
    }

    /// Add a member to the group in a pending state.
    void add_pending_member(const kafka::member_id&, duration_type);

    /// Check if the group contains a pending member.
    bool contains_pending_member(const kafka::member_id& member) const {
        return _pending_members.find(member) != _pending_members.end();
    }

    /// Reschedule all members' heartbeat expiration
    void reschedule_all_member_heartbeats() {
        for (auto& e : _members) {
            schedule_next_heartbeat_expiration(e.second);
        }
    }

    void remove_pending_member(kafka::member_id member_id);

    /// Check if a member id refers to the group leader.
    bool is_leader(const kafka::member_id& member_id) const {
        return _leader && _leader.value() == member_id;
    }

    /// Get the group's configured protocol type (if any).
    const std::optional<kafka::protocol_type>& protocol_type() const {
        return _protocol_type;
    }

    /// Get the group's configured protocol (if any).
    const std::optional<kafka::protocol_name>& protocol() const {
        return _protocol;
    }

    /// Get the group leader (if any).
    const std::optional<member_id>& leader() const { return _leader; }

    /**
     * \brief check if group supports a member's protocol configuration.
     *
     * if the group is empty, then as long as the member (1) specifies a
     * protocol type and (2) lists at least one protocol, the protocol
     * configuration is supported.
     *
     * if the group is non-empty, then the configuration is supported if
     * the group and member have the same protocol type and the member
     * specifies at least one protocol that is supported by all members of
     * the group.
     */
    bool supports_protocols(const join_group_request& r) const;

    /**
     * \brief Add a member to the group.
     *
     * If the group is empty, the member will define the group's protocol class
     * and become the group leader.
     *
     * \returns join response promise set at the end of the join phase.
     */
    ss::future<join_group_response> add_member(member_ptr member);

    /**
     * Same as add_member but without returning the join promise. Used when
     * restoring member state from persistent storage.
     */
    void add_member_no_join(member_ptr member);

    /**
     * \brief Update the set of protocols supported by a group member.
     *
     * \returns join response promise set at the end of the join phase.
     */
    ss::future<join_group_response> update_member(
      member_ptr member, std::vector<member_protocol>&& new_protocols);

    /**
     * \brief Get the timeout duration for rebalancing.
     *
     * Returns the maximum rebalance timeout across all group members.
     *
     * \throws std::runtime_error if the group has no members.
     */
    duration_type rebalance_timeout() const;

    /**
     * \brief Return member metadata for the group's selected protocol.
     *
     * This is used at the end of the join phase to generate the group leader's
     * response, which includes all of the member metadata associated with the
     * group's selected protocol.
     *
     * Caller must ensure that the group's protocol is set.
     */
    std::vector<join_group_response_member> member_metadata() const;

    /**
     * \brief Add empty assignments for missing group members.
     *
     * The assignments mapping is updated to include an empty assignment for any
     * group member without an assignment.
     */
    void add_missing_assignments(assignments_type& assignments) const;

    /**
     * \brief Apply the assignments to group members.
     *
     * Each assignment is a (member, bytes) mapping.
     *
     * \throws std::out_of_range if an assignment is for a member that does not
     * belong to the group.
     */
    void set_assignments(assignments_type assignments) const;

    /// Clears all member assignments.
    void clear_assignments() const;

    /**
     * \brief Advance the group to the next generation.
     *
     * When the group has members then a protocol is selected and the group
     * moves to the `completing_rebalance` state. Otherwise, the group is put
     * into the `empty` state.
     */
    void advance_generation();

    /**
     * \brief Select a group protocol.
     *
     * A protocol is selected by a voting process in which each member votes for
     * its preferred protocol from the set of protocols supported by all
     * members. The protocol with the most votes is selected.
     *
     * \throws std::out_of_range if any member fails to cast a vote.
     */
    kafka::protocol_name select_protocol() const;

    /// Check if moving to the given state is a valid transition.
    bool valid_previous_state(group_state s) const;

    /**
     * \brief Check if the leader has rejoined or choose new leader.
     *
     * Returns true if either the current leader has rejoined, or a joining
     * member is selected to be the new leader. Otherwise, false is returned.
     */
    bool leader_rejoined();

    /**
     * \brief Generate a new member id.
     *
     * The structure of a member id is "id-{uuid}" where `id` is the group
     * instance id if it exists, or the client id otherwise.
     */
    static kafka::member_id generate_member_id(const join_group_request& r);

    /// Handle join entry point.
    ss::future<join_group_response>
    handle_join_group(join_group_request&& r, bool is_new_group);

    /// Handle join of an unknown member.
    ss::future<join_group_response>
    join_group_unknown_member(join_group_request&& request);

    /// Handle join of a known member.
    ss::future<join_group_response>
    join_group_known_member(join_group_request&& request);

    /// Add a new member and initiate a rebalance.
    ss::future<join_group_response> add_member_and_rebalance(
      kafka::member_id member_id, join_group_request&& request);

    /// Update an existing member and rebalance.
    ss::future<join_group_response> update_member_and_rebalance(
      member_ptr member, join_group_request&& request);

    /// Transition to preparing rebalance if possible.
    void try_prepare_rebalance();

    /// Finalize the join phase.
    void complete_join();

    /// Handle a heartbeat expiration.
    void heartbeat_expire(
      kafka::member_id member_id, clock_type::time_point deadline);

    /// Send response to joining member.
    void try_finish_joining_member(
      member_ptr member, join_group_response&& response);

    /// Restart the member heartbeat timer.
    void schedule_next_heartbeat_expiration(member_ptr member);

    /// Removes a full member and may rebalance.
    void remove_member(member_ptr member);

    /// Handle a group sync request.
    ss::future<sync_group_response> handle_sync_group(sync_group_request&& r);

    /// Handle sync group in completing rebalance state.
    ss::future<sync_group_response> sync_group_completing_rebalance(
      member_ptr member, sync_group_request&& request);

    /// Complete syncing for members.
    void finish_syncing_members(error_code error);

    /// Handle a heartbeat request.
    ss::future<heartbeat_response> handle_heartbeat(heartbeat_request&& r);

    /// Handle a leave group request.
    ss::future<leave_group_response>
    handle_leave_group(leave_group_request&& r);

    std::optional<offset_metadata>
    offset(const model::topic_partition& tp) const {
        if (auto it = _offsets.find(tp); it != _offsets.end()) {
            return it->second->metadata;
        }
        return std::nullopt;
    }

    void complete_offset_commit(
      const model::topic_partition& tp, const offset_metadata& md);

    void fail_offset_commit(
      const model::topic_partition& tp, const offset_metadata& md);

    void reset_tx_state(model::term_id);

    ss::future<cluster::commit_group_tx_reply>
    commit_tx(cluster::commit_group_tx_request r);

    ss::future<cluster::begin_group_tx_reply>
      begin_tx(cluster::begin_group_tx_request);

    ss::future<cluster::prepare_group_tx_reply>
      prepare_tx(cluster::prepare_group_tx_request);

    ss::future<cluster::abort_group_tx_reply>
      abort_tx(cluster::abort_group_tx_request);

    ss::future<txn_offset_commit_response>
    store_txn_offsets(txn_offset_commit_request r);

    offset_commit_stages store_offsets(offset_commit_request&& r);

    ss::future<txn_offset_commit_response>
    handle_txn_offset_commit(txn_offset_commit_request r);

    ss::future<cluster::begin_group_tx_reply>
    handle_begin_tx(cluster::begin_group_tx_request r);

    ss::future<cluster::prepare_group_tx_reply>
    handle_prepare_tx(cluster::prepare_group_tx_request r);

    ss::future<cluster::abort_group_tx_reply>
    handle_abort_tx(cluster::abort_group_tx_request r);

    offset_commit_stages handle_offset_commit(offset_commit_request&& r);

    ss::future<cluster::commit_group_tx_reply>
    handle_commit_tx(cluster::commit_group_tx_request r);

    ss::future<offset_fetch_response>
    handle_offset_fetch(offset_fetch_request&& r);

    void insert_offset(model::topic_partition tp, offset_metadata md) {
        if (auto o_it = _offsets.find(tp); o_it != _offsets.end()) {
            o_it->second->metadata = std::move(md);
        } else {
            _offsets.emplace(
              std::move(tp),
              std::make_unique<offset_metadata_with_probe>(
                std::move(md), _id, tp, _enable_group_metrics));
        }
    }

    bool try_upsert_offset(model::topic_partition tp, offset_metadata md) {
        if (auto o_it = _offsets.find(tp); o_it != _offsets.end()) {
            if (o_it->second->metadata.log_offset < md.log_offset) {
                o_it->second->metadata = std::move(md);
                return true;
            }
            return false;
        } else {
            _offsets.emplace(
              std::move(tp),
              std::make_unique<offset_metadata_with_probe>(
                std::move(md), _id, tp, _enable_group_metrics));
            return true;
        }
    }

    void insert_prepared(prepared_tx);

    void try_set_fence(model::producer_id id, model::producer_epoch epoch) {
        auto [fence_it, _] = _fence_pid_epoch.try_emplace(id, epoch);
        if (fence_it->second < epoch) {
            fence_it->second = epoch;
        }
    }

    // helper for the kafka api: describe groups
    described_group describe() const;

    // transition group to `dead` state if empty, otherwise an appropriate error
    // is returned for the current state.
    ss::future<error_code> remove();

    // remove offsets associated with topic partitions
    ss::future<>
    remove_topic_partitions(const std::vector<model::topic_partition>& tps);

    const ss::lw_shared_ptr<cluster::partition> partition() const {
        return _partition;
    }

    // shutdown group. cancel all pending operations
    void shutdown();

private:
    using member_map = absl::node_hash_map<kafka::member_id, member_ptr>;
    using protocol_support = absl::node_hash_map<kafka::protocol_name, int>;

    friend std::ostream& operator<<(std::ostream&, const group&);

    class ctx_log {
    public:
        explicit ctx_log(ss::logger& logger, const group& group)
          : _logger(logger)
          , _group(group) {}

        template<typename... Args>
        void info(const char* format, Args&&... args) const {
            log(ss::log_level::info, format, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void error(const char* format, Args&&... args) const {
            log(ss::log_level::error, format, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void debug(const char* format, Args&&... args) const {
            log(ss::log_level::debug, format, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void trace(const char* format, Args&&... args) const {
            log(ss::log_level::trace, format, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void log(ss::log_level lvl, const char* format, Args&&... args) const {
            if (unlikely(_logger.is_enabled(lvl))) {
                auto line_fmt = ss::sstring("[N:{} S:{} G:{}] ") + format;
                _logger.log(
                  lvl,
                  line_fmt.c_str(),
                  _group.id()(),
                  _group.state(),
                  _group.generation(),
                  std::forward<Args>(args)...);
            }
        }

    private:
        ss::logger& _logger;
        const group& _group;
    };

    ss::lw_shared_ptr<mutex> get_tx_lock(model::producer_id pid) {
        auto lock_it = _tx_locks.find(pid);
        if (lock_it == _tx_locks.end()) {
            auto [new_it, _] = _tx_locks.try_emplace(
              pid, ss::make_lw_shared<mutex>());
            lock_it = new_it;
        }
        return lock_it->second;
    }

    void gc_tx_lock(model::producer_id pid) {
        if (auto it = _tx_locks.find(pid); it != _tx_locks.end()) {
            if (it->second->ready()) {
                _tx_locks.erase(it);
            }
        }
    }

    template<typename Func>
    auto with_pid_lock(model::producer_id pid, Func&& func) {
        return get_tx_lock(pid)
          ->with(std::forward<Func>(func))
          .then([this, pid](auto reply) {
              gc_tx_lock(pid);
              return reply;
          });
    }

    model::record_batch checkpoint(const assignments_type& assignments);

    cluster::abort_origin
    get_abort_origin(const model::producer_identity&, model::tx_seq) const;

    kafka::group_id _id;
    group_state _state;
    model::timestamp _state_timestamp;
    kafka::generation_id _generation;
    protocol_support _supported_protocols;
    member_map _members;
    int _num_members_joining;
    absl::node_hash_map<kafka::member_id, ss::timer<clock_type>>
      _pending_members;
    std::optional<kafka::protocol_type> _protocol_type;
    std::optional<kafka::protocol_name> _protocol;
    std::optional<kafka::member_id> _leader;
    ss::timer<clock_type> _join_timer;
    bool _new_member_added;
    config::configuration& _conf;
    ss::lw_shared_ptr<cluster::partition> _partition;
    absl::node_hash_map<
      model::topic_partition,
      std::unique_ptr<offset_metadata_with_probe>>
      _offsets;
    model::violation_recovery_policy _recovery_policy;
    ctx_log _ctxlog;
    ctx_log _ctx_txlog;
    group_metadata_serializer _md_serializer;

    absl::flat_hash_map<model::producer_id, ss::lw_shared_ptr<mutex>> _tx_locks;
    model::term_id _term;
    absl::node_hash_map<model::producer_id, model::producer_epoch>
      _fence_pid_epoch;
    absl::node_hash_map<model::topic_partition, offset_metadata>
      _pending_offset_commits;
    enable_group_metrics _enable_group_metrics;
    struct volatile_offset {
        model::offset offset;
        kafka::leader_epoch leader_epoch;
        std::optional<ss::sstring> metadata;
    };

    struct volatile_tx {
        model::tx_seq tx_seq;
        absl::node_hash_map<model::topic_partition, volatile_offset> offsets;
    };

    absl::node_hash_map<model::producer_identity, volatile_tx> _volatile_txs;
    absl::node_hash_map<model::producer_identity, prepared_tx> _prepared_txs;
};

using group_ptr = ss::lw_shared_ptr<group>;

std::ostream& operator<<(std::ostream&, const group&);

/// @}

} // namespace kafka
