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
#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tx_protocol_types.h"
#include "cluster/tx_utils.h"
#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"
#include "features/feature_table.h"
#include "kafka/protocol/fwd.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/group_probe.h"
#include "kafka/server/member.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "raft/replicate.h"
#include "utils/mutex.h"
#include "utils/rwlock.h"

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

using assignments_type = std::unordered_map<member_id, bytes>;

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
std::optional<group_state> group_state_from_kafka_name(std::string_view);
cluster::begin_group_tx_reply make_begin_tx_reply(cluster::tx::errc);
cluster::commit_group_tx_reply make_commit_tx_reply(cluster::tx::errc);
cluster::abort_group_tx_reply make_abort_tx_reply(cluster::tx::errc);
kafka::error_code map_store_offset_error_code(std::error_code);

/// \brief A Kafka group.
///
/// Container of members.
class group final : public ss::enable_lw_shared_from_this<group> {
public:
    using clock_type = ss::lowres_clock;
    using duration_type = clock_type::duration;
    using time_point_type = clock_type::time_point;

    static constexpr int8_t fence_control_record_v0_version{0};
    static constexpr int8_t fence_control_record_v1_version{1};
    static constexpr int8_t fence_control_record_version{2};
    static constexpr int8_t prepared_tx_record_version{0};
    static constexpr int8_t commit_tx_record_version{0};
    static constexpr int8_t aborted_tx_record_version{0};

    template<typename Result>
    struct stages {
        using value_type = Result;

        explicit stages(Result res)
          : dispatched(ss::now())
          , result(ss::make_ready_future<Result>(std::move(res))) {}

        explicit stages(ss::future<Result> res)
          : dispatched(ss::now())
          , result(std::move(res)) {}

        stages(ss::future<> dispatched, ss::future<Result> res)
          : dispatched(std::move(dispatched))
          , result(std::move(res)) {}

        ss::future<> dispatched;
        ss::future<Result> result;
    };
    using offset_commit_stages = stages<offset_commit_response>;
    using join_group_stages = stages<join_group_response>;
    using sync_group_stages = stages<sync_group_response>;
    /**
     * represents an offset that is to be stored as a part of transaction
     */
    struct pending_tx_offset {
        group_tx::partition_offset offset_metadata;
        model::offset log_offset;
    };
    /**
     * In memory representation of active transaction. The transaction is added
     * when a state machine executes begin transaction request. The transaction
     * is removed when the state machine executes commit or abort transaction
     * request. The transaction holds all pending offset commits.
     */
    struct ongoing_transaction {
        ongoing_transaction(
          model::tx_seq, model::partition_id, model::timeout_clock::duration);

        model::tx_seq tx_seq;
        model::partition_id coordinator_partition;

        model::timeout_clock::duration timeout;
        model::timeout_clock::time_point last_update;

        bool is_expiration_requested{false};

        model::timeout_clock::time_point deadline() const {
            return last_update + timeout;
        }

        bool is_expired() const {
            return is_expiration_requested || deadline() <= clock_type::now();
        }

        void update_last_update_time() {
            last_update = model::timeout_clock::now();
        }

        chunked_hash_map<model::topic_partition, pending_tx_offset> offsets;
    };

    struct tx_producer {
        explicit tx_producer(model::producer_epoch);

        model::producer_epoch epoch;
        std::unique_ptr<ongoing_transaction> transaction;
    };

    struct offset_metadata {
        model::offset log_offset;
        model::offset offset;
        ss::sstring metadata;
        kafka::leader_epoch committed_leader_epoch;
        model::timestamp commit_timestamp;
        std::optional<model::timestamp> expiry_timestamp;
        /*
         * this is an offset that was written prior to upgrading to redpanda
         * with offset retention support. because these offsets did not
         * persistent retention metadata we act conservatively and skip
         * automatic reclaim. offset delete api can be used to remove them.
         */
        bool non_reclaimable{false};

        friend std::ostream& operator<<(std::ostream&, const offset_metadata&);
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
                probe.setup_public_metrics(group_id, tp);
            }
        }
    };

    struct ongoing_tx_offsets {
        model::producer_identity pid;
        model::tx_seq tx_seq;
        absl::node_hash_map<model::topic_partition, offset_metadata> offsets;
    };

    group(
      kafka::group_id id,
      group_state s,
      config::configuration& conf,
      ss::lw_shared_ptr<ssx::rwlock> catchup_lock,
      ss::lw_shared_ptr<cluster::partition> partition,
      model::term_id,
      ss::sharded<cluster::tx_gateway_frontend>& tx_frontend,
      ss::sharded<features::feature_table>&,
      group_metadata_serializer,
      enable_group_metrics);

    // constructor used when loading state from log
    group(
      kafka::group_id id,
      group_metadata_value& md,
      config::configuration& conf,
      ss::lw_shared_ptr<ssx::rwlock> catchup_lock,
      ss::lw_shared_ptr<cluster::partition> partition,
      model::term_id,
      ss::sharded<cluster::tx_gateway_frontend>& tx_frontend,
      ss::sharded<features::feature_table>&,
      group_metadata_serializer,
      enable_group_metrics);

    ~group() noexcept;

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

    bool subscribed(const model::topic&) const;

    static absl::node_hash_set<model::topic>
      decode_consumer_subscriptions(iobuf);

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

    /// Check if this is a consumer_group or not
    bool is_consumer_group() const {
        return _protocol_type == consumer_group_protocol_type;
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
      member_ptr member,
      chunked_vector<member_protocol>&& new_protocols,
      const std::optional<kafka::client_id>& new_client_id,
      const kafka::client_host& new_client_host,
      std::chrono::milliseconds new_session_timeout,
      std::chrono::milliseconds new_rebalance_timeout);

    /**
     * Same as update_member but without returning the join promise. Used when
     * reverting member state after failed group checkpoint
     */
    void update_member_no_join(
      member_ptr member,
      chunked_vector<member_protocol>&& new_protocols,
      const std::optional<kafka::client_id>& new_client_id,
      const kafka::client_host& new_client_host,
      std::chrono::milliseconds new_session_timeout,
      std::chrono::milliseconds new_rebalance_timeout);

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
    chunked_vector<join_group_response_member> member_metadata() const;

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
    join_group_stages
    handle_join_group(join_group_request&& r, bool is_new_group);

    /// Handle join of an unknown member.
    join_group_stages join_group_unknown_member(join_group_request&& request);

    /// Handle join of a known member.
    join_group_stages join_group_known_member(join_group_request&& request);

    /// Add a new member and initiate a rebalance.
    join_group_stages add_member_and_rebalance(
      kafka::member_id member_id, join_group_request&& request);

    /// Update an existing member and rebalance.
    join_group_stages update_member_and_rebalance(
      member_ptr member, join_group_request&& request);

    /// Add new member with dynamic membership
    group::join_group_stages
    add_new_dynamic_member(member_id, join_group_request&&);

    /// Add new member with static membership
    group::join_group_stages
    add_new_static_member(member_id, join_group_request&&);

    /// Replaces existing static member with the new one
    member_ptr replace_static_member(
      const group_instance_id&, const member_id&, const member_id&);

    /// Updates existing static member and initiate rebalance if needed
    group::join_group_stages update_static_member_and_rebalance(
      member_id, member_id, join_group_request&&);

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
    sync_group_stages handle_sync_group(sync_group_request&& r);

    /// Handle sync group in completing rebalance state.
    sync_group_stages sync_group_completing_rebalance(
      member_ptr member, sync_group_request&& request);

    /// Complete syncing for members.
    void finish_syncing_members(error_code error);

    /// Handle a heartbeat request.
    ss::future<heartbeat_response> handle_heartbeat(heartbeat_request&& r);

    /// Handle a leave group request.
    ss::future<leave_group_response>
    handle_leave_group(leave_group_request&& r);

    /// Handle leave group for single member
    kafka::error_code member_leave_group(
      const member_id&, const std::optional<group_instance_id>&);

    std::optional<offset_metadata>
    offset(const model::topic_partition& tp) const {
        if (auto it = _offsets.find(tp); it != _offsets.end()) {
            return it->second->metadata;
        }
        return std::nullopt;
    }

    const auto& offsets() const { return _offsets; }

    void complete_offset_commit(
      const model::topic_partition& tp, const offset_metadata& md);

    void fail_offset_commit(
      const model::topic_partition& tp, const offset_metadata& md);

    void reset_tx_state(model::term_id);
    model::term_id term() const { return _term; }

    ss::future<cluster::commit_group_tx_reply>
    commit_tx(cluster::commit_group_tx_request r);

    ss::future<cluster::begin_group_tx_reply>
      begin_tx(cluster::begin_group_tx_request);

    ss::future<cluster::abort_group_tx_reply>
      abort_tx(cluster::abort_group_tx_request);

    ss::future<txn_offset_commit_response>
    store_txn_offsets(txn_offset_commit_request r);

    offset_commit_stages store_offsets(offset_commit_request&& r);

    ss::future<txn_offset_commit_response>
    handle_txn_offset_commit(txn_offset_commit_request r);

    ss::future<cluster::begin_group_tx_reply>
    handle_begin_tx(cluster::begin_group_tx_request r);

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

    void
    insert_ongoing_tx(model::producer_identity pid, ongoing_transaction tx);
    void try_set_fence(model::producer_id id, model::producer_epoch epoch) {
        auto [it, _] = _producers.try_emplace(id, epoch);
        if (it->second.epoch < epoch) {
            it->second.epoch = epoch;
            it->second.transaction.reset();
        }
    }

    // helper for the kafka api: describe groups
    described_group describe() const;

    // transition group to `dead` state if empty, otherwise an appropriate error
    // is returned for the current state.
    ss::future<error_code> remove();

    // remove offsets associated with topic partitions
    ss::future<>
    remove_topic_partitions(const chunked_vector<model::topic_partition>& tps);

    const ss::lw_shared_ptr<cluster::partition> partition() const {
        return _partition;
    }

    ss::future<result<raft::replicate_result>> store_group(model::record_batch);

    // validates state of a member existing in a group
    error_code validate_existing_member(
      const member_id&,
      const std::optional<group_instance_id>&,
      const ss::sstring&) const;

    // shutdown group. cancel all pending operations
    ss::future<> shutdown();

    void add_offset_tombstone_record(
      const kafka::group_id& group,
      const model::topic_partition& tp,
      storage::record_batch_builder& builder);

    void add_group_tombstone_record(
      const kafka::group_id& group, storage::record_batch_builder& builder);

    /*
     * Delete group offsets that have expired.
     *
     * If after expired offsets have been deleted the group is in the empty
     * state and contains no offsets then the group will be marked as dead.
     *
     * The set of expired offsets that have been removed is returned.
     */
    std::vector<model::topic_partition>
    delete_expired_offsets(std::chrono::seconds retention_period);

    /*
     *  Delete group offsets that do not have subscriptions.
     *
     *  Returns the set of offsets that were deleted.
     */
    std::vector<model::topic_partition>
    delete_offsets(std::vector<model::topic_partition> offsets);

private:
    using member_map = absl::node_hash_map<kafka::member_id, member_ptr>;
    using protocol_support = absl::node_hash_map<kafka::protocol_name, int>;
    using producers_map = chunked_hash_map<model::producer_id, tx_producer>;

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
        void warn(const char* format, Args&&... args) const {
            log(ss::log_level::warn, format, std::forward<Args>(args)...);
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
              pid, ss::make_lw_shared<mutex>("tx_lock_group"));
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
    model::record_batch checkpoint();

    template<typename Func>
    model::record_batch do_checkpoint(Func&& assignments_provider) {
        kafka::group_metadata_key key;
        key.group_id = _id;
        kafka::group_metadata_value metadata;

        metadata.protocol_type = protocol_type().value_or(
          kafka::protocol_type(""));
        metadata.generation = generation();
        metadata.protocol = protocol();
        metadata.leader = leader();
        metadata.state_timestamp = _state_timestamp.value_or(
          model::timestamp(-1));

        for (const auto& [id, member] : _members) {
            auto state = member->state().copy();
            // this is not coming from the member itself because the checkpoint
            // occurs right before the members go live and get their
            // assignments.
            state.assignment = bytes_to_iobuf(assignments_provider(id));
            state.subscription = bytes_to_iobuf(
              member->get_protocol_metadata(_protocol.value()));
            metadata.members.push_back(std::move(state));
        }

        cluster::simple_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        auto kv = _md_serializer.to_kv(group_metadata_kv{
          .key = std::move(key), .value = std::move(metadata)});
        builder.add_raw_kv(std::move(kv.key), std::move(kv.value));

        return std::move(builder).build();
    }

    error_code
    validate_expected_group(const txn_offset_commit_request& r) const;

    bool has_offsets() const;

    bool has_pending_transaction(const model::topic_partition& tp) {
        if (std::any_of(
              _pending_offset_commits.begin(),
              _pending_offset_commits.end(),
              [&tp](const auto& tp_info) { return tp_info.first == tp; })) {
            return true;
        }

        if (std::any_of(
              _producers.begin(), _producers.end(), [&tp](const auto& p) {
                  return p.second.transaction
                         && p.second.transaction->offsets.contains(tp);
              })) {
            return true;
        }

        return false;
    }

    void update_store_offset_builder(
      cluster::simple_batch_builder& builder,
      const model::topic& name,
      model::partition_id partition,
      model::offset commited_offset,
      leader_epoch commited_leader_epoch,
      const ss::sstring& metadata,
      model::timestamp commited_timestemp,
      std::optional<model::timestamp> expiry_timestamp);

    ss::future<cluster::abort_group_tx_reply> do_abort(
      kafka::group_id group_id,
      model::producer_identity pid,
      model::tx_seq tx_seq);

    ss::future<cluster::commit_group_tx_reply> do_commit(
      kafka::group_id group_id,
      model::producer_identity pid,
      model::tx_seq sequence);

    void start_abort_timer() {
        _auto_abort_timer.set_callback([this] { abort_old_txes(); });
        try_arm(clock_type::now() + _abort_interval_ms);
    }

    void abort_old_txes();
    ss::future<> do_abort_old_txes();
    ss::future<cluster::tx::errc> try_abort_old_tx(model::producer_identity);
    ss::future<cluster::tx::errc> do_try_abort_old_tx(model::producer_identity);
    void try_arm(time_point_type);
    void maybe_rearm_timer();

    void update_subscriptions();
    std::optional<absl::node_hash_set<model::topic>> _subscriptions;

    std::vector<model::topic_partition> filter_expired_offsets(
      std::chrono::seconds retention_period,
      const std::function<bool(const model::topic&)>&,
      const std::function<model::timestamp(const offset_metadata&)>&);

    std::vector<model::topic_partition>
    get_expired_offsets(std::chrono::seconds retention_period);

    bool use_dedicated_batch_type_for_fence() const {
        // Prior to this change group_tx_fence shared the fence record
        // batch type with data partitions (tx_fence). This made compaction
        // logic complicated particularly because different compaction rules
        // applied for fence batch in groups and data paritions. With the new
        // feature, group fence has a separate dedicated batch type so it is
        // easy to diambiguate both fence types.
        return _feature_table.local().is_active(
          features::feature::group_tx_fence_dedicated_batch_type);
    }

    cluster::tx::errc map_tx_replication_error(std::error_code ec);

    kafka::group_id _id;
    group_state _state;
    std::optional<model::timestamp> _state_timestamp;
    kafka::generation_id _generation;
    protocol_support _supported_protocols;
    member_map _members;
    chunked_hash_map<group_instance_id, member_id> _static_members;
    int _num_members_joining;
    absl::node_hash_map<kafka::member_id, ss::timer<clock_type>>
      _pending_members;
    std::optional<kafka::protocol_type> _protocol_type;
    std::optional<kafka::protocol_name> _protocol;
    std::optional<kafka::member_id> _leader;
    ss::timer<clock_type> _join_timer;
    bool _new_member_added;
    config::configuration& _conf;
    ss::lw_shared_ptr<ssx::rwlock> _catchup_lock;
    ss::lw_shared_ptr<cluster::partition> _partition;
    chunked_hash_map<
      model::topic_partition,
      std::unique_ptr<offset_metadata_with_probe>>
      _offsets;
    group_probe<
      model::topic_partition,
      std::unique_ptr<offset_metadata_with_probe>>
      _probe;
    ctx_log _ctxlog;
    ctx_log _ctx_txlog;
    group_metadata_serializer _md_serializer;
    /**
     * flag indicating that the group rebalance is a result of initial join i.e.
     * the group was in Empty state before it went into PreparingRebalance
     * state. Initial join in progress flag will prevent completing join when
     * all members joined but initial delay is still in progress
     */
    bool _initial_join_in_progress = false;

    absl::flat_hash_map<model::producer_id, ss::lw_shared_ptr<mutex>> _tx_locks;
    model::term_id _term;
    producers_map _producers;
    chunked_hash_map<model::topic_partition, offset_metadata>
      _pending_offset_commits;
    enable_group_metrics _enable_group_metrics;

    ss::gate _gate;
    ss::timer<clock_type> _auto_abort_timer;
    std::chrono::milliseconds _abort_interval_ms;

    ss::sharded<cluster::tx_gateway_frontend>& _tx_frontend;
    ss::sharded<features::feature_table>& _feature_table;
};

using group_ptr = ss::lw_shared_ptr<group>;

/// @}

} // namespace kafka
