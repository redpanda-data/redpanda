/*
 * Copyright 2026 Redpanda Data, Inc.
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
#include "kafka/protocol/consumer_group_describe.h"
#include "kafka/protocol/consumer_group_heartbeat.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <absl/container/node_hash_map.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace kafka {

/// Metadata of a topic subscribed to by a consumer group, resolved from the
/// controller metadata on the coordinator.
struct consumer_group_topic_metadata {
    model::topic_id id;
    int32_t partition_count{0};

    friend bool operator==(
      const consumer_group_topic_metadata&,
      const consumer_group_topic_metadata&) = default;
};

/// Resolves a subscribed topic name to its id and partition count. Returns
/// std::nullopt if the topic does not exist.
using consumer_group_topic_resolver = ss::noncopyable_function<
  std::optional<consumer_group_topic_metadata>(const model::topic&)>;

/// Coordinator controlled timeouts handed to members of a consumer group.
struct consumer_group_settings {
    std::chrono::milliseconds session_timeout;
    std::chrono::milliseconds heartbeat_interval;
};

/// \brief State machine for a group using the KIP-848 next generation
/// consumer group protocol.
///
/// The state is driven entirely by the ConsumerGroupHeartbeat API: members
/// join with member epoch 0, leave with member epoch -1 (-2 for a static
/// member that will rejoin: its member entry, and with it its instance id
/// and assignment, stays reserved until the new incarnation takes it over
/// or the session expires), and otherwise report their currently owned
/// partitions while the coordinator hands out epoch-scoped incremental
/// assignments.
///
/// Assignment reconciliation follows the epoch model of KIP-848:
///
///   - the group epoch is bumped whenever membership, subscriptions or the
///     metadata of subscribed topics change. A target assignment is computed
///     for the new epoch with a coordinator-side assignor.
///   - each member converges to the target assignment. A member first
///     revokes partitions that are no longer assigned to it (remaining at
///     its current member epoch until the revocation is acknowledged), then
///     advances to the target epoch and is granted the subset of its target
///     assignment not still owned by other members. Remaining partitions are
///     granted on subsequent heartbeats as other members release them.
///
/// This state is kept in memory only: on coordinator failover members are
/// fenced with unknown_member_id and rejoin with epoch 0. Committed offsets
/// are persisted through the regular offset commit path of the owning group.
class consumer_group {
public:
    using clock_type = ss::lowres_clock;
    /// partitions of a set of topics, keyed by topic id.
    using assignment_type
      = absl::btree_map<model::topic_id, absl::btree_set<model::partition_id>>;

    static constexpr int32_t join_epoch = 0;
    static constexpr int32_t leave_epoch = -1;
    static constexpr int32_t static_leave_epoch = -2;

    static constexpr std::string_view uniform_assignor = "uniform";
    static constexpr std::string_view range_assignor = "range";

    /// Reconciliation state of a member with respect to the current target
    /// assignment, mirroring the member states of KIP-848.
    enum class reconcile_state {
        /// member epoch == target epoch and assignment == target assignment.
        stable,
        /// the member has been asked to revoke partitions and remains at its
        /// current epoch until the revocation is acknowledged.
        unrevoked_partitions,
        /// the member is at the target epoch but part of its target
        /// assignment is still owned by other members.
        unreleased_partitions,
    };

    struct member {
        kafka::member_id id;
        std::optional<kafka::group_instance_id> instance_id;
        std::optional<ss::sstring> rack_id;
        std::optional<kafka::client_id> client_id;
        kafka::client_host client_host;
        int32_t member_epoch{0};
        int32_t previous_member_epoch{0};
        std::chrono::milliseconds rebalance_timeout{-1};
        std::vector<model::topic> subscribed_topics;
        /// partitions the member is currently allowed to own.
        assignment_type assigned;
        /// partitions the member has been asked to revoke.
        assignment_type revoking;
        reconcile_state state{reconcile_state::stable};
        ss::timer<clock_type> session_timer;
    };

    explicit consumer_group(kafka::group_id id);

    consumer_group(const consumer_group&) = delete;
    consumer_group& operator=(const consumer_group&) = delete;
    consumer_group(consumer_group&&) = delete;
    consumer_group& operator=(consumer_group&&) = delete;
    ~consumer_group() = default;

    /// Handle a ConsumerGroupHeartbeat request routed to this group.
    consumer_group_heartbeat_response handle_heartbeat(
      consumer_group_heartbeat_request req,
      const consumer_group_topic_resolver& resolver,
      const consumer_group_settings& settings);

    /// Build the ConsumerGroupDescribe entry for this group.
    consumer_group_described_group describe() const;

    /// Validate an offset commit from a member of this group. The
    /// generation id of the offset commit request carries the member epoch
    /// for consumers using the new protocol.
    error_code
    validate_offset_commit(const kafka::member_id&, int32_t member_epoch) const;

    bool has_members() const { return !_members.empty(); }
    size_t member_count() const { return _members.size(); }
    int32_t group_epoch() const { return _group_epoch; }
    int32_t assignment_epoch() const { return _assignment_epoch; }
    const ss::sstring& assignor() const { return _assignor; }

    /// KIP-848 group state name: Empty, Reconciling or Stable.
    ss::sstring state_name() const;

private:
    consumer_group_heartbeat_response
    handle_leave(const consumer_group_heartbeat_request_data& data);

    consumer_group_heartbeat_response handle_join(
      consumer_group_heartbeat_request req,
      const consumer_group_topic_resolver& resolver,
      const consumer_group_settings& settings);

    consumer_group_heartbeat_response handle_existing_member(
      consumer_group_heartbeat_request req,
      const consumer_group_topic_resolver& resolver,
      const consumer_group_settings& settings);

    /// Re-resolve the metadata of all subscribed topics. Returns true if it
    /// changed since the last refresh.
    bool refresh_subscription_metadata(
      const consumer_group_topic_resolver& resolver);

    /// Bump the group epoch and compute a new target assignment.
    void on_group_updated();

    /// Compute the target assignment for the current group epoch.
    void compute_target_assignment();
    void compute_range_assignment();
    void compute_uniform_assignment();

    /// Advance the reconciliation of a member towards the current target
    /// assignment, updating its epoch, assignment and pending revocations.
    void reconcile_member(
      member&, const std::optional<assignment_type>& owned_partitions);

    /// Union of partitions currently owned (assigned or pending revocation)
    /// by all members except \p except.
    assignment_type
    partitions_owned_by_others(const kafka::member_id& except) const;

    void schedule_session_expiration(
      member&, std::chrono::milliseconds session_timeout);
    /// Takes the member id by value: removing the member destroys its
    /// session timer (and the timer callback's captures) while the callback
    /// is still running, so the id must not be a reference into it.
    void handle_session_expired(kafka::member_id);

    void remove_member(const kafka::member_id&);

    consumer_group_heartbeat_response make_response(
      const member&,
      const consumer_group_settings&,
      bool include_assignment) const;

    static consumer_group_heartbeat_response
    make_error_response(error_code error, ss::sstring msg);

    std::optional<model::topic> topic_name(const model::topic_id&) const;

    kafka::group_id _id;
    int32_t _group_epoch{0};
    int32_t _assignment_epoch{0};
    ss::sstring _assignor{uniform_assignor};
    absl::node_hash_map<kafka::member_id, std::unique_ptr<member>> _members;
    /// metadata snapshot of all topics subscribed to by any member, used to
    /// compute assignments and to detect metadata changes.
    absl::btree_map<model::topic, consumer_group_topic_metadata>
      _subscription_metadata;
    /// target assignment per member for _assignment_epoch.
    absl::node_hash_map<kafka::member_id, assignment_type> _target;
};

} // namespace kafka
