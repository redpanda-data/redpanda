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
#include "base/format_to.h"
#include "base/seastarx.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/types.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/offset_store.h"
#include "model/fundamental.h"

#include <seastar/core/shared_ptr.hh>

#include <chrono>
#include <memory>
#include <optional>
#include <string_view>

namespace kafka {

/// Partitions grouped by topic, so a topic is named once however many of its
/// partitions one member holds. Keyed by topic id, as the assignment records
/// are, so a recreated topic is a different key.
using member_partitions
  = chunked_hash_map<model::topic_id, chunked_hash_set<model::partition_id>>;

/// A consumer group's lifecycle label.
enum class consumer_group_state {
    /// No members.
    empty,
    /// A new assignment is owed: the group epoch is ahead of the assignment
    /// epoch, so the target is being computed.
    assigning,
    /// A target is computed and the members are converging on it.
    reconciling,
    /// Every member has reached the assignment epoch.
    stable,
    /// Nominal terminal state, for parity with the classic group states.
    /// Deletion tombstones an empty group instead, so a group is never
    /// observed in it.
    dead,
};

std::string_view to_string_view(consumer_group_state);

inline fmt::iterator format_to(consumer_group_state s, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(s));
}

/// What a member asked for in its heartbeat.
struct consumer_group_subscription {
    kafka::client_id client_id;
    kafka::client_host client_host;
    std::optional<kafka::group_instance_id> instance_id;
    std::optional<model::rack_id> rack_id;
    chunked_vector<model::topic> topics;
    std::chrono::milliseconds rebalance_timeout{-1};
    /// The assignor the member asked the server to use, if it named one.
    std::optional<ss::sstring> assignor;
};

/// \brief How far a member has reconciled toward the target assignment.
///
/// A member owns a partition only once it has been assigned and the previous
/// owner has released it, so `assigned` and `pending_revocation` are disjoint.
struct consumer_group_member_assignment {
    /// Advances toward the group epoch, and equals the assignment epoch once
    /// the member is caught up.
    kafka::member_epoch epoch{0};
    /// The epoch to accept if the member never saw the last bump.
    kafka::member_epoch previous_epoch{0};
    consumer_group_member_state state{consumer_group_member_state::unknown};
    member_partitions assigned;
    member_partitions pending_revocation;
};

/// \brief One member of a consumer group.
///
/// The two halves mirror the member's two persisted records: the subscription
/// carries what the member asked for, and the assignment carries how far it
/// has reconciled toward the group's target. Each half is written, and
/// tombstoned, independently of the other.
struct consumer_group_member {
    kafka::member_id id;
    consumer_group_subscription subscription;
    consumer_group_member_assignment assignment;
};

/// \brief A consumer group, as defined by KIP-848.
///
/// The sibling of the classic `group`, for members that speak the consumer
/// rebalance protocol. Two differences shape it:
///
/// - assignment is computed by the server, so the group carries a target
///   assignment and each member's progress toward it rather than the opaque
///   bytes a client leader produced;
/// - there is no rebalance barrier. Three epochs stand in for one: the group's,
///   the one a target was computed at, and each member's. `state()` is derived
///   from them, so nothing sets it and there is no stop-the-world state.
///
/// Committed offsets and EOS live in the composed `offset_store`, which the
/// classic `group` composes too.
class consumer_group {
public:
    using members_map
      = chunked_hash_map<kafka::member_id, consumer_group_member>;

    /// Keyed independently of the members: a target is written per member id,
    /// and can outlive or precede the member's own records within a batch.
    using target_assignment_map
      = chunked_hash_map<kafka::member_id, member_partitions>;

    /// Takes the store its id's offsets live in rather than building one, so
    /// the caller decides what the offsets write through and how long they
    /// outlive the group.
    consumer_group(kafka::group_id id, ss::lw_shared_ptr<offset_store> offsets);

    consumer_group(const consumer_group&) = delete;
    consumer_group& operator=(const consumer_group&) = delete;
    consumer_group(consumer_group&&) = delete;
    consumer_group& operator=(consumer_group&&) = delete;
    ~consumer_group() noexcept = default;

    const kafka::group_id& id() const { return _id; }

    /// Bumped whenever a new assignment is owed.
    kafka::group_epoch epoch() const { return _epoch; }

    /// The group epoch the current target assignment was computed at. Behind
    /// the group epoch while a new target is owed.
    kafka::assignment_epoch assignment_epoch() const {
        return _assignment_epoch;
    }

    /// Hash over the subscribed topics' metadata, which is how a subscription
    /// change is detected.
    int64_t metadata_hash() const { return _metadata_hash; }

    const members_map& members() const { return _members; }

    /// The partitions the assignor wants each member to own.
    const target_assignment_map& target_assignment() const {
        return _target_assignment;
    }

    /// \brief The group's committed offsets and transaction state.
    ///
    /// Held rather than owned: a group id's offsets exist independently of the
    /// group, before it is created and after it is deleted, so whoever applies
    /// the records owns the store and hands the group a handle to it.
    offset_store& offsets() { return *_offset_store; }
    const offset_store& offsets() const { return *_offset_store; }

    /// \brief The group's lifecycle label, derived from the epochs.
    ///
    /// Not stored: a group has no state to set, so this is recomputed on every
    /// call from the epochs and each member's progress.
    consumer_group_state state() const;

    /// The mutators below are what applying a committed record does. Each
    /// corresponds to one of the group's persisted record types, so a caller
    /// stores what the log says rather than computing it here.

    void set_epoch(kafka::group_epoch epoch) { _epoch = epoch; }

    void set_metadata_hash(int64_t hash) { _metadata_hash = hash; }

    void set_assignment_epoch(kafka::assignment_epoch epoch) {
        _assignment_epoch = epoch;
    }

    /// Add the member, or replace it if the group already has one by that id.
    void upsert_member(consumer_group_member member);

    /// Set the member's subscription half, keeping its assignment half, and
    /// add the member if the group does not have it.
    void upsert_member_subscription(
      kafka::member_id id, consumer_group_subscription subscription);

    /// Set the member's assignment half, keeping its subscription half, and
    /// add the member if the group does not have it.
    void upsert_member_assignment(
      kafka::member_id id, consumer_group_member_assignment assignment);

    /// Applying the tombstone of the member's assignment record: the member
    /// stays, with nothing assigned and its epochs back at zero. A member the
    /// group does not have is left alone.
    void clear_member_assignment(const kafka::member_id& id);

    /// Set the member's target assignment, adding an entry if there is none.
    void set_member_target(kafka::member_id id, member_partitions target);

    /// \returns whether a target was removed.
    bool erase_member_target(const kafka::member_id& id) {
        return _target_assignment.erase(id) > 0;
    }

    /// \returns whether a member was removed.
    bool erase_member(const kafka::member_id& id) {
        return _members.erase(id) > 0;
    }

    /// Whether the group may be deleted or converted to a classic group: it
    /// has no members, and no transaction is staging offsets. Committed
    /// offsets do not count against it; a conversion preserves them.
    bool deletable() const {
        return _members.empty()
               && !_offset_store->has_transactions_in_progress();
    }

    /// Applying the group's tombstone. An offset commit that lands after this
    /// does not apply its offsets, so a deleted group cannot be resurrected by
    /// a write that was already in flight.
    void mark_removed() { _removed = true; }

    bool removed() const { return _removed; }

private:
    kafka::group_id _id;
    kafka::group_epoch _epoch{0};
    kafka::assignment_epoch _assignment_epoch{0};
    int64_t _metadata_hash{0};
    bool _removed{false};
    members_map _members;
    target_assignment_map _target_assignment;
    ss::lw_shared_ptr<offset_store> _offset_store;
};

} // namespace kafka
