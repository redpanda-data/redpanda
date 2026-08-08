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
#include "base/vassert.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"

#include <expected>
#include <optional>
#include <string_view>

namespace kafka {

/// The partitions of one topic assigned to one member, in order of increasing
/// partition id.
using partition_set = chunked_vector<model::partition_id>;

/// What one member is assigned of one topic.
struct topic_partitions {
    model::topic_id topic;
    partition_set partitions;

    topic_partitions copy() const {
        return {.topic = topic, .partitions = partitions.copy()};
    }

    friend bool
    operator==(const topic_partitions&, const topic_partitions&) = default;
};

/// The partitions assigned to one member. `assign` appends, so the topics keep
/// the order the assignor added them until `sort` puts them in order of
/// increasing topic id. An assignment the assignor hands back is sorted.
class member_assignment {
public:
    using const_iterator = chunked_vector<topic_partitions>::const_iterator;

    member_assignment() = default;

    /// Assigns one more partition of `topic`.
    void assign(model::topic_id topic, model::partition_id);

    /// Assigns some partitions of the specified topic to this group member.
    /// This operation is sensitive to order:
    /// - topics must be assigned in order of increasing topic_id
    /// - each partition_set must be sorted by increasing partition_id
    void assign(model::topic_id topic, partition_set partitions);

    /// This member's partitions of `topic`, or nullptr if it has none.
    const partition_set* partitions_of(model::topic_id topic) const;

    /// How many partitions are assigned, across every topic.
    size_t num_partitions() const;

    bool empty() const { return _topics.empty(); }
    const_iterator begin() const { return _topics.begin(); }
    const_iterator end() const { return _topics.end(); }

    /// Puts the topics and their partitions in increasing order.
    void sort();

    member_assignment copy() const;

    friend bool
    operator==(const member_assignment&, const member_assignment&) = default;

private:
    chunked_vector<topic_partitions> _topics;
};

/// An assignment for a whole group: one entry per member, in the same order as
/// `group_spec::members()`, so entry `i` belongs to member `i`.
class group_assignment {
public:
    group_assignment() = default;

    /// An assignment of nothing to each of `members` members.
    explicit group_assignment(size_t members);

    member_assignment& operator[](size_t member) noexcept {
        vassert(
          member < _members.size(),
          "member {} of a {} member assignment",
          member,
          _members.size());
        return _members[member];
    }
    const member_assignment& operator[](size_t member) const noexcept {
        vassert(
          member < _members.size(),
          "member {} of a {} member assignment",
          member,
          _members.size());
        return _members[member];
    }

    size_t size() const { return _members.size(); }
    auto begin() const { return _members.begin(); }
    auto end() const { return _members.end(); }

    /// Puts every member's assignment in increasing order.
    void sort();

    friend bool
    operator==(const group_assignment&, const group_assignment&) = default;

private:
    chunked_vector<member_assignment> _members;
};

/// A member's subscribed topics, in order of increasing topic id.
using subscribed_topic_ids = chunked_vector<model::topic_id>;

/// One member's input to the assignor.
struct member_spec {
    member_id id;
    /// The topics this member subscribes to. The caller resolves subscription
    /// names to ids before assignment.
    subscribed_topic_ids subscribed_topics;
    /// The partitions assigned to this member in the group's *current target*
    /// assignment. Stickiness measures against that target, not against the
    /// assignment the member has reconciled to. The group assigns each
    /// partition to at most one member.
    member_assignment current_assignment;

    friend bool operator==(const member_spec&, const member_spec&) = default;
};

enum class subscription_type {
    /// All members subscribe to the same set of topics.
    homogeneous,
    /// One or more members subscribe to a different set of topics.
    heterogeneous,
};

fmt::iterator format_to(subscription_type, fmt::iterator);

/// Input to the assignor, describes a group's members, their subscriptions, and
/// their current target assignment.
class group_spec {
public:
    group_spec() = default;
    explicit group_spec(chunked_vector<member_spec> members);

    const chunked_vector<member_spec>& members() const { return _members; }

    subscription_type type() const { return _type; }

private:
    chunked_vector<member_spec> _members;
    subscription_type _type{subscription_type::homogeneous};
};

/// Supplies the partition counts an assignment needs.
class topic_describer {
public:
    virtual ~topic_describer() = default;

    /// Returns std::nullopt if topic metadata lookup fails.
    virtual std::optional<int32_t>
    num_partitions(model::topic_id topic) const = 0;
};

enum class assignor_errc {
    /// Topic metadata lookup failed for one or more subscribed topics.
    unknown_topic,
    /// Metadata lookup failed for a partition in a current assignment. The
    /// assignor fails rather than adjust to metadata that may be stale.
    unknown_partition,
    /// A current assignment is out of order, holds duplicates, or holds an
    /// invalid partition_id.
    malformed_assignment,
    /// A member's subscribed topics are out of order, or hold duplicates.
    malformed_subscription,
    /// The assignor ran out of members before it placed every partition.
    /// Well-formed input cannot produce this, so either the current assignment
    /// is inconsistent or the assignor's own arithmetic is.
    partitions_left_unassigned,
    /// The metadata returned a negative partition count for a subscribed
    /// topic.
    invalid_partition_count,
};

fmt::iterator format_to(assignor_errc, fmt::iterator);

struct assignor_error {
    assignor_errc errc;
    model::topic_id topic;
    /// Unset for `unknown_topic`, which is about a whole topic.
    std::optional<model::partition_id> partition;

    fmt::iterator format_to(fmt::iterator) const;
    friend bool
    operator==(const assignor_error&, const assignor_error&) = default;
};

using assignment_result = std::expected<group_assignment, assignor_error>;

/// Interface for a broker-side partition assignor.
///
/// A group picks its assignor by name, so a `range` assignor or a custom one
/// uses this same interface. An implementation carries no state between calls.
///
/// This interface mirrors Kafka's PartitionAssignor:
/// https://github.com/apache/kafka/blob/fce22525f74bbd7feeb4f8b34c79a215db9a74c3/group-coordinator/group-coordinator-api/src/main/java/org/apache/kafka/coordinator/group/api/assignor/PartitionAssignor.java#L22
class assignor {
public:
    virtual ~assignor() = default;

    /// The name a group selects this assignor by.
    virtual std::string_view name() const = 0;

    /// Assigns every partition of the group's subscribed topics to a member
    /// that subscribes to that topic.
    ///
    /// Preconditions:
    ///
    /// - a member's subscribed topics are in order of increasing topic id,
    ///   with no duplicate topic ids
    /// - a member's current assignment is in order of increasing topic id,
    ///   with one entry per topic
    /// - each of those topics has its partitions sorted by increasing
    ///   partition id, with no duplicate partition ids
    /// - a partition id is at least zero, and below its topic's partition
    ///   count
    /// - one member at most holds each partition
    ///
    /// A subscription reaches a group from client requests, so an
    /// implementation checks the preconditions it depends on and returns
    /// `assignor_error`.
    virtual assignment_result
    assign(const group_spec&, const topic_describer&) const = 0;
};

} // namespace kafka
