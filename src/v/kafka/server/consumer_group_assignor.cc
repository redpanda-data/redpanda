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

#include "kafka/server/consumer_group_assignor.h"

#include "base/vassert.h"

#include <algorithm>
#include <utility>

namespace kafka {

void member_assignment::assign(
  model::topic_id topic, model::partition_id partition) {
    // An assignor assigns one topic's partitions in consecutive calls, so
    // the entry to extend is almost always the last one.
    if (!_topics.empty() && _topics.back().topic == topic) {
        _topics.back().partitions.push_back(partition);
        return;
    }
    auto found = std::ranges::find(_topics, topic, &topic_partitions::topic);
    if (found == _topics.end()) {
        _topics.push_back({.topic = topic});
        found = std::prev(_topics.end());
    }
    found->partitions.push_back(partition);
}

void member_assignment::assign(
  model::topic_id topic, partition_set partitions) {
    vassert(
      std::ranges::is_sorted(partitions),
      "{} assigned out of partition id order",
      topic);
    vassert(
      _topics.empty() || _topics.back().topic < topic,
      "{} is assigned after {}",
      topic,
      _topics.back().topic);
    _topics.push_back({.topic = topic, .partitions = std::move(partitions)});
}

const partition_set*
member_assignment::partitions_of(model::topic_id topic) const {
    auto found = std::ranges::find(_topics, topic, &topic_partitions::topic);
    return found == _topics.end() ? nullptr : &found->partitions;
}

size_t member_assignment::num_partitions() const {
    return std::ranges::fold_left(
      _topics, size_t{0}, [](size_t total, const topic_partitions& entry) {
          return total + entry.partitions.size();
      });
}

void member_assignment::sort() {
    std::ranges::sort(_topics, {}, &topic_partitions::topic);
    for (auto& [topic, partitions] : _topics) {
        std::ranges::sort(partitions);
    }
}

member_assignment member_assignment::copy() const {
    member_assignment out;
    out._topics.reserve(_topics.size());
    for (const auto& entry : _topics) {
        out._topics.push_back(entry.copy());
    }
    return out;
}

group_assignment::group_assignment(size_t members) {
    _members.reserve(members);
    while (_members.size() < members) {
        _members.push_back({});
    }
}

void group_assignment::sort() {
    std::ranges::for_each(_members, &member_assignment::sort);
}

group_spec::group_spec(chunked_vector<member_spec> members)
  : _members(std::move(members)) {
    _type = std::ranges::all_of(
              _members,
              [&](const member_spec& member) {
                  return member.subscribed_topics
                         == _members.front().subscribed_topics;
              })
              ? subscription_type::homogeneous
              : subscription_type::heterogeneous;
}

fmt::iterator format_to(subscription_type type, fmt::iterator it) {
    switch (type) {
    case subscription_type::homogeneous:
        return fmt::format_to(it, "homogeneous");
    case subscription_type::heterogeneous:
        return fmt::format_to(it, "heterogeneous");
    }
    std::unreachable();
}

fmt::iterator format_to(assignor_errc errc, fmt::iterator it) {
    switch (errc) {
    case assignor_errc::unknown_topic:
        return fmt::format_to(it, "unknown_topic");
    case assignor_errc::unknown_partition:
        return fmt::format_to(it, "unknown_partition");
    case assignor_errc::malformed_assignment:
        return fmt::format_to(it, "malformed_assignment");
    case assignor_errc::malformed_subscription:
        return fmt::format_to(it, "malformed_subscription");
    case assignor_errc::partitions_left_unassigned:
        return fmt::format_to(it, "partitions_left_unassigned");
    case assignor_errc::invalid_partition_count:
        return fmt::format_to(it, "invalid_partition_count");
    }
    std::unreachable();
}

fmt::iterator assignor_error::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "{{errc: {}, topic: {}", errc, topic);
    if (partition.has_value()) {
        it = fmt::format_to(it, ", partition: {}", *partition);
    }
    return fmt::format_to(it, "}}");
}

} // namespace kafka
