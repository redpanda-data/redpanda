/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/state_update.h"

#include "datalake/coordinator/state.h"
#include "datalake/coordinator/translated_offset_range.h"

#include <iterator>

namespace datalake::coordinator {

std::ostream& operator<<(std::ostream& o, const update_key& u) {
    switch (u) {
    case update_key::add_files:
        return o << "update_key::add_files";
    case update_key::mark_files_committed:
        return o << "update_key::mark_files_committed";
    }
}

checked<add_files_update, stm_update_error> add_files_update::build(
  const topics_state& state,
  const model::topic_partition& tp,
  model::revision_id topic_revision,
  chunked_vector<translated_offset_range> entries) {
    add_files_update update{
      .tp = tp,
      .topic_revision = topic_revision,
      .entries = std::move(entries),
    };
    auto allowed = update.can_apply(state);
    if (allowed.has_error()) {
        return allowed.error();
    }
    return update;
}

checked<std::nullopt_t, stm_update_error>
add_files_update::can_apply(const topics_state& state) {
    if (entries.empty()) {
        return stm_update_error{"No entries requested"};
    }
    auto topic_it = state.topic_to_state.find(tp.topic);
    if (topic_it == state.topic_to_state.end()) {
        return std::nullopt;
    }
    auto& cur_topic = topic_it->second;
    if (topic_revision < cur_topic.revision) {
        return stm_update_error{fmt::format(
          "topic {} rev {} not yet registered (current rev {})",
          tp.topic,
          topic_revision,
          cur_topic.revision)};
    } else if (topic_revision > cur_topic.revision) {
        // We are ready to accept files for an instance with the higher revision
        // id.
        return std::nullopt;
    }

    auto partition_it = topic_it->second.pid_to_pending_files.find(
      tp.partition);
    if (partition_it == topic_it->second.pid_to_pending_files.end()) {
        return std::nullopt;
    }
    const auto& prt_state = partition_it->second;

    if (
      prt_state.pending_entries.empty()
      && !prt_state.last_committed.has_value()) {
        // No entries at all, this partition hasn't ever added any files.
        return std::nullopt;
    }
    auto last_added_offset
      = prt_state.pending_entries.empty()
          ? prt_state.last_committed.value()
          : kafka::offset{prt_state.pending_entries.back().data.last_offset()};
    auto update_range_start_offset = entries.begin()->start_offset;
    if (kafka::next_offset(last_added_offset) == update_range_start_offset) {
        // The last offset for the partition aligns exactly with where we're
        // adding new data.
        return std::nullopt;
    }
    // Misalignment, likely because the current `state` doesn't match the state
    // the update was built with.
    return stm_update_error{fmt::format(
      "Last added offset {} is not contiguous with requested next offset {}",
      last_added_offset,
      update_range_start_offset)};
}

checked<std::nullopt_t, stm_update_error>
add_files_update::apply(topics_state& state, model::offset applied_offset) {
    auto allowed = can_apply(state);
    if (allowed.has_error()) {
        return allowed.error();
    }
    const auto& topic = tp.topic;
    const auto& pid = tp.partition;

    auto& tp_state = state.topic_to_state[topic];
    if (topic_revision > tp_state.revision) {
        // We've got files for a topic instance with higher revision id, reset
        // topic state
        topic_state new_state;
        new_state.revision = topic_revision;
        tp_state = std::move(new_state);
    }
    // after this point tp_state.revision == topic_revision
    auto& partition_state = tp_state.pid_to_pending_files[pid];
    for (auto& e : entries) {
        partition_state.pending_entries.emplace_back(pending_entry{
          .data = std::move(e),
          .added_pending_at = applied_offset,
        });
    }
    return std::nullopt;
}

checked<mark_files_committed_update, stm_update_error>
mark_files_committed_update::build(
  const topics_state& state,
  const model::topic_partition& tp,
  model::revision_id topic_revision,
  kafka::offset o) {
    mark_files_committed_update update{
      .tp = tp,
      .topic_revision = topic_revision,
      .new_committed = o,
    };
    auto allowed = update.can_apply(state);
    if (allowed.has_error()) {
        return allowed.error();
    }
    return update;
}

checked<std::nullopt_t, stm_update_error>
mark_files_committed_update::can_apply(const topics_state& state) {
    auto topic_it = state.topic_to_state.find(tp.topic);
    if (topic_it == state.topic_to_state.end()) {
        return stm_update_error{fmt::format(
          "topic {} rev {} not yet registered", tp.topic, topic_revision)};
    }
    const auto& cur_topic = topic_it->second;
    if (topic_revision != cur_topic.revision) {
        return stm_update_error{fmt::format(
          "topic {} revision mismatch: got {}, current rev {}",
          tp.topic,
          topic_revision,
          cur_topic.revision)};
    }

    auto partition_it = cur_topic.pid_to_pending_files.find(tp.partition);
    if (
      partition_it == cur_topic.pid_to_pending_files.end()
      || partition_it->second.pending_entries.empty()) {
        return stm_update_error{
          "Can't mark files committed if there are no files"};
    }
    const auto& prt_state = partition_it->second;
    if (
      prt_state.last_committed.has_value()
      && prt_state.last_committed.value() >= new_committed) {
        // The state already has committed up to the given offset.
        return stm_update_error{fmt::format(
          "The state has committed up to {} >= requested offset {}",
          prt_state.last_committed.value(),
          new_committed)};
    }
    // At this point, the desired offset looks okay. Examine the entries to
    // make sure the new committed offset corresponds to one of them.
    for (const auto& entry_state : prt_state.pending_entries) {
        if (entry_state.data.last_offset == new_committed) {
            return std::nullopt;
        }
    }
    return stm_update_error{fmt::format(
      "The state does not have an entry ending in offset {}", new_committed)};
}

checked<std::nullopt_t, stm_update_error>
mark_files_committed_update::apply(topics_state& state) {
    auto allowed = can_apply(state);
    if (allowed.has_error()) {
        return allowed.error();
    }
    const auto& topic = tp.topic;
    const auto& pid = tp.partition;

    // Mark all files that fall entirely below `new_committed` as committed.
    auto& files_state = state.topic_to_state[topic].pid_to_pending_files[pid];
    while (!files_state.pending_entries.empty()
           && files_state.pending_entries.front().data.last_offset
                <= new_committed) {
        files_state.pending_entries.pop_front();
    }
    files_state.last_committed = new_committed;
    return std::nullopt;
}

} // namespace datalake::coordinator
