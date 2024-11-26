/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/state.h"

namespace datalake::coordinator {

pending_entry pending_entry::copy() const {
    return {.data = data.copy(), .added_pending_at = added_pending_at};
}

partition_state partition_state::copy() const {
    partition_state result;
    result.last_committed = last_committed;
    for (const auto& entry : pending_entries) {
        result.pending_entries.push_back(entry.copy());
    }
    return result;
}

std::ostream& operator<<(std::ostream& o, topic_state::lifecycle_state_t s) {
    switch (s) {
    case topic_state::lifecycle_state_t::live:
        return o << "live";
    case topic_state::lifecycle_state_t::closed:
        return o << "closed";
    case topic_state::lifecycle_state_t::purged:
        return o << "purged";
    }
}

topic_state topic_state::copy() const {
    topic_state result;
    result.revision = revision;
    result.pid_to_pending_files.reserve(pid_to_pending_files.size());
    for (const auto& [id, state] : pid_to_pending_files) {
        result.pid_to_pending_files[id] = state.copy();
    }
    result.lifecycle_state = lifecycle_state;
    return result;
}

std::optional<std::reference_wrapper<const partition_state>>
topics_state::partition_state(const model::topic_partition& tp) const {
    auto state_iter = topic_to_state.find(tp.topic);
    if (state_iter == topic_to_state.end()) {
        return std::nullopt;
    }
    const auto& topic_state = state_iter->second;
    auto prt_iter = topic_state.pid_to_pending_files.find(tp.partition);
    if (prt_iter == topic_state.pid_to_pending_files.end()) {
        return std::nullopt;
    }
    return prt_iter->second;
}

topics_state topics_state::copy() const {
    topics_state result;
    result.topic_to_state.reserve(topic_to_state.size());
    for (const auto& [id, state] : topic_to_state) {
        result.topic_to_state[id] = state.copy();
    }
    return result;
}

bool topic_state::has_pending_entries() const {
    for (const auto& [_, partition_state] : pid_to_pending_files) {
        if (!partition_state.pending_entries.empty()) {
            return true;
        }
    }
    return false;
}

} // namespace datalake::coordinator
