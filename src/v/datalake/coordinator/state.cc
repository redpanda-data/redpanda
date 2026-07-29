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

#include "base/vassert.h"
#include "container/chunked_vector.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "datalake/logger.h"

#include <algorithm>
#include <optional>
#include <queue>
#include <utility>

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

topic_state topic_state::copy() const {
    topic_state result;
    result.revision = revision;
    result.pid_to_pending_files.reserve(pid_to_pending_files.size());
    for (const auto& [id, state] : pid_to_pending_files) {
        result.pid_to_pending_files[id] = state.copy();
    }
    result.lifecycle_state = lifecycle_state;
    result.total_kafka_bytes_processed = total_kafka_bytes_processed;
    result.last_committed_snapshot_id = last_committed_snapshot_id;
    return result;
}

topic_state topic_state::copy_bounded(
  size_t max_files, size_t max_bytes, bool& was_bounded) const {
    was_bounded = false;
    // Fast path: if every pending file already fits within the limit, there is
    // nothing to bound, so skip building the merge below and copy as-is.
    bool is_within_bound = [this, max_files, max_bytes] {
        size_t total_files = 0;
        size_t total_bytes = 0;
        for (const auto& [_, p_state] : pid_to_pending_files) {
            for (const auto& e : p_state.pending_entries) {
                total_files += e.data.files.size() + e.data.dlq_files.size();
                total_bytes += estimated_memory_bytes(e.data);
                if (total_files > max_files || total_bytes > max_bytes) {
                    return false;
                }
            }
        }
        return true;
    }();
    if (is_within_bound) {
        return copy();
    }
    topic_state result;
    result.revision = revision;
    result.lifecycle_state = lifecycle_state;
    result.total_kafka_bytes_processed = total_kafka_bytes_processed;
    result.last_committed_snapshot_id = last_committed_snapshot_id;

    // Go through the partitions' state, ordering by offset added to the
    // coordinator. Collect files in coordinator offset order so there's an
    // exact offset-defined cutline that can be committed to the catalog that
    // is represented by the returned topic state.
    struct cursor {
        model::partition_id pid;
        const partition_state* p;
        std::deque<pending_entry>::const_iterator iter;
        std::deque<pending_entry>::const_iterator end;
        model::offset offset() const { return iter->added_pending_at; }
    };
    auto offset_greater = [](const cursor& a, const cursor& b) {
        return a.offset() > b.offset();
    };
    std::
      priority_queue<cursor, chunked_vector<cursor>, decltype(offset_greater)>
        fronts(offset_greater);
    for (const auto& [id, p_state] : pid_to_pending_files) {
        if (!p_state.pending_entries.empty()) {
            fronts.push(
              cursor{
                .pid = id,
                .p = &p_state,
                .iter = p_state.pending_entries.begin(),
                .end = p_state.pending_entries.end()});
        }
    }
    std::optional<model::offset> accepted_up_to;
    size_t files = 0;
    size_t bytes = 0;
    while (!fronts.empty()) {
        auto cur = fronts.top();
        const auto& cur_entry = *cur.iter;
        const size_t entry_files = cur_entry.data.files.size()
                                   + cur_entry.data.dlq_files.size();
        const size_t entry_bytes = estimated_memory_bytes(cur_entry.data);
        if (
          accepted_up_to.has_value()
          // Make sure we add all files added at `accepted_up_to` (ensuring if
          // there were multiple added in the same offset we get them all),
          // even if that means going above our limits.
          && cur_entry.added_pending_at != *accepted_up_to
          && (files + entry_files > max_files || bytes + entry_bytes > max_bytes)) {
            break;
        }
        fronts.pop();
        files += entry_files;
        bytes += entry_bytes;
        accepted_up_to = cur_entry.added_pending_at;
        auto& p_result = result.pid_to_pending_files[cur.pid];
        p_result.last_committed = cur.p->last_committed;
        p_result.pending_entries.push_back(cur_entry.copy());
        if (++cur.iter != cur.end) {
            fronts.push(std::move(cur));
        }
    }
    // If we stopped with cursors still queued, the limit left entries out.
    was_bounded = !fronts.empty();
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
    result.pending_files_ = pending_files_;
    result.pending_bytes_ = pending_bytes_;
    return result;
}

void topics_state::note_added(const translated_offset_range& r) {
    pending_files_ += r.files.size() + r.dlq_files.size();
    pending_bytes_ += estimated_memory_bytes(r);
}

drift_detected topics_state::note_removed(const translated_offset_range& r) {
    const auto files = r.files.size() + r.dlq_files.size();
    const auto bytes = estimated_memory_bytes(r);
    // A wrapped total would sit above every backpressure limit and reject adds
    // for the whole coordinator until the next recompute, so clamp instead.
    if (unlikely(pending_files_ < files || pending_bytes_ < bytes)) {
        vlog(
          datalake_log.error,
          "Pending totals underflow: {} files {} bytes, removing {}/{}",
          pending_files_,
          pending_bytes_,
          files,
          bytes);
        dassert(false, "pending totals underflow");
        pending_files_ = 0;
        pending_bytes_ = 0;
        return drift_detected::yes;
    }
    pending_files_ -= files;
    pending_bytes_ -= bytes;
    return drift_detected::no;
}

std::pair<size_t, size_t> topics_state::compute_pending() const {
    size_t files = 0;
    size_t bytes = 0;
    for (const auto& [_, t_state] : topic_to_state) {
        for (const auto& [pid, p_state] : t_state.pid_to_pending_files) {
            for (const auto& e : p_state.pending_entries) {
                files += e.data.files.size() + e.data.dlq_files.size();
                bytes += estimated_memory_bytes(e.data);
            }
        }
    }
    return {files, bytes};
}

void topics_state::recompute_pending() {
    std::tie(pending_files_, pending_bytes_) = compute_pending();
}

void topics_state::dassert_pending_totals() const {
    // An update that over-counted never underflows, so it only shows up here.
    dassert(
      compute_pending() == std::make_pair(pending_files_, pending_bytes_),
      "pending totals drifted: {} files, {} bytes",
      pending_files_,
      pending_bytes_);
}

bool topic_state::has_pending_entries() const {
    for (const auto& [_, partition_state] : pid_to_pending_files) {
        if (!partition_state.pending_entries.empty()) {
            return true;
        }
    }
    return false;
}

bool topic_state::has_pending_main_entries() const {
    for (const auto& [_, partition_state] : pid_to_pending_files) {
        for (const auto& e : partition_state.pending_entries) {
            if (!e.data.files.empty()) {
                return true;
            }
        }
    }
    return false;
}

bool topic_state::has_pending_dlq_entries() const {
    for (const auto& [_, partition_state] : pid_to_pending_files) {
        for (const auto& e : partition_state.pending_entries) {
            if (!e.data.dlq_files.empty()) {
                return true;
            }
        }
    }
    return false;
}

} // namespace datalake::coordinator
