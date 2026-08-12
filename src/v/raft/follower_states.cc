// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/follower_states.h"

#include "absl/container/node_hash_map.h"
#include "raft/group_configuration.h"

namespace raft {
void follower_states::update_with_configuration(
  const group_configuration& cfg) {
    cfg.for_each_replica([this](const vnode& rni) {
        if (rni == _self || _followers.contains(rni)) {
            return;
        }
        _followers.emplace(rni, follower_index_metadata(rni));
    });
    // update learner state
    cfg.for_each_voter([this](const vnode& rni) {
        if (rni == _self) {
            return;
        }

        auto it = _followers.find(rni);
        vassert(
          it != _followers.end(),
          "voter {} have to exists in follower stats",
          rni);
        it->second.is_learner = false;
    });

    for (auto it = _followers.begin(); it != _followers.end();) {
        // if follower is not present in configuration brake condition variable
        // and remove
        if (!cfg.contains(it->first)) {
            it->second.follower_state_change.broken();
            _followers.erase(it++);
            continue;
        }
        ++it;
    }
}

fmt::iterator follower_states::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "{{followers:{}, [", _followers.size());
    for (const auto& f : *this) {
        it = fmt::format_to(it, "{}", f.second);
    }
    return fmt::format_to(it, "]}}");
}

void follower_index_metadata::reset() {
    last_dirty_log_index = model::offset{};
    last_flushed_log_index = model::offset{};
    expected_log_end_offset = model::offset{};
    match_index = model::offset{};
    next_index = model::offset{};
    heartbeats_failed = 0;
    last_sent_seq = follower_req_seq{0};
    last_received_seq = follower_req_seq{0};
    last_successful_received_seq = follower_req_seq{0};
    inflight_append_request_count = 0;
    last_sent_protocol_meta.reset();
    lw_heartbeat_failed = false;
    follower_state_change.broadcast();
    max_cleanly_compacted_offset = {};
    coordinated_compaction_offsets_getter = std::make_unique<void_executor>();
    coordinated_compaction_offsets_sender = std::make_unique<void_executor>();
}

fmt::iterator follower_index_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{node_id: {}, last_flushed_log_index: {}, last_dirty_log_index: {}, "
      "match_index: {}, next_index: {}, expected_log_end_offset: {}, "
      "heartbeats_failed: {}, last_sent_seq: {}, last_received_seq: {}, "
      "last_successful_received_seq: {}, is_learner: {}, is_recovering: {}, "
      "max_cleanly_compacted_offset: {}}}",
      node_id,
      last_flushed_log_index,
      last_dirty_log_index,
      match_index,
      next_index,
      expected_log_end_offset,
      heartbeats_failed,
      last_sent_seq,
      last_received_seq,
      last_successful_received_seq,
      is_learner,
      is_recovering,
      max_cleanly_compacted_offset);
}

} // namespace raft
