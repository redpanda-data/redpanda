// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/follower_stats.h"

#include "raft/group_configuration.h"

#include <absl/container/node_hash_map.h>

namespace raft {
void follower_stats::update_with_configuration(const group_configuration& cfg) {
    cfg.for_each_broker_id([this](const vnode& rni) {
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

ss::future<ssx::semaphore_units>
follower_stats::get_append_entries_unit(vnode id) {
    if (auto it = _queues.find(id); it != _queues.end()) {
        return it->second.get_append_entries_unit();
    }
    auto [it, _] = _queues.emplace(id, _max_concurrent_append_entries);

    return it->second.get_append_entries_unit();
}

void follower_stats::return_append_entries_units(vnode id) {
    if (auto it = _queues.find(id);
        it != _queues.end() && it->second.is_idle()) {
        _queues.erase(it);
    }
}

std::ostream& operator<<(std::ostream& o, const follower_stats& s) {
    o << "{followers:" << s._followers.size() << ", [";
    for (auto& f : s) {
        o << f.second;
    }
    return o << "]}";
}

} // namespace raft
