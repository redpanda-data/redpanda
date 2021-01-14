// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/follower_stats.h"

#include <absl/container/node_hash_map.h>

namespace raft {
void follower_stats::update_with_configuration(const group_configuration& cfg) {
    cfg.for_each_broker([this](const model::broker& n) {
        if (n.id() == _self || _followers.contains(n.id())) {
            return;
        }
        _followers.emplace(n.id(), follower_index_metadata(n.id()));
    });

    absl::erase_if(_followers, [&cfg](const container_t::value_type& p) {
        return !cfg.contains_broker(p.first);
    });
}
std::ostream& operator<<(std::ostream& o, const follower_stats& s) {
    o << "{followers:" << s._followers.size() << ", [";
    for (auto& f : s) {
        o << f.second;
    }
    return o << "]}";
}

} // namespace raft
