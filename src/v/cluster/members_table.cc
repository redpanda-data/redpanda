// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/members_table.h"

#include "model/metadata.h"

namespace cluster {

std::vector<broker_ptr> members_table::all_brokers() const {
    std::vector<broker_ptr> brokers;
    brokers.reserve(_brokers.size());
    std::transform(
      std::cbegin(_brokers),
      std::cend(_brokers),
      std::back_inserter(brokers),
      [](const broker_cache_t::value_type& b) { return b.second; });

    return brokers;
}
std::vector<model::node_id> members_table::all_broker_ids() const {
    std::vector<model::node_id> ids;
    ids.reserve(_brokers.size());
    std::transform(
      std::cbegin(_brokers),
      std::cend(_brokers),
      std::back_inserter(ids),
      [](const broker_cache_t::value_type& b) { return b.second->id(); });

    return ids;
}

std::optional<broker_ptr> members_table::get_broker(model::node_id id) const {
    if (auto it = _brokers.find(id); it != _brokers.end()) {
        return it->second;
    }
    return std::nullopt;
}

void members_table::update_brokers(patch<broker_ptr> patch) {
    for (auto& br : patch.additions) {
        _brokers.insert_or_assign(br->id(), br);
    }

    for (auto& br : patch.deletions) {
        _brokers.erase(br->id());
    }
}
} // namespace cluster