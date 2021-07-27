// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/members_table.h"

#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "vlog.h"

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
        /**
         * broker properties may be updated event if it is draining partitions,
         * we have to preserve its membership_state.
         */
        auto it = _brokers.find(br->id());
        if (it != _brokers.end()) {
            auto membership_state = it->second->get_membership_state();
            it->second = br;
            // do not override membership state of brokers
            it->second->set_membership_state(membership_state);
        }
        _brokers.emplace(br->id(), br);
    }

    for (auto& br : patch.deletions) {
        _brokers.erase(br->id());
    }
}
std::error_code members_table::apply(decommission_node_cmd cmd) {
    if (auto it = _brokers.find(cmd.key); it != _brokers.end()) {
        if (
          it->second->get_membership_state()
          != model::membership_state::active) {
            return errc::invalid_node_opeartion;
        }
        vlog(
          clusterlog.info,
          "changing node {} membership state to: {}",
          it->first,
          model::membership_state::draining);
        it->second->set_membership_state(model::membership_state::draining);
        return errc::success;
    }
    return errc::node_does_not_exists;
}

std::error_code members_table::apply(recommission_node_cmd cmd) {
    if (auto it = _brokers.find(cmd.key); it != _brokers.end()) {
        if (
          it->second->get_membership_state()
          != model::membership_state::draining) {
            return errc::invalid_node_opeartion;
        }
        vlog(
          clusterlog.info,
          "changing node {} membership state to: {}",
          it->first,
          model::membership_state::active);
        it->second->set_membership_state(model::membership_state::active);
        return errc::success;
    }
    return errc::node_does_not_exists;
}

std::vector<model::node_id> members_table::get_decommissioned() const {
    std::vector<model::node_id> ret;
    for (const auto& [id, broker] : _brokers) {
        if (
          broker->get_membership_state() == model::membership_state::draining) {
            ret.push_back(id);
        }
    }
    return ret;
}

bool members_table::contains(model::node_id id) const {
    return _brokers.contains(id);
}

} // namespace cluster
