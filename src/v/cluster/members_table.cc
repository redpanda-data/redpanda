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

#include <algorithm>
#include <vector>

namespace cluster {

std::vector<broker_ptr> members_table::all_brokers() const {
    std::vector<broker_ptr> brokers;
    brokers.reserve(_brokers.size());
    for (auto& [id, broker] : _brokers) {
        if (
          broker->get_membership_state() != model::membership_state::removed) {
            brokers.push_back(broker);
        }
    }

    return brokers;
}
std::vector<model::node_id> members_table::all_broker_ids() const {
    std::vector<model::node_id> ids;
    ids.reserve(_brokers.size());
    for (auto& [id, broker] : _brokers) {
        if (
          broker->get_membership_state() != model::membership_state::removed) {
            ids.push_back(id);
        }
    }
    return ids;
}

std::optional<broker_ptr> members_table::get_broker(model::node_id id) const {
    if (auto it = _brokers.find(id); it != _brokers.end()) {
        return it->second;
    }
    return std::nullopt;
}

void members_table::update_brokers(
  model::offset version, const std::vector<model::broker>& new_brokers) {
    _version = model::revision_id(version());

    for (auto& br : new_brokers) {
        /**
         * broker properties may be updated event if it is draining partitions,
         * we have to preserve its membership and maintenance states.
         */
        auto it = _brokers.find(br.id());
        if (it != _brokers.end()) {
            // save state from the previous version of the broker
            const auto membership_state = it->second->get_membership_state();
            const auto maintenance_state = it->second->get_maintenance_state();
            it->second = ss::make_lw_shared<model::broker>(br);

            // preserve state from previous version
            if (membership_state != model::membership_state::removed) {
                it->second->set_membership_state(membership_state);
            }
            it->second->set_maintenance_state(maintenance_state);
        } else {
            _brokers.emplace(br.id(), ss::make_lw_shared<model::broker>(br));
        }

        _waiters.notify(br.id());
    }

    for (auto& [id, br] : _brokers) {
        auto it = std::find_if(
          new_brokers.begin(),
          new_brokers.end(),
          [id = id](const model::broker& br) { return br.id() == id; });
        if (it == new_brokers.end()) {
            br->set_membership_state(model::membership_state::removed);
        }
    }
}
std::error_code
members_table::apply(model::offset version, decommission_node_cmd cmd) {
    _version = model::revision_id(version());

    if (auto it = _brokers.find(cmd.key); it != _brokers.end()) {
        if (
          it->second->get_membership_state()
          != model::membership_state::active) {
            return errc::invalid_node_operation;
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

std::error_code
members_table::apply(model::offset version, recommission_node_cmd cmd) {
    _version = model::revision_id(version());

    if (auto it = _brokers.find(cmd.key); it != _brokers.end()) {
        if (
          it->second->get_membership_state()
          != model::membership_state::draining) {
            return errc::invalid_node_operation;
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

std::error_code
members_table::apply(model::offset version, maintenance_mode_cmd cmd) {
    _version = model::revision_id(version());

    const auto target = _brokers.find(cmd.key);
    if (target == _brokers.end()) {
        return errc::node_does_not_exists;
    }

    // no rules to enforce when disabling maintenance mode
    const auto enable = cmd.value;
    if (!enable) {
        target->second->set_maintenance_state(
          model::maintenance_state::inactive);
        return errc::success;
    }

    if (
      target->second->get_maintenance_state()
      == model::maintenance_state::active) {
        return errc::success;
    }

    /*
     * enforce one-node-at-a-time in maintenance mode rule
     */
    const auto other = std::find_if(
      _brokers.cbegin(), _brokers.cend(), [](const auto& b) {
          return b.second->get_maintenance_state()
                 == model::maintenance_state::active;
      });

    if (other != _brokers.cend()) {
        vlog(
          clusterlog.info,
          "cannot place node {} into maintenance mode. node {} already in "
          "maintenance mode",
          target->first,
          other->first);
        return errc::invalid_node_operation;
    }

    vlog(
      clusterlog.info, "changing node {} to maintenance state", target->first);

    target->second->set_maintenance_state(model::maintenance_state::active);

    return errc::success;
}

bool members_table::contains(model::node_id id) const {
    return _brokers.contains(id)
           && _brokers.find(id)->second->get_membership_state()
                != model::membership_state::removed;
}

} // namespace cluster
