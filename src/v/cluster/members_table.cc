// Copyright 2020 Redpanda Data, Inc.
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

size_t members_table::all_brokers_count() const {
    return std::count_if(_brokers.begin(), _brokers.end(), [](auto entry) {
        return entry.second->get_membership_state()
               != model::membership_state::removed;
    });
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

    notify_members_updated();
}
std::error_code
members_table::apply(model::offset version, decommission_node_cmd cmd) {
    _version = model::revision_id(version());

    if (auto it = _brokers.find(cmd.key); it != _brokers.end()) {
        auto& [id, broker] = *it;
        if (broker->get_membership_state() != model::membership_state::active) {
            return errc::invalid_node_operation;
        }
        vlog(
          clusterlog.info,
          "changing node {} membership state to: {}",
          id,
          model::membership_state::draining);
        broker->set_membership_state(model::membership_state::draining);
        return errc::success;
    }
    return errc::node_does_not_exists;
}

std::error_code
members_table::apply(model::offset version, recommission_node_cmd cmd) {
    _version = model::revision_id(version());

    if (auto it = _brokers.find(cmd.key); it != _brokers.end()) {
        auto& [id, broker] = *it;
        if (
          broker->get_membership_state() != model::membership_state::draining) {
            return errc::invalid_node_operation;
        }
        vlog(
          clusterlog.info,
          "changing node {} membership state to: {}",
          id,
          model::membership_state::active);
        broker->set_membership_state(model::membership_state::active);
        return errc::success;
    }
    return errc::node_does_not_exists;
}

std::error_code
members_table::apply(model::offset version, maintenance_mode_cmd cmd) {
    _version = model::revision_id(version());

    const auto target = _brokers.find(cmd.key);
    if (
      target == _brokers.end()
      || target->second->get_membership_state()
           == model::membership_state::removed) {
        return errc::node_does_not_exists;
    }
    auto& [id, broker] = *target;

    // no rules to enforce when disabling maintenance mode
    const auto enable = cmd.value;
    if (!enable) {
        if (
          broker->get_maintenance_state()
          == model::maintenance_state::inactive) {
            vlog(
              clusterlog.trace, "node {} already not in maintenance state", id);
            return errc::success;
        }

        vlog(clusterlog.info, "marking node {} not in maintenance state", id);
        broker->set_maintenance_state(model::maintenance_state::inactive);
        notify_maintenance_state_change(id, model::maintenance_state::inactive);

        return errc::success;
    }

    if (_brokers.size() < 2) {
        // Maintenance mode is refused on size 1 clusters in the admin API, but
        // we might be upgrading from a version that didn't have the validation.
        vlog(
          clusterlog.info,
          "Dropping maintenance mode enable operation on single node cluster");

        // Return success to enable progress: this is a clean no-op.
        return errc::success;
    }

    if (broker->get_maintenance_state() == model::maintenance_state::active) {
        vlog(clusterlog.trace, "node {} already in maintenance state", id);
        return errc::success;
    }

    /*
     * enforce one-node-at-a-time in maintenance mode rule
     */
    const auto other = std::find_if(
      _brokers.cbegin(), _brokers.cend(), [](const auto& b) {
          return b.second->get_maintenance_state()
                   == model::maintenance_state::active
                 && b.second->get_membership_state()
                      != model::membership_state::removed;
      });

    if (other != _brokers.cend()) {
        vlog(
          clusterlog.info,
          "cannot place node {} into maintenance mode. node {} already in "
          "maintenance mode",
          id,
          other->first);
        return errc::invalid_node_operation;
    }

    vlog(clusterlog.info, "marking node {} in maintenance state", id);

    broker->set_maintenance_state(model::maintenance_state::active);

    notify_maintenance_state_change(id, model::maintenance_state::active);

    return errc::success;
}

bool members_table::contains(model::node_id id) const {
    return _brokers.contains(id)
           && _brokers.find(id)->second->get_membership_state()
                != model::membership_state::removed;
}

notification_id_type
members_table::register_maintenance_state_change_notification(
  maintenance_state_cb_t cb) {
    auto id = _maintenance_state_change_notification_id++;
    _maintenance_state_change_notifications.emplace_back(id, std::move(cb));
    return id;
}

void members_table::unregister_maintenance_state_change_notification(
  notification_id_type id) {
    auto it = std::find_if(
      _maintenance_state_change_notifications.begin(),
      _maintenance_state_change_notifications.end(),
      [id](const auto& n) { return n.first == id; });
    if (it != _maintenance_state_change_notifications.end()) {
        _maintenance_state_change_notifications.erase(it);
    }
}

void members_table::notify_maintenance_state_change(
  model::node_id node_id, model::maintenance_state ms) {
    for (const auto& [id, cb] : _maintenance_state_change_notifications) {
        cb(node_id, ms);
    }
}

notification_id_type
members_table::register_members_updated_notification(members_updated_cb_t cb) {
    auto id = _members_updated_notification_id++;
    _members_updated_notifications.emplace_back(id, std::move(cb));

    return id;
}

void members_table::unregister_members_updated_notification(
  notification_id_type id) {
    auto it = std::find_if(
      _members_updated_notifications.begin(),
      _members_updated_notifications.end(),
      [id](const auto& n) { return n.first == id; });
    if (it != _members_updated_notifications.end()) {
        _members_updated_notifications.erase(it);
    }
}

void members_table::notify_members_updated() {
    for (const auto& [id, cb] : _members_updated_notifications) {
        cb(all_broker_ids());
    }
}

} // namespace cluster
