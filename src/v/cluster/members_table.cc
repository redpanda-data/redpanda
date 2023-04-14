// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/members_table.h"

#include "cluster/controller_snapshot.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "vassert.h"
#include "vlog.h"

#include <algorithm>
#include <vector>

namespace cluster {

const members_table::cache_t& members_table::nodes() const { return _nodes; }

std::vector<node_metadata> members_table::node_list() const {
    std::vector<node_metadata> nodes;
    nodes.reserve(_nodes.size());
    for (const auto& [_, meta] : _nodes) {
        nodes.push_back(meta);
    }
    return nodes;
}
size_t members_table::node_count() const { return _nodes.size(); }

std::vector<model::node_id> members_table::node_ids() const {
    std::vector<model::node_id> ids;
    ids.reserve(_nodes.size());
    for (const auto& [id, _] : _nodes) {
        ids.push_back(id);
    }
    return ids;
}

std::optional<std::reference_wrapper<const node_metadata>>
members_table::get_node_metadata_ref(model::node_id id) const {
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        return std::nullopt;
    }
    return std::make_optional(std::cref(it->second));
}

std::optional<model::rack_id>
members_table::get_node_rack_id(model::node_id id) const {
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        return std::nullopt;
    }
    return it->second.broker.rack();
}

std::optional<node_metadata>
members_table::get_node_metadata(model::node_id id) const {
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        return std::nullopt;
    }
    return it->second;
}
std::optional<std::reference_wrapper<const node_metadata>>
members_table::get_removed_node_metadata_ref(model::node_id id) const {
    auto it = _removed_nodes.find(id);
    if (it == _removed_nodes.end()) {
        return std::nullopt;
    }
    return std::make_optional(std::cref(it->second));
}

std::error_code members_table::apply(model::offset o, add_node_cmd cmd) {
    _version = model::revision_id(o);
    auto it = _nodes.find(cmd.value.id());
    if (it != _nodes.end()) {
        return errc::invalid_node_operation;
    }
    vlog(clusterlog.info, "adding node {}", cmd.value);
    _nodes.emplace(cmd.value.id(), node_metadata{.broker = cmd.value});

    _waiters.notify(cmd.value.id());

    notify_members_updated();
    return errc::success;
}

void members_table::set_initial_brokers(std::vector<model::broker> brokers) {
    vassert(!_nodes.empty(), "can not initialize not empty members table");
    vlog(clusterlog.info, "setting initial nodes {}", brokers);
    for (auto& b : brokers) {
        const auto id = b.id();
        _nodes.emplace(id, node_metadata{.broker = std::move(b)});
        _waiters.notify(id);
    }

    notify_members_updated();
}

std::error_code members_table::apply(model::offset o, update_node_cfg_cmd cmd) {
    _version = model::revision_id(o);
    auto it = _nodes.find(cmd.value.id());
    if (it == _nodes.end()) {
        return errc::node_does_not_exists;
    }
    vlog(clusterlog.info, "updating node configuration {}", cmd.value);
    it->second.broker = std::move(cmd.value);

    notify_members_updated();
    return errc::success;
}

std::error_code members_table::apply(model::offset o, remove_node_cmd cmd) {
    _version = model::revision_id(o);
    auto it = _nodes.find(cmd.key);
    if (it == _nodes.end()) {
        return errc::node_does_not_exists;
    }
    if (
      it->second.state.get_membership_state()
      != model::membership_state::draining) {
        return errc::invalid_node_operation;
    }

    vlog(clusterlog.info, "removing node {}", cmd.key);
    auto handle = _nodes.extract(it);

    handle.mapped().state.set_membership_state(
      model::membership_state::removed);
    _removed_nodes.insert(std::move(handle));

    notify_members_updated();
    return errc::success;
}

std::error_code
members_table::apply(model::offset version, decommission_node_cmd cmd) {
    _version = model::revision_id(version());

    if (auto it = _nodes.find(cmd.key); it != _nodes.end()) {
        auto& [id, metadata] = *it;
        if (
          metadata.state.get_membership_state()
          != model::membership_state::active) {
            return errc::invalid_node_operation;
        }
        vlog(
          clusterlog.info,
          "changing node {} membership state to: {}",
          id,
          model::membership_state::draining);
        metadata.state.set_membership_state(model::membership_state::draining);
        return errc::success;
    }
    return errc::node_does_not_exists;
}

std::error_code
members_table::apply(model::offset version, recommission_node_cmd cmd) {
    _version = model::revision_id(version());

    if (auto it = _nodes.find(cmd.key); it != _nodes.end()) {
        auto& [id, metadata] = *it;
        if (
          metadata.state.get_membership_state()
          != model::membership_state::draining) {
            return errc::invalid_node_operation;
        }
        vlog(
          clusterlog.info,
          "changing node {} membership state to: {}",
          id,
          model::membership_state::active);
        metadata.state.set_membership_state(model::membership_state::active);
        return errc::success;
    }
    return errc::node_does_not_exists;
}

std::error_code
members_table::apply(model::offset version, maintenance_mode_cmd cmd) {
    _version = model::revision_id(version());

    const auto target = _nodes.find(cmd.key);
    if (target == _nodes.end()) {
        return errc::node_does_not_exists;
    }
    auto& [id, metadata] = *target;

    // no rules to enforce when disabling maintenance mode
    const auto enable = cmd.value;
    if (!enable) {
        if (
          metadata.state.get_maintenance_state()
          == model::maintenance_state::inactive) {
            vlog(
              clusterlog.trace, "node {} already not in maintenance state", id);
            return errc::success;
        }

        vlog(clusterlog.info, "marking node {} not in maintenance state", id);
        metadata.state.set_maintenance_state(
          model::maintenance_state::inactive);
        notify_maintenance_state_change(id, model::maintenance_state::inactive);

        return errc::success;
    }

    if (_nodes.size() < 2) {
        // Maintenance mode is refused on size 1 clusters in the admin API, but
        // we might be upgrading from a version that didn't have the validation.
        vlog(
          clusterlog.info,
          "Dropping maintenance mode enable operation on single node cluster");

        // Return success to enable progress: this is a clean no-op.
        return errc::success;
    }

    if (
      metadata.state.get_maintenance_state()
      == model::maintenance_state::active) {
        vlog(clusterlog.trace, "node {} already in maintenance state", id);
        return errc::success;
    }

    /*
     * enforce one-node-at-a-time in maintenance mode rule
     */
    const auto other = std::find_if(
      _nodes.cbegin(), _nodes.cend(), [](const auto& b) {
          return b.second.state.get_maintenance_state()
                 == model::maintenance_state::active;
      });

    if (other != _nodes.cend()) {
        vlog(
          clusterlog.info,
          "cannot place node {} into maintenance mode. node {} already in "
          "maintenance mode",
          id,
          other->first);
        return errc::invalid_node_operation;
    }

    vlog(clusterlog.info, "marking node {} in maintenance state", id);

    metadata.state.set_maintenance_state(model::maintenance_state::active);

    notify_maintenance_state_change(id, model::maintenance_state::active);

    return errc::success;
}

void members_table::fill_snapshot(controller_snapshot& controller_snap) {
    auto& snap = controller_snap.members;
    for (const auto& [id, md] : _nodes) {
        snap.nodes.emplace(
          id,
          controller_snapshot_parts::members_t::node_t{
            .broker = md.broker, .state = md.state});
    }
    for (const auto& [id, md] : _removed_nodes) {
        snap.removed_nodes.emplace(
          id,
          controller_snapshot_parts::members_t::node_t{
            .broker = md.broker, .state = md.state});
    }
}

void members_table::apply_snapshot(
  model::offset snap_offset, const controller_snapshot& controller_snap) {
    _version = model::revision_id(snap_offset);

    const auto& snap = controller_snap.members;

    // update the list of brokers

    cache_t old_nodes;
    std::swap(old_nodes, _nodes);

    for (const auto& [id, node] : snap.nodes) {
        _nodes.emplace(id, node_metadata{node.broker, node.state});
        _waiters.notify(id);
    }

    _removed_nodes.clear();

    for (const auto& [id, node] : snap.removed_nodes) {
        _removed_nodes.emplace(id, node_metadata{node.broker, node.state});
    }

    // notify for changes in broker state

    auto maybe_notify = [&](const node_metadata& new_node) {
        model::node_id id = new_node.broker.id();
        auto it = old_nodes.find(id);
        if (it == old_nodes.end()) {
            return;
        }

        auto old_maintenance_state = it->second.state.get_maintenance_state();
        if (old_maintenance_state != new_node.state.get_maintenance_state()) {
            notify_maintenance_state_change(
              id, new_node.state.get_maintenance_state());
        }
    };

    for (const auto& [id, node] : _nodes) {
        maybe_notify(node);
    }
    for (const auto& [id, node] : _removed_nodes) {
        maybe_notify(node);
    }

    notify_members_updated();
}

bool members_table::contains(model::node_id id) const {
    return _nodes.contains(id);
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
        cb(node_ids());
    }
}

} // namespace cluster
