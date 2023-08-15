/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/commands.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "utils/waiter_queue.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

/// Class containing information about cluster members. The members class is
/// instantiated on each core. Cluster members updates are coming directly from
/// cluster::members_manager
class members_table {
public:
    using broker_ptr = ss::lw_shared_ptr<model::broker>;
    using broker_cache_t = absl::flat_hash_map<model::node_id, broker_ptr>;

    std::vector<broker_ptr> all_brokers() const;

    size_t all_brokers_count() const;

    std::vector<model::node_id> all_broker_ids() const;

    /// Returns single broker if exists in cache
    std::optional<broker_ptr> get_broker(model::node_id) const;

    bool contains(model::node_id) const;
    bool contains_removed(model::node_id) const;

    void update_brokers(model::offset, const std::vector<model::broker>&);

    std::error_code apply(model::offset, decommission_node_cmd);
    std::error_code apply(model::offset, recommission_node_cmd);
    std::error_code apply(model::offset, maintenance_mode_cmd);

    model::revision_id version() const { return _version; }

    ss::future<> await_membership(model::node_id id, ss::abort_source& as) {
        if (!contains(id)) {
            return _waiters.await(id, as);
        } else {
            return ss::now();
        }
    }

    using maintenance_state_cb_t = ss::noncopyable_function<void(
      model::node_id, model::maintenance_state)>;

    using members_updated_cb_t
      = ss::noncopyable_function<void(std::vector<model::node_id>)>;

    notification_id_type
      register_maintenance_state_change_notification(maintenance_state_cb_t);

    void unregister_maintenance_state_change_notification(notification_id_type);

    notification_id_type
      register_members_updated_notification(members_updated_cb_t);

    void unregister_members_updated_notification(notification_id_type);

private:
    broker_cache_t _brokers;
    model::revision_id _version;

    waiter_queue<model::node_id> _waiters;

    notification_id_type _maintenance_state_change_notification_id{0};
    std::vector<std::pair<notification_id_type, maintenance_state_cb_t>>
      _maintenance_state_change_notifications;

    notification_id_type _members_updated_notification_id{0};
    std::vector<std::pair<notification_id_type, members_updated_cb_t>>
      _members_updated_notifications;

    void
      notify_maintenance_state_change(model::node_id, model::maintenance_state);

    void notify_members_updated();
};
} // namespace cluster
