/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/utils/partition_change_notifier_impl.h"

#include "cluster/partition_manager.h"
#include "cluster/topic_table.h"
#include "raft/group_manager.h"

#include <seastar/util/defer.hh>

namespace cluster {

partition_change_notifier_impl::partition_change_notifier_impl(
  ss::sharded<raft::group_manager>& group_mgr,
  ss::sharded<cluster::partition_manager>& partition_mgr,
  ss::sharded<cluster::topic_table>& topic_table)
  : _group_mgr(group_mgr)
  , _partition_mgr(partition_mgr)
  , _topic_table(topic_table) {}

void partition_change_notifier_impl::do_notify_call_back(
  notification_type type,
  notification_id_type id,
  const model::ntp& ntp,
  ss::lw_shared_ptr<partition> partition) {
    auto it = _notification_ids.find(id);
    if (it == _notification_ids.end()) [[unlikely]] {
        return;
    }
    std::optional<partition_state> state;
    if (partition) {
        state = partition_state{
          partition->term(),
          partition->is_leader(),
          _topic_table.local().get_topic_cfg(
            model::topic_namespace_view{partition->ntp()})};
    }
    it->second.cb(type, ntp, std::move(state));
}

notification_id_type
partition_change_notifier_impl::register_partition_notifications(
  notification_cb_t cb, notify_current_state notify_current) {
    notification_state nstate;
    auto id = _notification_id++;
    auto exceptional_cleanup = ss::defer(
      [this, &nstate] { do_unregister_partition_notifications(nstate); });
    nstate.leadership = _group_mgr.local().register_leadership_notification(
      [this, id](
        raft::group_id group,
        model::term_id,
        std::optional<model::node_id>) mutable {
          auto partition = _partition_mgr.local().partition_for(group);
          if (!partition) [[unlikely]] {
              // If the partition is not found, it means that the group is
              // being deleted concurrently, ignore
              return;
          }
          do_notify_call_back(
            notification_type::leadership_change,
            id,
            partition->ntp(),
            partition);
      });
    // kafka namespace
    nstate.kafka_replica_assigned
      = _partition_mgr.local().register_manage_notification(
        model::kafka_namespace,
        [this, id](ss::lw_shared_ptr<cluster::partition> partition) mutable {
            vassert(partition, "Invalid partition in managed notification");
            do_notify_call_back(
              notification_type::partition_replica_assigned,
              id,
              partition->ntp(),
              partition);
        });
    nstate.kafka_replica_unassigned
      = _partition_mgr.local().register_unmanage_notification(
        model::kafka_namespace,
        [this, id](model::topic_partition_view tp) mutable {
            model::ntp ntp{model::kafka_namespace, tp.topic, tp.partition};
            do_notify_call_back(
              notification_type::partition_replica_unassigned,
              id,
              ntp,
              _partition_mgr.local().get(ntp));
        });
    // kafka internal namespace
    nstate.kafka_int_replica_assigned
      = _partition_mgr.local().register_manage_notification(
        model::kafka_internal_namespace,
        [this, id](ss::lw_shared_ptr<cluster::partition> partition) mutable {
            vassert(partition, "Invalid partition in managed notification");
            do_notify_call_back(
              notification_type::partition_replica_assigned,
              id,
              partition->ntp(),
              partition);
        });
    nstate.kafka_int_replica_unassigned
      = _partition_mgr.local().register_unmanage_notification(
        model::kafka_internal_namespace,
        [this, id](model::topic_partition_view tp) mutable {
            model::ntp ntp{model::kafka_namespace, tp.topic, tp.partition};
            do_notify_call_back(
              notification_type::partition_replica_unassigned,
              id,
              ntp,
              _partition_mgr.local().get(ntp));
        });
    // partition properties changes
    nstate.properties = _topic_table.local().register_ntp_delta_notification(
      [this, id](cluster::topic_table::ntp_delta_range_t range) mutable {
          for (const auto& entry : range) {
              if (
                entry.type
                == cluster::topic_table_ntp_delta_type::properties_updated) {
                  do_notify_call_back(
                    notification_type::partition_properties_change,
                    id,
                    entry.ntp,
                    _partition_mgr.local().get(entry.ntp));
              }
          }
      });
    exceptional_cleanup.cancel();
    nstate.cb = std::move(cb);
    if (notify_current) {
        // notify about all current partitions
        for (const auto& partition :
             _partition_mgr.local().partitions() | std::views::values) {
            partition_state state{
              partition->term(),
              partition->is_leader(),
              _topic_table.local().get_topic_cfg(
                model::topic_namespace_view{partition->ntp()})};

            nstate.cb(
              notification_type::partition_replica_assigned,
              partition->ntp(),
              std::move(state));
        }
    }
    _notification_ids.emplace(id, std::move(nstate));
    return id;
}

void partition_change_notifier_impl::do_unregister_partition_notifications(
  const notification_state& ids) {
    _group_mgr.local().unregister_leadership_notification(ids.leadership);
    _partition_mgr.local().unregister_manage_notification(
      ids.kafka_replica_assigned);
    _partition_mgr.local().unregister_unmanage_notification(
      ids.kafka_replica_unassigned);
    _partition_mgr.local().unregister_manage_notification(
      ids.kafka_int_replica_assigned);
    _partition_mgr.local().unregister_unmanage_notification(
      ids.kafka_int_replica_unassigned);
    _topic_table.local().unregister_ntp_delta_notification(ids.properties);
}

void partition_change_notifier_impl::unregister_partition_notifications(
  notification_id_type id) {
    auto it = _notification_ids.find(id);
    if (it == _notification_ids.end()) [[unlikely]] {
        return;
    }
    do_unregister_partition_notifications(it->second);
    _notification_ids.erase(it);
}

std::unique_ptr<cluster::partition_change_notifier>
partition_change_notifier_impl::make_default(
  ss::sharded<raft::group_manager>& group_mgr,
  ss::sharded<cluster::partition_manager>& partition_mgr,
  ss::sharded<cluster::topic_table>& topic_table) {
    return std::make_unique<partition_change_notifier_impl>(
      group_mgr, partition_mgr, topic_table);
}
} // namespace cluster
