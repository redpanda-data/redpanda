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

#pragma once

#include "cluster/fwd.h"
#include "cluster/utils/partition_change_notifier.h"
#include "container/chunked_hash_map.h"
#include "raft/fwd.h"
#include "raft/notification.h"
#include "seastar/core/sharded.hh"

namespace cluster {

class partition_change_notifier_impl final : public partition_change_notifier {
public:
    partition_change_notifier_impl(
      ss::sharded<raft::group_manager>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<cluster::topic_table>&);

    notification_id_type register_partition_notifications(
      notification_cb_t, notify_current_state) final;

    void unregister_partition_notifications(notification_id_type) final;

    static std::unique_ptr<partition_change_notifier> make_default(
      ss::sharded<raft::group_manager>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<cluster::topic_table>&);

private:
    struct notification_state {
        raft::group_manager_notification_id leadership
          = raft::notification_id_type_invalid;
        notification_id_type kafka_replica_assigned
          = notification_id_type_invalid;
        notification_id_type kafka_replica_unassigned
          = notification_id_type_invalid;
        notification_id_type kafka_int_replica_assigned
          = notification_id_type_invalid;
        notification_id_type kafka_int_replica_unassigned
          = notification_id_type_invalid;
        notification_id_type properties = notification_id_type_invalid;
        notification_cb_t cb;
    };
    void do_notify_call_back(
      notification_type,
      notification_id_type,
      const model::ntp&,
      ss::lw_shared_ptr<partition>);
    void do_unregister_partition_notifications(const notification_state&);
    ss::sharded<raft::group_manager>& _group_mgr;
    ss::sharded<cluster::partition_manager>& _partition_mgr;
    ss::sharded<cluster::topic_table>& _topic_table;
    notification_id_type _notification_id{0};
    chunked_hash_map<notification_id_type, notification_state>
      _notification_ids;
};

} // namespace cluster
