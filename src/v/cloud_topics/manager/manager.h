/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "cluster/notification.h"
#include "cluster/topic_configuration.h"
#include "cluster/utils/partition_change_notifier.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/metadata.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace cluster {
class partition;
class partition_manager;
class topic_table;
} // namespace cluster

namespace raft {
class group_manager;
}

namespace cloud_topics {

/*
 * Management of leadership following for various different cloud topics related
 * partitions.
 *
 * Examples
 * - L0 GC follows L1 metastore partition 0 to have a centralized location
 * - CTP partitions need reconcilers and housekeepers attached to them.
 * - L1 domains need a manager that is colocated with leaders.
 */
class cloud_topics_manager {
public:
    // The callback to be invoked when leadership for a partition changes.
    //
    // If the partition is `nullptr`, then that means the leader is no longer on
    // this core. If the partition is not `nullptr` then the leader is now
    // living on this core. Callers should be idempotent if the callback is
    // notified twice with the same state.
    using notification_cb_t = ss::noncopyable_function<void(
      const model::ntp& ntp,
      model::topic_id_partition tidp,
      ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>>&
        partition) noexcept>;

    cloud_topics_manager(
      ss::sharded<cluster::partition_manager>*,
      ss::sharded<raft::group_manager>*,
      ss::sharded<cluster::topic_table>*);

    // Register for notifications to kafka namespaced partitions which have
    // cloud topics enabled. These partitions have a l0::ctp_stm attached to
    // them. The provided callback will be invoked when
    // leadership changes on this shard for the partition. See the
    // partition_change_notifier for more details.
    //
    // This method should be called *before* start is called on the
    // cloud_topics_manager.
    //
    // The provided callback will be invoked for all
    // existing shards once `start` has been called for the
    // `cloud_topics_manager`.
    //
    // These callbacks are invoked until the cloud topics manager is stopped.
    void on_ctp_partition_leader(notification_cb_t) noexcept;

    // Register for notifications to our metastore domain partition leadership
    // changes. These partitions are colocated with a domain (which is a
    // partition of the metastore). The provided callback will be invoked when
    // leadership changes on this shard for the partition. See the
    // partition_change_notifier for more details.
    //
    // This method should be called *before* start is called on the
    // cloud_topics_manager.
    //
    // The provided callback will be invoked for all
    // existing shards once `start` has been called for the
    // `cloud_topics_manager`.
    //
    // These callbacks are invoked until the cloud topics manager is stopped.
    void on_l1_domain_leader(notification_cb_t) noexcept;

    // Start the cloud topics manager. After this is invoked, all notifications
    // will be invoked with existing leadership status. All `on_*_leader`
    // methods should be already be setup before this method is called. It's not
    // supported to register new callbacks once the manager is started.
    ss::future<> start();

    // Stop the cloud topics manager. After this point notifications will no
    // longer be invoked.
    ss::future<> stop();

private:
    void on_leadership_change(
      const model::ntp& ntp,
      const model::topic_id_partition& tidp,
      bool is_leader) noexcept;

    ss::sharded<cluster::partition_manager>* partition_manager_;
    ss::sharded<cluster::topic_table>* topic_table_;
    std::unique_ptr<cluster::partition_change_notifier> notifier_;
    std::vector<notification_cb_t> ctp_callbacks_;
    std::vector<notification_cb_t> l1_callbacks_;
    std::optional<cluster::notification_id_type> notification_;
    // In the case of a topic being deleted, we no longer have the
    // topic_id_mapping_, but we need to still emit a notification with it.
    //
    // Fix this by keeping an explicit mapping and looking it up if we can't
    // find it.
    //
    // We have to key this by ntp and not just ns_tp because we want to GC
    // entries over time.
    model::ntp_map_type<model::topic_id> topic_id_mapping_;
};

} // namespace cloud_topics
