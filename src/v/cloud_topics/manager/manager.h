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

#include "cloud_topics/level_zero/gc/level_zero_gc.h"
#include "cluster/notification.h"
#include "model/fundamental.h"
#include "raft/notification.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace cluster {
class partition;
class partition_manager;
class health_monitor_frontend;
} // namespace cluster

namespace cloud_io {
class remote;
}

namespace raft {
class group_manager;
}

namespace cloud_topics {

/*
 * Management of cluster-level state and operations.
 *
 * Examples
 * - L0 GC is a global operation that spans NTPs.
 * - L1 domains are cluster-level resources
 *
 * The manager runs as a singleton attached to the partition 0 leader of the
 * domains topic. This is convenient because the manager current only implements
 * L0 GC using a strategy that is stateless. A more sophisticated L0 GC strategy
 * or L1 domain management functions will likely require us to re-home the
 * manager on top of a dedicated partition for cloud topics management state.
 */
class cloud_topics_manager {
public:
    cloud_topics_manager(
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      seastar::sharded<cluster::partition_manager>*,
      seastar::sharded<raft::group_manager>*,
      seastar::sharded<cluster::health_monitor_frontend>*);

    seastar::future<> start();
    seastar::future<> stop();

private:
    cloud_io::remote* remote_;
    cloud_storage_clients::bucket_name bucket_;
    seastar::sharded<cluster::partition_manager>* partition_manager_;
    seastar::sharded<raft::group_manager>* group_manager_;
    seastar::sharded<cluster::health_monitor_frontend>* health_monitor_;

    std::optional<cluster::notification_id_type> manage_notifications_;
    std::optional<cluster::notification_id_type> unmanage_notifications_;
    std::optional<raft::group_manager_notification_id>
      leadership_notifications_;

    void start_managing(cluster::partition&);
    void stop_managing(const model::ntp&);
    void notify_leadership(
      seastar::lw_shared_ptr<cluster::partition>,
      std::optional<model::node_id>);

    level_zero_gc level_zero_gc_;
};

} // namespace cloud_topics
