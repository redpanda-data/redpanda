/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/manager/manager.h"

#include "base/vlog.h"
#include "cloud_topics/logger.h"
#include "cluster/partition_manager.h"
#include "model/namespace.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics {

cloud_topics_manager::cloud_topics_manager(
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<raft::group_manager>* group_manager,
  ss::sharded<cluster::health_monitor_frontend>* health_monitor)
  : remote_(remote)
  , bucket_(std::move(bucket))
  , partition_manager_(partition_manager)
  , group_manager_(group_manager)
  , health_monitor_(health_monitor)
  , level_zero_gc_(remote_, bucket_, health_monitor_) {}

seastar::future<> cloud_topics_manager::start() {
    vlog(cd_log.info, "Cloud topics manager starting");

    const auto manager_ntp = model::ntp(
      model::kafka_internal_namespace,
      model::l1_metastore_topic,
      model::partition_id(0));

    manage_notifications_
      = partition_manager_->local().register_manage_notification(
        manager_ntp.ns,
        manager_ntp.tp.topic,
        [this, manager_ntp](const auto& partition) {
            if (partition->ntp() == manager_ntp) {
                start_managing(*partition);
            }
        });

    unmanage_notifications_
      = partition_manager_->local().register_unmanage_notification(
        manager_ntp.ns, manager_ntp.tp.topic, [this, manager_ntp](auto tp) {
            if (tp.partition == manager_ntp.tp.partition) {
                stop_managing(manager_ntp);
            }
        });

    leadership_notifications_
      = group_manager_->local().register_leadership_notification(
        [this, manager_ntp](
          raft::group_id group,
          model::term_id,
          std::optional<model::node_id> leader_id) {
            auto partition = partition_manager_->local().partition_for(group);
            if (partition && partition->ntp() == manager_ntp) {
                notify_leadership(partition, leader_id);
            }
        });

    vlog(cd_log.info, "Cloud topics manager started");

    co_return;
}

seastar::future<> cloud_topics_manager::stop() {
    vlog(cd_log.info, "Cloud topics manager stopping");

    if (manage_notifications_) {
        partition_manager_->local().unregister_manage_notification(
          *manage_notifications_);
    }

    if (unmanage_notifications_) {
        partition_manager_->local().unregister_unmanage_notification(
          *unmanage_notifications_);
    }

    if (leadership_notifications_) {
        group_manager_->local().unregister_leadership_notification(
          *leadership_notifications_);
    }

    co_await level_zero_gc_.shutdown();

    vlog(cd_log.info, "Cloud topics manager stopped");

    co_return;
}

void cloud_topics_manager::start_managing(cluster::partition&) {
    vlog(cd_log.info, "Cloud topics manager partition registered");
}

void cloud_topics_manager::stop_managing(const model::ntp&) {
    vlog(cd_log.info, "Cloud topics manager partition deregistered");
    level_zero_gc_.stop();
}

void cloud_topics_manager::notify_leadership(
  seastar::lw_shared_ptr<cluster::partition> partition,
  std::optional<model::node_id>) {
    if (partition->is_leader()) {
        vlog(cd_log.info, "Cloud topics manager leader starting");
        level_zero_gc_.start();
    } else {
        vlog(cd_log.info, "Cloud topics manager leader stopping");
        level_zero_gc_.stop();
    }
}

} // namespace cloud_topics
