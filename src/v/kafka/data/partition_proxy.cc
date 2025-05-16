/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "partition_proxy.h"

#include "cluster/partition_manager.h"
#include "kafka/data/cloud_topic_partition.h"
#include "kafka/data/replicated_partition.h"

namespace kafka {

template<typename Impl, typename... Args>
partition_proxy make_with_impl(Args&&... args) {
    return partition_proxy(std::make_unique<Impl>(std::forward<Args>(args)...));
}

partition_proxy make_partition_proxy(
  const ss::lw_shared_ptr<cluster::partition>& partition,
  ss::sharded<experimental::cloud_topics::app>& ct_app) {
    if (partition->get_ntp_config().cloud_topic_enabled()) {
        if (!ct_app.local_is_initialized()) {
            throw std::runtime_error(
              "Cloud topic partition can't be created because the cloud-topics "
              "subsystem is not initialized");
        }
        return make_with_impl<cloud_topic_partition>(partition, ct_app);
    }
    return make_with_impl<replicated_partition>(partition);
}

std::optional<partition_proxy> make_partition_proxy(
  const model::ktp& ktp,
  cluster::partition_manager& cluster_pm,
  ss::sharded<experimental::cloud_topics::app>& ct_app) {
    auto partition = cluster_pm.get(ktp);
    if (partition) {
        return make_partition_proxy(partition, ct_app);
    }
    return std::nullopt;
}

std::optional<partition_proxy> make_partition_proxy(
  const model::ntp& ntp,
  cluster::partition_manager& cluster_pm,
  ss::sharded<experimental::cloud_topics::app>& ct_app) {
    auto partition = cluster_pm.get(ntp);
    if (partition) {
        return make_partition_proxy(partition, ct_app);
    }
    return std::nullopt;
}

partition_proxy make_partition_proxy(
  const ss::lw_shared_ptr<cluster::partition>& partition,
  const ss::shared_ptr<experimental::cloud_topics::api>& ct_app) {
    if (partition->get_ntp_config().cloud_topic_enabled()) {
        return make_with_impl<cloud_topic_partition>(partition, ct_app);
    }
    return make_with_impl<replicated_partition>(partition);
}

std::optional<partition_proxy> make_partition_proxy(
  const model::ntp& ntp,
  cluster::partition_manager& pm,
  const ss::shared_ptr<experimental::cloud_topics::api>& ct_app) {
    auto partition = pm.get(ntp);
    if (partition) {
        return make_partition_proxy(partition, ct_app);
    }
    return std::nullopt;
}

std::optional<partition_proxy> make_partition_proxy(
  const model::ktp& ntp,
  cluster::partition_manager& pm,
  const ss::shared_ptr<experimental::cloud_topics::api>& ct_app) {
    auto partition = pm.get(ntp);
    if (partition) {
        return make_partition_proxy(partition, ct_app);
    }
    return std::nullopt;
}

} // namespace kafka
