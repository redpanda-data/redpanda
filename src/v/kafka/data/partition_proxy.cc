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

#include "cloud_topics/frontend/frontend.h"
#include "cloud_topics/read_replica/stm.h"
#include "cloud_topics/state_accessors.h"
#include "cluster/partition_manager.h"
#include "encryption/encryption_services.h"
#include "encryption/schema_resolver.h"
#include "kafka/data/cloud_topic_partition.h"
#include "kafka/data/cloud_topic_read_replica.h"
#include "kafka/data/encrypting_partition_proxy.h"
#include "kafka/data/replicated_partition.h"

namespace kafka {

template<typename Impl, typename... Args>
partition_proxy make_with_impl(Args&&... args) {
    return partition_proxy(std::make_unique<Impl>(std::forward<Args>(args)...));
}

partition_proxy
make_partition_proxy(const ss::lw_shared_ptr<cluster::partition>& partition) {
    auto is_ct = partition->get_ntp_config().cloud_topic_enabled();
    auto is_rr = partition->is_read_replica_mode_enabled();
    if (is_ct) {
        auto ct_state = partition->get_cloud_topics_state();
        if (!ct_state || !ct_state->local_is_initialized()) {
            throw std::runtime_error(
              "Cloud topic partition can't be created because the cloud-topics "
              "subsystem is not initialized");
        }

        // Check for read replica first (before regular cloud topic)
        if (is_rr) {
            auto& stm_manager = partition->raft()->stm_manager();
            auto stm = stm_manager->get<cloud_topics::read_replica::stm>();
            if (!stm) {
                throw std::runtime_error("Read replica partition missing STM");
            }

            return make_with_impl<cloud_topics::read_replica::partition_proxy>(
              partition, stm, &ct_state->local());
        }

        auto frontend_instance = std::make_unique<cloud_topics::frontend>(
          partition, ct_state->local().get_data_plane());
        return make_with_impl<cloud_topic_partition>(
          partition, std::move(frontend_instance));
    }
    return make_with_impl<replicated_partition>(partition);
}

std::optional<partition_proxy> make_partition_proxy(
  const model::ktp& ktp, cluster::partition_manager& cluster_pm) {
    auto partition = cluster_pm.get(ktp);
    if (partition) {
        return make_partition_proxy(partition);
    }
    return std::nullopt;
}

std::optional<partition_proxy> make_partition_proxy(
  const model::ntp& ntp, cluster::partition_manager& cluster_pm) {
    auto partition = cluster_pm.get(ntp);
    if (partition) {
        return make_partition_proxy(partition);
    }
    return std::nullopt;
}

partition_proxy make_partition_proxy(
  const ss::lw_shared_ptr<cluster::partition>& partition,
  encryption::encryption_services* enc) {
    auto proxy = make_partition_proxy(partition);
    if (!enc) {
        return proxy;
    }
    // Always wrap when encryption services are available. The encrypting
    // proxy's encrypt_batch() resolves rules asynchronously and returns
    // the batch unchanged if no encryption annotations exist for the topic.
    auto inner = std::move(proxy).release_impl();
    return partition_proxy(
      std::make_unique<encrypting_partition_proxy>(
        std::move(inner), enc->resolver, enc->dek_mgr, enc->transformer));
}

std::optional<partition_proxy> make_partition_proxy(
  const model::ntp& ntp,
  cluster::partition_manager& mgr,
  encryption::encryption_services* enc) {
    auto partition = mgr.get(ntp);
    if (!partition) {
        return std::nullopt;
    }
    return make_partition_proxy(partition, enc);
}

} // namespace kafka
