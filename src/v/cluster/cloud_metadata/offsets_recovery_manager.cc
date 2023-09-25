/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cloud_metadata/offsets_recovery_manager.h"

#include "cloud_storage/remote_file.h"
#include "cloud_storage/types.h"
#include "cluster/cloud_metadata/offsets_recovery_router.h"
#include "cluster/cloud_metadata/offsets_recovery_rpc_types.h"
#include "cluster/logger.h"
#include "cluster/offsets_recovery_rpc_service.h"
#include "cluster/topics_frontend.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/rm_group_frontend.h"
#include "model/namespace.h"
#include "vlog.h"

#include <seastar/core/smp.hh>
#include <seastar/util/later.hh>

#include <absl/container/node_hash_set.h>

namespace cluster::cloud_metadata {

ss::future<error_outcome> offsets_recovery_manager::recover(
  retry_chain_node& parent_retry,
  const cloud_storage_clients::bucket_name& bucket,
  std::vector<std::vector<cloud_storage::remote_segment_path>>
    snapshot_paths_per_pid) {
    vlog(
      clusterlog.info,
      "Requested recovery for {} partitions of the offsets topic",
      snapshot_paths_per_pid.size());
    retry_chain_node retry_node(&parent_retry);

    if (!_mapper.local().topic_exists()) {
        vlog(clusterlog.info, "Consumer group topic is missing, creating...");
        // XXX: validate partition count and pass in members
        auto success = co_await kafka::try_create_consumer_group_topic(
          _mapper.local(), _topics_frontend.local(), 1);
        if (!success) {
            vlog(clusterlog.error, "Failed to create consumer group topic");
            co_return error_outcome::ntp_not_found;
        }
    }
    for (size_t i = 0; i < snapshot_paths_per_pid.size(); i++) {
        offsets_recovery_request req;
        auto pid = model::partition_id{i};
        auto ntp = model::ntp{
          model::kafka_consumer_offsets_nt.ns,
          model::kafka_consumer_offsets_nt.tp,
          pid,
        };
        req.offsets_ntp = ntp;
        req.bucket = bucket;
        const auto& paths = snapshot_paths_per_pid[i];
        for (const auto& snap_path : paths) {
            req.offsets_snapshot_paths.emplace_back(snap_path());
        }
        vlog(
          clusterlog.info,
          "Sending recovery request to NTP {} for {} offsets snapshots",
          ntp,
          req.offsets_snapshot_paths.size());
        vlog(clusterlog.debug, "Sending recovery request {}", req);
        auto reply = co_await _recovery_router.local().process_or_dispatch(
          std::move(req), ntp, retry_node.get_timeout());
        if (reply.ec != cluster::errc::success) {
            co_return error_outcome::download_failed;
        }
    }
    co_return error_outcome::success;
}

} // namespace cluster::cloud_metadata
