/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/remote_topic_configuration_source.h"

#include "cloud_storage/topic_manifest.h"
#include "cluster/logger.h"
#include "config/configuration.h"
#include "s3/client.h"

namespace cluster {

remote_topic_configuration_source::remote_topic_configuration_source(
  cloud_storage::remote& remote)
  : _remote(remote) {}

ss::future<errc>
remote_topic_configuration_source::set_remote_properties_in_config(
  custom_assignable_topic_configuration& cfg,
  const s3::bucket_name& bucket,
  ss::abort_source& as) {
    cloud_storage::topic_manifest manifest;

    auto timeout
      = config::shard_local_cfg().cloud_storage_manifest_upload_timeout_ms();
    auto backoff = config::shard_local_cfg().cloud_storage_initial_backoff_ms();
    retry_chain_node rc_node(as, timeout, backoff);

    model::ns ns = cfg.cfg.tp_ns.ns;
    model::topic topic = cfg.cfg.tp_ns.tp;
    cloud_storage::remote_manifest_path key
      = cloud_storage::topic_manifest::get_topic_manifest_path(ns, topic);

    auto res = co_await _remote.download_manifest(
      bucket, key, manifest, rc_node);

    if (res != cloud_storage::download_result::success) {
        vlog(
          clusterlog.warn,
          "Could not download topic manifest {} from bucket {}: {}",
          key,
          bucket,
          res);
        co_return errc::topic_operation_error;
    }

    if (!manifest.get_topic_config()) {
        vlog(
          clusterlog.warn,
          "Topic manifest {} doesn't contain topic config",
          key);
        co_return errc::topic_operation_error;
    } else {
        cfg.cfg.properties.remote_topic_properties = remote_topic_properties(
          manifest.get_revision(),
          manifest.get_topic_config()->partition_count);
    }
    co_return errc::success;
}
} // namespace cluster
