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

#include "cloud_storage/remote.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/topic_manifest_downloader.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/configuration.h"
#include "cluster/logger.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/timeout_clock.h"

namespace cluster {

remote_topic_configuration_source::remote_topic_configuration_source(
  cloud_storage::remote& remote)
  : _remote(remote) {}

static ss::future<errc> download_topic_manifest(
  cloud_storage::remote& remote,
  custom_assignable_topic_configuration& cfg,
  cloud_storage::topic_manifest& manifest,
  const cloud_storage_clients::bucket_name& bucket,
  ss::abort_source& as) {
    auto timeout
      = config::shard_local_cfg().cloud_storage_manifest_upload_timeout_ms();
    auto backoff = config::shard_local_cfg().cloud_storage_initial_backoff_ms();
    retry_chain_node retry_node(as);
    cloud_storage::topic_manifest_downloader dl(
      bucket,
      /*remote_label=*/std::nullopt,
      cfg.cfg.remote_tp_ns(),
      remote);
    auto deadline = model::timeout_clock::now() + timeout;
    auto download_res = co_await dl.download_manifest(
      retry_node, deadline, backoff, &manifest);
    if (download_res.has_error()) {
        co_return errc::topic_operation_error;
    }
    if (
      download_res.value()
      != cloud_storage::find_topic_manifest_outcome::success) {
        co_return errc::topic_operation_error;
    }
    co_return errc::success;
}

ss::future<errc>
remote_topic_configuration_source::set_remote_properties_in_config(
  custom_assignable_topic_configuration& cfg,
  const cloud_storage_clients::bucket_name& bucket,
  ss::abort_source& as) {
    cloud_storage::topic_manifest manifest;

    auto res = co_await download_topic_manifest(
      _remote, cfg, manifest, bucket, as);
    if (res != errc::success) {
        co_return res;
    }

    if (!manifest.get_topic_config()) {
        co_return errc::topic_operation_error;
    } else {
        const auto& dl_cfg = manifest.get_topic_config();
        cfg.cfg.properties.remote_topic_properties = remote_topic_properties(
          manifest.get_revision(), dl_cfg->partition_count);
        cfg.cfg.properties.remote_label = dl_cfg->properties.remote_label;
    }
    co_return errc::success;
}

/// If property is set in source apply it to target.
static void apply_retention_defaults(
  topic_properties& target, const topic_properties& source) {
    // If the retention properties are not set explicitly by the command we
    // should apply them from topic_manifest.
    if (!target.cleanup_policy_bitflags) {
        target.cleanup_policy_bitflags = source.cleanup_policy_bitflags;
    }
    if (!target.retention_bytes.has_optional_value()) {
        target.retention_bytes = source.retention_bytes;
    }
    if (!target.retention_duration.has_optional_value()) {
        target.retention_duration = source.retention_duration;
    }
}

ss::future<errc>
remote_topic_configuration_source::set_recovered_topic_properties(
  custom_assignable_topic_configuration& cfg,
  const cloud_storage_clients::bucket_name& bucket,
  ss::abort_source& as) {
    cloud_storage::topic_manifest manifest;

    auto res = co_await download_topic_manifest(
      _remote, cfg, manifest, bucket, as);
    if (res != errc::success) {
        co_return res;
    }

    if (!manifest.get_topic_config()) {
        co_return errc::topic_operation_error;
    } else {
        // Update all topic properties
        const auto& rc = manifest.get_topic_config();
        cfg.cfg.partition_count = rc->partition_count;
        apply_retention_defaults(cfg.cfg.properties, rc->properties);

        // Use remote_topic_properties to pass revision id from the
        // topic_manifest.json
        cfg.cfg.properties.remote_topic_properties = remote_topic_properties(
          manifest.get_revision(),
          manifest.get_topic_config()->partition_count);
        cfg.cfg.properties.remote_label = rc.value().properties.remote_label;
    }
    co_return errc::success;
}
} // namespace cluster
