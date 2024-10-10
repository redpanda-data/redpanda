/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cloud_storage/topic_mount_handler.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/topic_mount_manifest.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

namespace cloud_storage {

std::ostream& operator<<(std::ostream& o, const topic_mount_result& r) {
    switch (r) {
    case topic_mount_result::mount_manifest_does_not_exist:
        return o << "{mount_manifest_does_not_exist}";
    case topic_mount_result::mount_manifest_not_deleted:
        return o << "{mount_manifest_not_deleted}";
    case topic_mount_result::mount_manifest_exists:
        return o << "{topic_manifest_exists}";
    case topic_mount_result::success:
        return o << "{success}";
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, const topic_unmount_result& r) {
    switch (r) {
    case topic_unmount_result::mount_manifest_not_created:
        return o << "{mount_manifest_not_created}";
    case topic_unmount_result::success:
        return o << "{success}";
    }
    return o;
}

topic_mount_handler::topic_mount_handler(
  const cloud_storage_clients::bucket_name& bucket, remote& remote)
  : _bucket(bucket)
  , _remote(remote) {}

ss::future<topic_mount_result> topic_mount_handler::check_mount(
  const topic_mount_manifest& manifest,
  const remote_path_provider& path_provider,
  retry_chain_node& parent) {
    const auto manifest_path = cloud_storage_clients::object_key{
      manifest.get_manifest_path(path_provider)};

    const auto exists_result = co_await _remote.object_exists(
      _bucket, manifest_path, parent, existence_check_type::manifest);

    // If the manifest exists, it is possible to mount the topic.
    // If the result is anything but download_result::success, it is
    // assumed that the mount manifest does not exist, or is unreachable for
    // whatever reason. In that case, it is simple enough for the user to
    // reissue a request to `mount_topic()`.
    co_return (exists_result == download_result::success)
      ? topic_mount_result::mount_manifest_exists
      : topic_mount_result::mount_manifest_does_not_exist;
}

ss::future<topic_mount_result> topic_mount_handler::commit_mount(
  const topic_mount_manifest& manifest,
  const remote_path_provider& path_provider,
  retry_chain_node& parent) {
    const auto manifest_path = cloud_storage_clients::object_key(
      manifest.get_manifest_path(path_provider));

    const auto delete_result = co_await _remote.delete_object(
      _bucket, manifest_path, parent);

    // If the upload_result is anything but upload_result::success,
    // the mount manifest was not deleted, and we cannot mount the topic.
    co_return (delete_result == upload_result::success)
      ? topic_mount_result::success
      : topic_mount_result::mount_manifest_not_deleted;
}

ss::future<topic_mount_result> topic_mount_handler::mount_topic(
  const cluster::topic_configuration& topic_cfg,
  model::initial_revision_id rev,
  bool prepare_only,
  retry_chain_node& parent) {
    const auto remote_tp_ns = topic_cfg.remote_tp_ns();
    const auto path_provider = remote_path_provider(
      topic_cfg.properties.remote_label, remote_tp_ns);
    // The default UUID (all zeros) in the case that the topic to be
    // mounted doesn't have a remote label.
    const auto manifest = topic_mount_manifest(
      topic_cfg.properties.remote_label.value_or(
        remote_label{model::default_cluster_uuid}),
      remote_tp_ns,
      rev);

    const auto check_result = co_await check_mount(
      manifest, path_provider, parent);
    if (check_result != topic_mount_result::mount_manifest_exists) {
        vlog(
          cst_log.warn,
          "Couldn't mount topic {}, check result was {}.",
          topic_cfg.tp_ns,
          check_result);
        co_return check_result;
    }

    if (prepare_only) {
        co_return check_result;
    }

    const auto commit_result = co_await commit_mount(
      manifest, path_provider, parent);

    if (commit_result != topic_mount_result::success) {
        vlog(
          cst_log.warn,
          "Couldn't mount topic {}, commit result was {}.",
          topic_cfg.tp_ns,
          commit_result);
    }

    co_return commit_result;
}

ss::future<topic_unmount_result> topic_mount_handler::unmount_topic(
  const cluster::topic_configuration& topic_cfg,
  model::initial_revision_id rev,
  retry_chain_node& parent) {
    const auto remote_tp_ns = topic_cfg.remote_tp_ns();
    const auto path_provider = remote_path_provider(
      topic_cfg.properties.remote_label, remote_tp_ns);
    // The default UUID (all zeros) in the case that the topic to be
    // unmounted doesn't have a remote label.
    const auto manifest = topic_mount_manifest(
      topic_cfg.properties.remote_label.value_or(
        remote_label{model::default_cluster_uuid}),
      remote_tp_ns,
      rev);

    const auto manifest_path = manifest.get_manifest_path(path_provider);

    // Check if manifest already exists: this means a topic of the same name and
    // initial revision id has been unmounted previously.
    const auto exists_result = co_await _remote.object_exists(
      _bucket,
      cloud_storage_clients::object_key{manifest_path},
      parent,
      existence_check_type::manifest);
    if (exists_result == download_result::success) {
        vlog(
          cst_log.warn,
          "Existing topic mount manifest during the unmount process: {}",
          manifest_path);
    }

    // Upload manifest to cloud storage to mark it as mountable.
    const auto upload_result = co_await _remote.upload_manifest(
      _bucket, manifest, manifest_path, parent);

    if (upload_result != upload_result::success) {
        vlog(
          cst_log.error,
          "Failed to unmount topic {} due to failed manifest upload",
          topic_cfg.tp_ns);
        co_return topic_unmount_result::mount_manifest_not_created;
    }

    co_return topic_unmount_result::success;
}

ss::future<topic_mount_result> topic_mount_handler::prepare_mount_topic(
  const cluster::topic_configuration& topic_cfg,
  model::initial_revision_id rev,
  retry_chain_node& parent) {
    return mount_topic(topic_cfg, rev, true, parent);
}

ss::future<topic_mount_result> topic_mount_handler::confirm_mount_topic(
  const cluster::topic_configuration& topic_cfg,
  model::initial_revision_id rev,
  retry_chain_node& parent) {
    return mount_topic(topic_cfg, rev, false, parent);
}

} // namespace cloud_storage
