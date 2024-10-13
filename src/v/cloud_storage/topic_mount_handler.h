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
#pragma once

#include "cloud_storage/fwd.h"
#include "cloud_storage/remote_label.h"
#include "cloud_storage_clients/types.h"
#include "cluster/topic_configuration.h"
#include "model/fundamental.h"
#include "utils/retry_chain_node.h"

namespace cloud_storage {

enum class topic_mount_result {
    mount_manifest_does_not_exist,
    mount_manifest_not_deleted,
    mount_manifest_exists,
    success
};

std::ostream& operator<<(std::ostream& o, const topic_mount_result& r);

enum class topic_unmount_result { mount_manifest_not_created, success };

std::ostream& operator<<(std::ostream& o, const topic_unmount_result& r);

class topic_mount_handler {
public:
    topic_mount_handler(
      const cloud_storage_clients::bucket_name& bucket, remote& remote);

    topic_mount_handler(const topic_mount_handler&) = delete;
    topic_mount_handler& operator=(const topic_mount_handler&) = delete;

    topic_mount_handler(topic_mount_handler&&) = default;
    topic_mount_handler& operator=(topic_mount_handler&&) = delete;

    ~topic_mount_handler() = default;

    // Perform the first step of mounting process by checking the topic mount
    // manifest exists.
    ss::future<topic_mount_result> prepare_mount_topic(
      const cluster::topic_configuration& topic_cfg,
      model::initial_revision_id rev,
      retry_chain_node& parent);

    // Perform the second step of mounting process by deleting the topic mount
    // manifest.
    ss::future<topic_mount_result> confirm_mount_topic(
      const cluster::topic_configuration& topic_cfg,
      model::initial_revision_id rev,
      retry_chain_node& parent);

    // Perform the unmounting process by creating the topic mount manifest.
    // topic_cfg should be the recovered topic configuration from a topic
    // manifest in cloud storage. If it has a value, the remote_label stored in
    // the topic properties is used as the "source" label. Otherwise, the
    // default uuid (all zeros) is used.
    ss::future<topic_unmount_result> unmount_topic(
      const cluster::topic_configuration& topic_cfg,
      model::initial_revision_id rev,
      retry_chain_node& parent);

private:
    // Perform the mounting process by deleting the topic mount manifest.
    // topic_cfg should be the recovered topic configuration from a topic
    // manifest in cloud storage. If it has a value, the remote_label stored in
    // the topic properties is used as the "source" label. Otherwise, the
    // default uuid (all zeros) is used.
    ss::future<topic_mount_result> mount_topic(
      const cluster::topic_configuration& topic_cfg,
      model::initial_revision_id rev,
      bool prepare_only,
      retry_chain_node& parent);

    // Check for the existence of a topic mount manifest in tiered storage.
    // If it exists, then the topic can be mounted.
    ss::future<topic_mount_result> check_mount(
      const topic_mount_manifest& manifest,
      const remote_path_provider& path_provider,
      retry_chain_node& parent);

    // Commits to the mount of the topic by deleting the topic mount manifest in
    // tiered storage.
    ss::future<topic_mount_result> commit_mount(
      const topic_mount_manifest& manifest,
      const remote_path_provider& path_provider,
      retry_chain_node& parent);

    cloud_storage_clients::bucket_name _bucket;
    remote& _remote;
};

} // namespace cloud_storage
