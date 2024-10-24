// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/outcome.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_label.h"
#include "cloud_storage/remote_path_provider.h"
#include "model/fundamental.h"

namespace cloud_storage {

enum class find_partition_manifest_outcome {
    success = 0,

    // There is no partition manifest for the given partition.
    no_matching_manifest,
};

// Encapsulates downloading manifests for a given partition.
//
// Partition manifests have gone through a few format/naming schemes:
// - cluster-uuid-labeled, binary format
// - hash-prefixed, binary format
// - hash-prefixed, JSON format
//
// The handling of naming scheme evolution is handled by the path provider, and
// this class focuses primarily with deserializing the manifests from the
// correct format.
class partition_manifest_downloader {
public:
    partition_manifest_downloader(
      const cloud_storage_clients::bucket_name bucket,
      const remote_path_provider& path_provider,
      const model::ntp& ntp,
      model::initial_revision_id rev,
      remote& remote);

    // Attempts to download the partition manifest, transparently checking for
    // both the binary and JSON format (if supported by the path provider).
    ss::future<result<find_partition_manifest_outcome, error_outcome>>
    download_manifest(
      retry_chain_node& parent_retry,
      ss::lowres_clock::time_point deadline,
      model::timestamp_clock::duration backoff,
      partition_manifest*);
    ss::future<result<find_partition_manifest_outcome, error_outcome>>
    download_manifest(retry_chain_node& retry_node, partition_manifest*);

    ss::future<result<find_partition_manifest_outcome, error_outcome>>
    manifest_exists(retry_chain_node& retry_node);

private:
    const cloud_storage_clients::bucket_name bucket_;
    const remote_path_provider& remote_path_provider_;
    const model::ntp ntp_;
    const model::initial_revision_id rev_;
    remote& remote_;
};

} // namespace cloud_storage
