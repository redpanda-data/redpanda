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
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_label.h"
#include "cloud_storage/topic_manifest.h"
#include "model/fundamental.h"

namespace cloud_storage {

enum class find_topic_manifest_outcome {
    success = 0,
    no_matching_manifest,
    multiple_matching_manifests,
};

// Encapsulates downloading manifests for a given topic.
//
// Topic manifests have gone through a few naming schemes.
class topic_manifest_downloader {
public:
    topic_manifest_downloader(
      const cloud_storage_clients::bucket_name bucket,
      const std::optional<remote_label> remote_label,
      const model::topic_namespace topoic,
      remote& remote);

    // Attempts to download the topic manifest, transparently checking paths
    // that have been used by older versions of Redpanda if a new manifest
    // isn't found.
    ss::future<result<find_topic_manifest_outcome, error_outcome>>
    download_manifest(
      retry_chain_node& parent_retry,
      ss::lowres_clock::time_point deadline,
      model::timestamp_clock::duration backoff,
      topic_manifest*);

private:
    const cloud_storage_clients::bucket_name bucket_;
    const std::optional<remote_label> remote_label_;
    const model::topic_namespace topic_;
    remote& remote_;
};

} // namespace cloud_storage
