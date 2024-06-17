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
    no_matching_manifest,
};

class partition_manifest_downloader {
public:
    partition_manifest_downloader(
      const cloud_storage_clients::bucket_name bucket,
      const remote_path_provider& path_provider,
      const model::ntp& ntp,
      model::initial_revision_id rev,
      remote& remote);
    ss::future<result<find_partition_manifest_outcome, error_outcome>>
    download_manifest(
      retry_chain_node& parent_retry,
      ss::lowres_clock::time_point deadline,
      model::timestamp_clock::duration backoff,
      partition_manifest*);
    ss::future<result<find_partition_manifest_outcome, error_outcome>>
    download_manifest(retry_chain_node& retry_node, partition_manifest*);

private:
    const cloud_storage_clients::bucket_name bucket_;
    const remote_path_provider& remote_path_provider_;
    const model::ntp ntp_;
    const model::initial_revision_id rev_;
    remote& remote_;
};

} // namespace cloud_storage
