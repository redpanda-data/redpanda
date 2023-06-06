/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "outcome.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace cloud_storage {
class remote;
} // namespace cloud_storage

class retry_chain_node;

namespace cluster::cloud_metadata {

// Downloads the manifest with the highest metadata ID for the cluster and
// returns it, or any error that may have been hit along the way.
//
// Possible error results:
// - list_failed/download_failed: there was a physical error sending requests
//   to remote storage, preventing us from returning an accurate result.
// - no_matching_metadata: we were able to list an sift through the bucket, but
//   none matches this cluster (e.g. because no uploader has previously
//   uploaded metadata).
ss::future<cluster_manifest_result> download_highest_manifest_for_cluster(
  cloud_storage::remote& remote,
  const model::cluster_uuid& cluster_uuid,
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& retry_node);

} // namespace cluster::cloud_metadata
