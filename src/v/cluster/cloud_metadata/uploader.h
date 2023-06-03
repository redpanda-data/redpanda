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

#include "cloud_storage_clients/types.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/types.h"
#include "seastarx.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>

namespace cloud_storage {
class remote;
} // namespace cloud_storage

namespace cluster::cloud_metadata {

class uploader {
public:
    uploader(
      model::cluster_uuid cluster_uuid,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage::remote& remote,
      consensus_ptr raft0);

    // Downloads the manifest with the highest metadata ID for the cluster. If
    // no manifest exists, creates one with a default-initialized metadata ID.
    //
    // On success, the returned manifest may be used as the basis for further
    // uploads, provided callers increment the metadata ID to avoid collisions.
    //
    // May return list_failed or download_failed, in which case callers may
    // retry later.
    ss::future<cluster_manifest_result>
    download_highest_manifest_or_create(retry_chain_node& retry_node);

    // Uploads metadata, updating the manifest as appropriate.
    //
    // Regardless of outcome, further attempts to upload in this term should be
    // called with the resulting manifest; the resulting manifest will have its
    // metadata ID incremented, ensuring further uploads get unique IDs, e.g. if
    // the error still resulted in the manifest landing in remote storage, the
    // next upload should have a different metadata ID.
    //
    // Possible error results:
    // - upload_failed: there was a physical error uploading to remote storage,
    //   callers may retry with the resulting manifest in the same term.
    // - term_has_changed: the underlying Raft replica is no longer leader or
    //   the term has changed; callers may not use the resulting manifest for
    //   subsequent calls as it may be stale, and instead should resync with
    //   download_highest_manifest_or_create() upon becoming leader.
    ss::future<error_outcome> upload_next_metadata(
      model::term_id synced_term,
      cluster_metadata_manifest& manifest,
      retry_chain_node& retry_node);

private:
    // Returns true if we're no longer the leader or the term has changed since
    // the input term.
    ss::future<bool> term_has_changed(model::term_id);

    const model::cluster_uuid _cluster_uuid;
    cloud_storage::remote& _remote;
    consensus_ptr _raft0;
    const cloud_storage_clients::bucket_name _bucket;
};

} // namespace cluster::cloud_metadata
