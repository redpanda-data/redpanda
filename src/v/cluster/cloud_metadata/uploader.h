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

#include "base/seastarx.h"
#include "cloud_storage/remote.h"
#include "cloud_storage_clients/types.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/notification.h"
#include "cluster/types.h"
#include "config/property.h"
#include "raft/notification.h"
#include "storage/fwd.h"
#include "utils/lazy_abort_source.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>

namespace cloud_storage {
class remote;
} // namespace cloud_storage

namespace raft {
class group_manager;
} // namespace raft

namespace cluster::cloud_metadata {

class offsets_upload_requestor;

// Periodically uploads cluster metadata within a given term.
//
// It is expected that this is instantiated on all controller replicas. Unlike
// other Raft-driven loops (e.g. the NTP archiver), this is not driven by
// replicating messages via Raft (e.g. archival_metadata_stm). Instead, this
// uploader uses Raft linearizable barriers to send heartbeats to followers and
// assert it is still the leader before performing operations.
//
// Since there is no Raft-replicated state machine that would be replicated on
// all nodes, only the leader uploader keeps an in-memory view of cluster
// metadata. Upon becoming leader, this view is hydrated from remote storage.
class uploader {
public:
    uploader(
      raft::group_manager& group_manager,
      storage::api& storage,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage::remote& remote,
      consensus_ptr raft0,
      cluster::topic_table& topics,
      ss::shared_ptr<offsets_upload_requestor> offsets_uploader);

    // Begins the upload loop and listens for leadership changes.
    void start();

    // Stops the upload loop.
    void stop();
    ss::future<> stop_and_wait();

    ss::future<> upload_until_abort();

    // Periodically uploads cluster metadata for as long as the local
    // controller replica is the leader.
    //
    // At most one invocation should be running at any given time.
    ss::future<> upload_until_term_change();

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

    // Uploads the controller snapshot if the local snapshot has a higher
    // offset than that referenced by the manifest.
    //
    // Possible error results:
    // - upload_failed: there was a physical error uploading to remote storage,
    //   callers may retry with the resulting manifest in the same term.
    ss::future<error_outcome> maybe_upload_controller_snapshot(
      cluster_metadata_manifest& manifest,
      lazy_abort_source& lazy_as,
      retry_chain_node& retry_node);

    auto manifest() const { return _term_manifest; }

private:
    // Returns true if we're no longer the leader or the term has changed since
    // the input term.
    ss::future<bool> term_has_changed(model::term_id);

    raft::group_manager& _group_manager;
    storage::api& _storage;
    model::cluster_uuid _cluster_uuid;
    cloud_storage::remote& _remote;
    consensus_ptr _raft0;
    cluster::topic_table& _topic_table;
    ss::shared_ptr<offsets_upload_requestor> _offsets_uploader;
    const cloud_storage_clients::bucket_name _bucket;

    config::binding<std::chrono::milliseconds> _upload_interval_ms;

    ss::gate _gate;
    ss::abort_source _as;

    raft::group_manager_notification_id _leader_cb_id{
      notification_id_type_invalid};

    // Abort source to stop sleeping if there is a term change.
    std::optional<std::reference_wrapper<ss::abort_source>> _term_as;
    std::optional<std::reference_wrapper<cluster_metadata_manifest>>
      _term_manifest;

    // Used to wait for leadership. It will be triggered by notify_leadership.
    ss::condition_variable _leader_cond;
};

} // namespace cluster::cloud_metadata
