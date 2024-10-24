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

#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage_clients/types.h"
#include "cluster/nt_revision.h"
#include "hashing/xx.h"

namespace cloud_storage {

// Lifecycle states of a tiered storage topic:
//
//                |--- Topic deletion with remote.delete=false
//                v
// ┌──────────┬──────►┌───────────┐
// │   Live   │       │ Offloaded │
// └────┬─────┘◄──────┴───────────┘
//      │         ^
//      │         |--- Topic recovery (by user)
//      │
//      │
//      │ Topic deletion with remote.delete=true
//      ▼
// ┌──────────┐
// │  Purging │
// └────┬─────┘
//      │
//      │
//      │ Topic purge complete (by purger)
//      ▼
// ┌──────────┐
// │  Purged  │
// └──────────┘
enum class lifecycle_status : uint8_t {
    // exists in a live redpanda cluster.
    live = 1,

    // still exists in a cluster's controller, but is in the process
    // of being cleaned up.
    purging = 2,

    // cluster has finished cleaning up, and dropped the topic from
    // its controller state.  Possible that there could be stray orphan
    // objects still to clean up.
    purged = 3,

    // Cluster has dropped local state for the topic, but not deleted
    // any data: this topic is elegible for being revive on the same or
    // another cluster.
    offloaded = 4,
};

/**
 * The remote lifecycle marker is the object storage equivalent of the
 * controllers nt_lifecycle_marker.  It is left behind after topic deletion
 * as a tombstone that enables
 *  - readers to unambiguously understand that the
 *    topic is deleted and not just missing
 *  - purgers to know that it is safe to erase stray objects within the
 *    NT that the lifecycle describes.
 *
 * For normal not-deleted topics, this marker doesn't tell you anything more
 * than the topic manifest.  The main difference between this and the topic
 * manifest is:
 * - This structure knows how to represent post-deletion states, such as
 *   a topic that has been deleted with remote.delete=false
 * - This structure is unique to the namespace-topic-revision, whereas the
 *   topic manifest omits revision in its key, so if a topic is deleted and
 *   another topic is created with the same name, the topic manifests would
 *   collide, but the lifecycle markers may coexist.
 */
struct remote_nt_lifecycle_marker
  : serde::envelope<
      remote_nt_lifecycle_marker,
      serde::version<0>,
      serde::compat_version<0>> {
    /// ID of the cluster that wrote this marker, in case multiple clusters
    /// are addressing the same bucket
    ss::sstring cluster_id;

    /// The unique identify of the topic-revision.  This will also be present
    /// in the object key, but including it in the body is convenient for
    /// readers.
    cluster::nt_revision topic;

    lifecycle_status status;

    auto serde_fields() { return std::tie(cluster_id, topic, status); }

    cloud_storage_clients::object_key
    get_key(const remote_path_provider& path_provider) {
        return cloud_storage_clients::object_key{
          path_provider.topic_lifecycle_marker_path(
            topic.nt, topic.initial_revision_id)};
    }
};

} // namespace cloud_storage

template<>
struct fmt::formatter<cloud_storage::lifecycle_status> final
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto
    format(const cloud_storage::lifecycle_status& s, FormatContext& ctx) const {
        using status = cloud_storage::lifecycle_status;
        const char* str = "unknown";
        if (s == status::live) {
            str = "live";
        } else if (s == status::purging) {
            str = "purging";
        } else if (s == status::purged) {
            str = "purged";
        } else if (s == status::offloaded) {
            str = "offloaded";
        }

        return formatter<string_view>::format(str, ctx);
    }
};
