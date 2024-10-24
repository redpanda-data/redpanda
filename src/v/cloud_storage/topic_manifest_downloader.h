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
#include "cloud_storage/types.h"
#include "model/fundamental.h"

namespace cloud_storage {

enum class find_topic_manifest_outcome {
    success = 0,

    // There are no topic manifests belonging to a given topic.
    no_matching_manifest,

    // Indicates that multiple topic manifests are found for the given topic
    // and a hint must be provided to determine winner.
    multiple_matching_manifests,
};
std::ostream& operator<<(std::ostream&, find_topic_manifest_outcome);

// Encapsulates downloading manifests for a given topic.
//
// Topic manifests have gone through a few format/naming schemes:
// - v24.2 and up: cluster-uuid-labeled, binary format
// - v24.1 and up: hash-prefixed, binary format
// - below v24.1: hash-prefixed, JSON format
//
// Precednece of manifests is reverse chronological, matching the top-down
// order of the above list.
//
// This downloader returns the topic manifest, hiding the details of this
// history from callers.
class topic_manifest_downloader {
public:
    topic_manifest_downloader(
      const cloud_storage_clients::bucket_name bucket,
      std::optional<ss::sstring> hint,
      const model::topic_namespace topic,
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

    // Attempts to scan the topic manifest root paths for any topic manifests
    // that match the given filter.
    using tp_ns_filter_t = std::function<bool(const model::topic_namespace&)>;
    static ss::future<result<find_topic_manifest_outcome, error_outcome>>
    find_manifests(
      remote& remote,
      cloud_storage_clients::bucket_name bucket,
      retry_chain_node& parent_retry,
      ss::lowres_clock::time_point deadline,
      model::timestamp_clock::duration backoff,
      std::optional<tp_ns_filter_t> tp_filter,
      chunked_vector<topic_manifest>*);

private:
    const cloud_storage_clients::bucket_name bucket_;
    const std::optional<ss::sstring> label_hint_;
    const model::topic_namespace topic_;
    remote& remote_;
};

} // namespace cloud_storage
