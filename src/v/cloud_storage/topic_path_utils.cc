// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cloud_storage/topic_path_utils.h"

#include "cloud_storage/remote_label.h"
#include "hashing/xx.h"
#include "model/fundamental.h"

namespace cloud_storage {

ss::sstring labeled_topic_manifest_root(const model::topic_namespace& topic) {
    return fmt::format("meta/{}/{}", topic.ns(), topic.tp());
}

ss::sstring labeled_topic_manifest_prefix(
  const remote_label& label, const model::topic_namespace& topic) {
    return fmt::format(
      "{}/{}", labeled_topic_manifest_root(topic), label.cluster_uuid());
}

ss::sstring labeled_topic_manifest_path(
  const remote_label& label,
  const model::topic_namespace& topic,
  model::initial_revision_id rev) {
    return fmt::format(
      "{}/{}/topic_manifest.bin",
      labeled_topic_manifest_prefix(label, topic),
      rev());
}

ss::sstring
prefixed_topic_manifest_prefix(const model::topic_namespace& topic) {
    constexpr uint32_t bitmask = 0xF0000000;
    auto path = fmt::format("{}/{}", topic.ns(), topic.tp());
    uint32_t hash = bitmask & xxhash_32(path.data(), path.size());
    return fmt::format("{:08x}/meta/{}/{}", hash, topic.ns(), topic.tp());
}

ss::sstring
prefixed_topic_manifest_bin_path(const model::topic_namespace& topic) {
    return fmt::format(
      "{}/topic_manifest.bin", prefixed_topic_manifest_prefix(topic));
}

ss::sstring
prefixed_topic_manifest_json_path(const model::topic_namespace& topic) {
    return fmt::format(
      "{}/topic_manifest.json", prefixed_topic_manifest_prefix(topic));
}

} // namespace cloud_storage
