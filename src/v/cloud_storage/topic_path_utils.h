// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "cloud_storage/remote_label.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/sstring.hh>

namespace cloud_storage {

// Redpanda has supported different formats and naming schemes for topic
// manifests through its lifetime:
// - v24.2 and up: cluster-uuid-labeled name, binary format
// - v24.1 and up: hash-prefixed name, binary format
// - below v24.1: hash-prefixed name, JSON format
//
// Because manifests are persistent state, we must be able to access older
// versions, in cases we need to read when newer manifests have not yet been
// written. This header contains methods to build paths for all versions.

// meta
ss::sstring labeled_topic_manifests_root();

// meta/kafka/panda-topic
ss::sstring labeled_topic_manifest_root(const model::topic_namespace& topic);

// meta/kafka/panda-topic/806a0f4a-e691-4a2b-9352-ec4b769a5e6e
ss::sstring labeled_topic_manifest_prefix(
  const remote_label& label, const model::topic_namespace& topic);

// meta/kafka/panda-topic/806a0f4a-e691-4a2b-9352-ec4b769a5e6e/0/topic_manifest.bin
ss::sstring labeled_topic_manifest_path(
  const remote_label& label,
  const model::topic_namespace& topic,
  model::initial_revision_id rev);

// meta/kafka/panda-topic/806a0f4a-e691-4a2b-9352-ec4b769a5e6e/0_lifecycle.bin
ss::sstring labeled_topic_lifecycle_marker_path(
  const remote_label& label,
  const model::topic_namespace& topic,
  model::initial_revision_id rev);

//[0-9a-f]0000000
chunked_vector<ss::sstring> prefixed_topic_manifests_roots();

// a0000000/meta/kafka/panda-topic
ss::sstring prefixed_topic_manifest_prefix(const model::topic_namespace& topic);

// a0000000/meta/kafka/panda-topic/topic_manifest.bin
ss::sstring
prefixed_topic_manifest_bin_path(const model::topic_namespace& topic);

// a0000000/meta/kafka/panda-topic/topic_manifest.json
ss::sstring
prefixed_topic_manifest_json_path(const model::topic_namespace& topic);

// a0000000/meta/kafka/panda-topic/0_lifecycle.bin
ss::sstring prefixed_topic_lifecycle_marker_path(
  const model::topic_namespace& topic, model::initial_revision_id rev);

// Returns the topic_namespace of the given labeled manifest path, or
// std::nullopt if the input is not a labeled manifest path.
std::optional<model::topic_namespace>
tp_ns_from_labeled_path(const std::string& path);

// Returns the topic_namespace of the given prefixed manifest path, or
// std::nullopt if the input is not a prefixed manifest path.
std::optional<model::topic_namespace>
tp_ns_from_prefixed_path(const std::string& path);

} // namespace cloud_storage
