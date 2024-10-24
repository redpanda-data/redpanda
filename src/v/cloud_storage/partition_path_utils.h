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
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/sstring.hh>

namespace cloud_storage {

// Redpanda has supported different formats and naming schemes for partition
// manifests through its lifetime:
// - v24.2 and up: cluster-uuid-labeled name, binary format
// - v23.2 and up: hash-prefixed name, binary format
// - below v23.2: hash-prefixed name, JSON format
//
// Because manifests are persistent state, we must be able to access older
// versions, in cases we need to read when newer manifests have not yet been
// written. This header contains methods to build paths for all versions.

// 806a0f4a-e691-4a2b-9352-ec4b769a5e6e/meta/kafka/panda-topic/0_123
ss::sstring labeled_partition_manifest_prefix(
  const remote_label& cluster_hint,
  const model::ntp& ntp,
  model::initial_revision_id rev);

// 806a0f4a-e691-4a2b-9352-ec4b769a5e6e/meta/kafka/panda-topic/0_123/manifest.bin
ss::sstring labeled_partition_manifest_path(
  const remote_label& cluster_hint,
  const model::ntp& ntp,
  model::initial_revision_id rev);

// a0000000/meta/kafka/panda-topic/0_123
ss::sstring prefixed_partition_manifest_prefix(
  const model::ntp& ntp, model::initial_revision_id rev);

// a0000000/meta/kafka/panda-topic/0_123/manifest.bin
ss::sstring prefixed_partition_manifest_bin_path(
  const model::ntp& ntp, model::initial_revision_id rev);

// a0000000/meta/kafka/panda-topic/0_123/manifest.json
ss::sstring prefixed_partition_manifest_json_path(
  const model::ntp& ntp, model::initial_revision_id rev);

} // namespace cloud_storage
