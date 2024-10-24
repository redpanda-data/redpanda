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
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

namespace cloud_storage {

// Redpanda has supported different naming schemes for segments through its
// lifetime:
// - v24.2 and up: cluster-uuid-labeled name
// - below v24.2: hash-prefixed name
//
// Because segments are persistent state, we must be able to access older
// versions, in cases we need to read when newer manifests have not yet been
// written. This header contains methods to build paths for all versions.
//
// NOTE: the exact segment-name format is still determined by manifest state.
// As such, this class only guides the prefix, and leaves filename generation
// to callers (typically via methods in partition_manifest).

// 806a0f4a-e691-4a2b-9352-ec4b769a5e6e/kafka/panda-topic/0_123/0-100-1024-16-v1.log.16
ss::sstring labeled_segment_path(
  const remote_label& remote_label,
  const model::ntp& ntp,
  model::initial_revision_id rev,
  const segment_name& segment,
  model::term_id archiver_term);

// a1b2c3d4/kafka/panda-topic/0_123/0-100-1024-16-v1.log.16
ss::sstring prefixed_segment_path(
  const model::ntp& ntp,
  model::initial_revision_id rev,
  const segment_name& segment,
  model::term_id archiver_term);

} // namespace cloud_storage
