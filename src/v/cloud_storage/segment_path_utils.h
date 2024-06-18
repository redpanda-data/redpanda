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

ss::sstring labeled_segment_path(
  const remote_label& remote_label,
  const model::ntp& ntp,
  model::initial_revision_id rev,
  const segment_name& segment,
  model::term_id archiver_term);

ss::sstring prefixed_segment_path(
  const model::ntp& ntp,
  model::initial_revision_id rev,
  const segment_name& segment,
  model::term_id archiver_term);

} // namespace cloud_storage
