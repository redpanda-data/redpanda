// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/bytes.h"
#include "compaction/key.h"
#include "model/record_batch_types.h"

namespace storage {

// Adds additional context (batch type & whether the owning batch is a control
// batch) to a key represented by `bytes_view`, and returns the result as a
// `compaction_key`.
compaction::compaction_key enhance_key(
  model::record_batch_type type, bool is_control_batch, bytes_view key);

} // namespace storage
