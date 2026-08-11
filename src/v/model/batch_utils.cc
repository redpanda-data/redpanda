/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "model/batch_utils.h"

namespace model {
// Explicit instantiations for the template functions:
template record_batch make_placeholder_batch<record_batch_type::ghost_batch>(
  offset start_offset, offset end_offset, term_id term);
template record_batch
make_placeholder_batch<record_batch_type::compaction_placeholder>(
  offset start_offset, offset end_offset, term_id term);

template std::vector<record_batch>
make_placeholder_batches<record_batch_type::ghost_batch>(
  offset start_offset, offset end_offset, term_id term);
template std::vector<record_batch>
make_placeholder_batches<record_batch_type::compaction_placeholder>(
  offset start_offset, offset end_offset, term_id term);
} // namespace model
