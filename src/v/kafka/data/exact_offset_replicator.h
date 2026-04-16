/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/outcome.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "raft/replicate.h"

#include <seastar/core/abort_source.hh>

#include <optional>
#include <system_error>

namespace kafka {

/// Replicates record batches at exact Kafka offsets. Used by cluster linking
/// to replicate data from a source cluster to a target partition, ensuring
/// that each batch lands at the same offset it had on the source.
///
/// Unlike normal replication (which appends at the end of the log), this
/// interface guarantees offset-precise placement and fills any gaps between
/// non-contiguous batches with ghost batches.
///
/// Obtain via partition_proxy::make_exact_offset_replicator().
class exact_offset_replicator {
public:
    virtual ~exact_offset_replicator() noexcept = default;

    /// Replicate batches at specific Kafka offsets. Each batch is placed at
    /// the corresponding entry in expected_base_offsets. Gaps between
    /// non-contiguous offsets are filled with ghost batches. prev_log_offset
    /// is the last offset already present in the log (used for gap detection).
    virtual raft::replicate_stages replicate(
      chunked_vector<model::record_batch>,
      chunked_vector<kafka::offset> expected_base_offsets,
      std::optional<kafka::offset> prev_log_offset,
      model::timeout_clock::duration timeout,
      std::optional<std::reference_wrapper<ss::abort_source>> as = std::nullopt)
      = 0;

    /// Returns the last offset tracked by the underlying state machine.
    /// Syncs internal state before returning to ensure the value is current.
    virtual ss::future<result<kafka::offset>> get_last_offset(
      model::timeout_clock::duration sync_timeout,
      std::optional<std::reference_wrapper<ss::abort_source>> as = std::nullopt)
      = 0;

    /// Fills gaps with ghost batches so the log can be safely prefix-truncated
    /// up to new_start_offset.
    virtual ss::future<std::error_code> ensure_truncatable(
      kafka::offset new_start_offset,
      model::timeout_clock::duration timeout,
      std::optional<std::reference_wrapper<ss::abort_source>> as = std::nullopt)
      = 0;
};

} // namespace kafka
