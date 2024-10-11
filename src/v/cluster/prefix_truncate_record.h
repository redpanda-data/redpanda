/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "model/fundamental.h"
#include "serde/envelope.h"

namespace cluster {

// Key types for log_eviction_stm. These are placed here rather than together
// with log_eviction_stm to allow other listeners on this key type (e.g.
// archival_metadata_stm).

inline constexpr uint8_t prefix_truncate_key = 0;

// Record data to be used in model::record_batch_type::prefix_truncate batches
// (e.g. during DeleteRecords API calls).
struct prefix_truncate_record
  : public serde::envelope<
      prefix_truncate_record,
      serde::version<0>,
      serde::compat_version<0>> {
    // May be empty if offset translation state was not available when
    // replicating the batch (e.g. the corresponding local segments had been
    // GCed, but the data still exists in cloud).
    model::offset rp_start_offset{};

    // May not be empty.
    kafka::offset kafka_start_offset{};

    auto serde_fields() {
        return std::tie(rp_start_offset, kafka_start_offset);
    }
};

} // namespace cluster
