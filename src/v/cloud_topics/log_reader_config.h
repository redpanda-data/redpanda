/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/format_to.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"

#include <optional>
#include <ostream>

namespace cloud_topics {

// Operates on Kafka offsets
// TODO: Add enum/flag for L0 vs L1 reader
struct cloud_topic_log_reader_config {
    cloud_topic_log_reader_config(
      kafka::offset start_offset,
      kafka::offset max_offset,
      size_t min_bytes,
      size_t max_bytes,
      std::optional<model::record_batch_type> type_filter,
      std::optional<model::timestamp> time,
      model::opt_abort_source_t as,
      model::opt_client_address_t client_addr = std::nullopt,
      bool strict_max_bytes = false)
      : start_offset(start_offset)
      , max_offset(max_offset)
      , min_bytes(min_bytes)
      , max_bytes(max_bytes)
      , type_filter(type_filter)
      , first_timestamp(time)
      , abort_source(as)
      , client_address(std::move(client_addr))
      , strict_max_bytes(strict_max_bytes) {}

    /**
     * Read offsets [start, end].
     */
    cloud_topic_log_reader_config(
      kafka::offset start_offset,
      kafka::offset max_offset,
      model::opt_abort_source_t as = std::nullopt,
      model::opt_client_address_t client_addr = std::nullopt)
      : cloud_topic_log_reader_config(
          start_offset,
          max_offset,
          0,
          std::numeric_limits<size_t>::max(),
          std::nullopt,
          std::nullopt,
          as,
          std::move(client_addr),
          false) {}

    kafka::offset start_offset;
    kafka::offset max_offset;
    size_t min_bytes;
    size_t max_bytes;

    // Batch type to filter for (i.e specified type will be the only one
    // observed in read).
    std::optional<model::record_batch_type> type_filter;

    /// \brief guarantees first_timestamp >= record_batch.first_timestamp
    /// it is the std::lower_bound
    std::optional<model::timestamp> first_timestamp;

    /// abort source for read operations
    model::opt_abort_source_t abort_source;

    model::opt_client_address_t client_address;

    // do not let the lower level readers go over budget even when that means
    // that the reader will return no batches.
    bool strict_max_bytes{false};

    bool skip_cache{false};

    // When set, the reader tolerates download_not_found (404) errors during
    // extent materialization. Failed extents are skipped and produce no
    // batches, allowing the reconciler to advance past deleted L0 objects.
    allow_materialization_failure allow_mat_failure;

    // Number of objects to look ahead when fetching object metadata from
    // the metastore. 0 (default) means no lookahead and is equivalent to 1:
    // fetch one object's metadata at a time. Values > 1 batch-fetch multiple
    // objects' metadata in a single metastore RPC.
    //
    // NB: Applies to the L1 reader only.
    size_t lookahead_objects{0};

    fmt::iterator format_to(fmt::iterator it) const;
};

}; // namespace cloud_topics
