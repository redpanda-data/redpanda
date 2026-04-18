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

#include "absl/container/btree_map.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/reconciler/reconciler_probe.h"
#include "encryption/dek_dedup.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace cloud_topics::reconciler {

// Metadata about a range of batches consumed by a reconciliation
// consumer.
struct consumer_metadata {
    kafka::offset base_offset;
    kafka::offset last_offset;
    model::timestamp last_timestamp;
    absl::btree_map<model::term_id, kafka::offset> terms;
    uint64_t batch_count{0};
};

/// Consumes record batches from a reader and writes them to an L1 object.
/// Produces metadata about the consumed range including offsets, timestamps,
/// and term transitions. Strips duplicate DEK headers across batches within
/// the same L1 index segment to avoid redundant encryption metadata.
ss::future<std::optional<consumer_metadata>> build_from_reader(
  model::topic_id_partition,
  model::record_batch_reader,
  l1::object_builder*,
  reconciler_probe*,
  encryption::seen_dek_set& seen_deks);

} // namespace cloud_topics::reconciler
