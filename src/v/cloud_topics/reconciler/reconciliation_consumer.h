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
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>

namespace cloud_topics::reconciler {

// Metadata about a range of batches consumed by a reconciliation
// consumer.
struct partition_metadata {
    kafka::offset base_offset;
    kafka::offset last_offset;
    model::timestamp base_timestamp;
    model::timestamp last_timestamp;
    absl::btree_map<model::term_id, kafka::offset> terms;
};

/// Consumes record batches from a partition and writes them to an L1 object.
/// Produces metadata about the consumed range including offsets, timestamps,
/// and term transitions.
class reconciliation_consumer {
public:
    reconciliation_consumer(
      l1::object_builder* builder, model::topic_id_partition tidp);

    ss::future<ss::stop_iteration> operator()(model::record_batch);
    std::optional<partition_metadata> end_of_stream();

private:
    l1::object_builder* _builder;
    model::topic_id_partition _tidp;
    partition_metadata _metadata;
};

} // namespace cloud_topics::reconciler
