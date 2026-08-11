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

#include "cluster/fwd.h"
#include "model/fundamental.h"

namespace cluster {

/// \brief Effective Kafka start offset of the partition: the earliest offset a
/// consumer can actually read, accounting for cloud storage (read replica and
/// tiered fetch). Ignores any start-offset override.
///
/// These are free functions rather than members of cluster::partition because
/// they are derived purely from the partition's public state, and need to be
/// reachable both from the Kafka adapter (kafka::replicated_partition) and from
/// cluster-layer callers such as the health monitor, without putting
/// Kafka-offset semantics on the core partition class.
model::offset partition_kafka_start_offset(const partition&);

/// \brief As partition_kafka_start_offset(), but folding in an explicit Kafka
/// start-offset override (clamped down to the high watermark for read
/// replicas).
model::offset kafka_start_offset_with_override(
  const partition&, model::offset start_override);

/// \brief Effective Kafka start offset including the partition's own
/// start-offset override. Equivalent to replicated_partition::start_offset().
model::offset kafka_start_offset(const partition&);

/// \brief High watermark expressed in the Kafka offset space, accounting for
/// cloud data. For read replicas it is derived from the uploaded segments
/// (next_cloud_offset); otherwise it is high_watermark() translated from the
/// log offset space. Equivalent to replicated_partition::high_watermark().
model::offset kafka_high_watermark(const partition&);

} // namespace cluster
