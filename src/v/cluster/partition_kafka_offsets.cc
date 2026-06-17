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
#include "cluster/partition_kafka_offsets.h"

#include "cluster/partition.h"

#include <algorithm>

namespace cluster {

model::offset partition_kafka_start_offset(const partition& p) {
    if (p.is_read_replica_mode_enabled() && p.cloud_data_available()) {
        // Always assume remote read in this case.
        return p.start_cloud_offset();
    }
    auto local_start = p.log()->from_log_offset(p.raft_start_offset());
    if (
      p.is_remote_fetch_enabled() && p.cloud_data_available()
      && p.start_cloud_offset() < local_start) {
        return p.start_cloud_offset();
    }
    return local_start;
}

model::offset kafka_high_watermark(const partition& p) {
    if (p.is_read_replica_mode_enabled()) {
        if (p.cloud_data_available()) {
            return p.next_cloud_offset();
        } else {
            return model::offset(0);
        }
    }
    return p.log()->from_log_offset(p.high_watermark());
}

model::offset kafka_start_offset_with_override(
  const partition& p, model::offset start_override) {
    if (start_override == model::offset{}) {
        return partition_kafka_start_offset(p);
    }
    if (p.is_read_replica_mode_enabled()) {
        // The start override may fall ahead of the HWM since read replicas
        // compute HWM based on uploaded segments, and the override may
        // appear in the manifest before uploading corresponding segments.
        // Clamp down to the HWM.
        const auto hwm = kafka_high_watermark(p);
        if (hwm <= start_override) {
            return hwm;
        }
    }
    return std::max(partition_kafka_start_offset(p), start_override);
}

model::offset kafka_start_offset(const partition& p) {
    const auto override_opt = p.kafka_start_offset_override();
    return kafka_start_offset_with_override(
      p, override_opt.value_or(model::offset{}));
}

} // namespace cluster
