/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "model/fundamental.h"

namespace cloud_topics::read_replica {

// Metadata tracked on each replica of each partition. This is expected to be
// managed for every read replica partition on a shard, and used to query
// metastore with some bounded staleness.
struct partition_metadata {
public:
    // The time the partition began being managed by this shard. This is
    // tracked so that we can ensure we get a database snapshot later than this
    // value, ensuring the view of the partition is later than those prior from
    // other shards.
    ss::lowres_clock::time_point creation_time;

    // The last time at which this partition became a leader. This is tracked
    // so that we can ensure we get a database snapshot later than this value,
    // ensuring the view of the partition is later than those prior from
    // previous leadership.
    std::optional<ss::lowres_clock::time_point> last_leadership_time;

    model::topic_id_partition tidp;
    cloud_storage_clients::bucket_name bucket;
    // TODO: additional bucket-specific configs?

    ss::lowres_clock::time_point earliest_snapshot_time() const {
        if (last_leadership_time) {
            return std::max(creation_time, *last_leadership_time);
        }
        return creation_time;
    }
};

} // namespace cloud_topics::read_replica
