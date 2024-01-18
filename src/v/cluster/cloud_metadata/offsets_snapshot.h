/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/outcome.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "serde/envelope.h"

namespace cluster::cloud_metadata {

struct group_offsets
  : public serde::
      envelope<group_offsets, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    struct partition_offset
      : public serde::envelope<
          partition_offset,
          serde::version<0>,
          serde::compat_version<0>> {
        using rpc_adl_exempt = std::true_type;
        partition_offset(model::partition_id p, kafka::offset o)
          : partition(p)
          , offset(o) {}
        partition_offset() = default;

        model::partition_id partition;
        kafka::offset offset;

        auto serde_fields() { return std::tie(partition, offset); }
        auto operator<=>(const partition_offset&) const = default;
    };

    struct topic_partitions
      : public serde::envelope<
          topic_partitions,
          serde::version<0>,
          serde::compat_version<0>> {
        using rpc_adl_exempt = std::true_type;
        topic_partitions(model::topic t, fragmented_vector<partition_offset> ps)
          : topic(std::move(t))
          , partitions(std::move(ps)) {}
        topic_partitions() = default;

        model::topic topic;
        fragmented_vector<partition_offset> partitions;

        auto serde_fields() { return std::tie(topic, partitions); }
        friend bool operator==(const topic_partitions&, const topic_partitions&)
          = default;
    };

    // The consumer group ID.
    ss::sstring group_id;

    // Data partitions and their committed offsets.
    fragmented_vector<topic_partitions> offsets;

    auto serde_fields() { return std::tie(group_id, offsets); }

    friend bool operator==(const group_offsets&, const group_offsets&)
      = default;
};

struct group_offsets_snapshot
  : public serde::envelope<
      group_offsets_snapshot,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    // Partition ID of the offsets topic that managed these groups.
    model::partition_id offsets_topic_pid;

    // Consumer groups and their offsets.
    fragmented_vector<group_offsets> groups;

    auto serde_fields() { return std::tie(offsets_topic_pid, groups); }

    friend bool
    operator==(const group_offsets_snapshot&, const group_offsets_snapshot&)
      = default;
};

using group_offsets_snapshot_result
  = result<std::vector<group_offsets_snapshot>, error_outcome>;

} // namespace cluster::cloud_metadata
