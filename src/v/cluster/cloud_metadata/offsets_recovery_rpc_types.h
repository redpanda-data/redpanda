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

#include "cloud_storage/types.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/errc.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "serde/envelope.h"
#include "utils/fragmented_vector.h"
#include "utils/retry_chain_node.h"

namespace cluster::cloud_metadata {

// Subset of Kafka protocol structs for committed offsets.

// Committed offsets belonging to a partition.
struct offset_commit_request_partition
  : public serde::envelope<
      offset_commit_request_partition,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    model::partition_id partition_index{};
    model::offset committed_offset{};

    auto serde_fields() { return std::tie(partition_index, committed_offset); }

    friend bool operator==(
      const offset_commit_request_partition&,
      const offset_commit_request_partition&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const offset_commit_request_partition& p) {
        fmt::print(
          o,
          "{{partition_index: {}, committed_offset: {}}}",
          p.partition_index,
          p.committed_offset);
        return o;
    }
};

// Partition offsets belonging to a topic.
struct offset_commit_request_topic
  : public serde::envelope<
      offset_commit_request_topic,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    model::topic name{};
    fragmented_vector<offset_commit_request_partition> partitions{};

    auto serde_fields() { return std::tie(name, partitions); }

    offset_commit_request_topic() = default;
    offset_commit_request_topic(const offset_commit_request_topic& r)
      : name(r.name)
      , partitions(r.partitions.copy()) {}

    friend bool operator==(
      const offset_commit_request_topic&, const offset_commit_request_topic&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const offset_commit_request_topic& t) {
        fmt::print(o, "{{name: {}, partitions: {}}}", t.name, t.partitions);
        return o;
    }
};

// Topic offsets belonging to a group.
struct offset_commit_request_data
  : public serde::envelope<
      offset_commit_request_data,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    ss::sstring group_id{};
    fragmented_vector<offset_commit_request_topic> topics{};

    auto serde_fields() { return std::tie(group_id, topics); }

    offset_commit_request_data() = default;
    offset_commit_request_data(const offset_commit_request_data& r)
      : group_id(r.group_id)
      , topics(r.topics.copy()) {}
    friend bool operator==(
      const offset_commit_request_data&, const offset_commit_request_data&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const offset_commit_request_data& d) {
        fmt::print(o, "{{group_id: {}, topics: {}}}", d.group_id, d.topics);
        return o;
    }
};
// Request to restore the given groups. It is expected that each group in this
// request maps to the same offset topic partition.
struct offsets_recovery_request
  : public serde::envelope<
      offsets_recovery_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::ntp offsets_ntp;
    cloud_storage_clients::bucket_name bucket;
    std::vector<ss::sstring> offsets_snapshot_paths;

    auto serde_fields() {
        return std::tie(offsets_ntp, bucket, offsets_snapshot_paths);
    }

    friend bool
    operator==(const offsets_recovery_request&, const offsets_recovery_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const offsets_recovery_request& r) {
        fmt::print(
          o,
          "{{ntp: {}, bucket: {}, offsets_snapshot_paths: {}}}",
          r.offsets_ntp,
          r.bucket,
          r.offsets_snapshot_paths);
        return o;
    }
};

// Result of a restore request.
struct offsets_recovery_reply
  : public serde::envelope<
      offsets_recovery_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    cluster::errc ec{};

    std::vector<ss::sstring> completed_offsets_snapshot_paths;

    auto serde_fields() {
        return std::tie(ec, completed_offsets_snapshot_paths);
    }

    friend bool
    operator==(const offsets_recovery_reply&, const offsets_recovery_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const offsets_recovery_reply& r) {
        fmt::print(
          o,
          "{{ec: {}, completed_offsets_snapshot_paths: {}}}",
          r.ec,
          r.completed_offsets_snapshot_paths);
        return o;
    }
};

class offsets_recovery_requestor {
public:
    virtual ss::future<error_outcome> recover(
      retry_chain_node& parent_retry,
      const cloud_storage_clients::bucket_name& bucket,
      std::vector<std::vector<cloud_storage::remote_segment_path>>)
      = 0;
};

} // namespace cluster::cloud_metadata
