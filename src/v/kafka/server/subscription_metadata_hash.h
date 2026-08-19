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

#include "hashing/xx.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <absl/container/btree_map.h>

#include <cstdint>
#include <span>
#include <string_view>

namespace kafka {

/// Input to subscription_metadata_hash.
/// We use an ordered container so the hash remains stable across invocations.
using topic_metadata_hashes = absl::btree_map<model::topic_id, int64_t>;

namespace detail {
template<typename T>
concept OrdersByKey = requires { typename T::key_compare; };
} // namespace detail

static_assert(
  detail::OrdersByKey<topic_metadata_hashes>,
  "subscription_metadata_hash reads iteration order, so this map must order by "
  "key");

/// Hashes one topic's metadata over the same inputs as Kafka's
/// Utils.computeTopicHash
/// https://github.com/apache/kafka/blob/fce22525f7/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/Utils.java#L491-L522
///
/// Hashes:
/// - version byte (byte)
/// - topic id (16 bytes)
/// - topic name (string)
/// - partition count (int32)
/// - for each partition, in ascending order of partition id:
///   - partition id (int32)
///   - sorted rack ids (string)
///
/// The caller passes one partition at a time, so we don't have to materialize a
/// topic's racks all at once and each can be discarded once hashed. The hasher
/// iterates the partition ID space internally, so the caller must walk the
/// partitions in ascending order of id.
///
/// Should be stable across nodes, processes, and architectures.
class topic_metadata_hasher {
public:
    topic_metadata_hasher(
      model::topic_id id, std::string_view name, int32_t partition_count);

    /// Hashes `racks` as the next partition, sorting them in place. Pass one
    /// entry per replica whose broker has a rack.
    /// Precondition: Total `partition` calls <= partition_count
    void partition(std::span<model::rack_id> racks);

    /// Precondition: `partition` ran once for every partition counted at
    /// construction.
    int64_t digest();

private:
    incremental_xxh3_64 _hash;
    int32_t _partition_count;
    int32_t _next_partition{0};
};

/// Hashes a whole subscription from the per-topic hashes, in order of ascending
/// topic id. See Kafka's Utils.computeGroupHash:
/// https://github.com/apache/kafka/blob/fce22525f7/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/Utils.java#L453-L468
///
/// Returns 0 for an empty map.
int64_t subscription_metadata_hash(const topic_metadata_hashes& topic_hashes);

} // namespace kafka
