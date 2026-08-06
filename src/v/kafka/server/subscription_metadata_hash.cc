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

#include "kafka/server/subscription_metadata_hash.h"

#include "base/vassert.h"
#include "hashing/xx.h"
#include "utils/uuid.h"

#include <seastar/core/byteorder.hh>

#include <algorithm>
#include <bit>
#include <concepts>
#include <span>
#include <string_view>

namespace kafka {

namespace {

/// This byte starts every topic hash. Bump it when the hash function or inputs
/// change to force a rebalance on every group irrespective of subscriptions.
constexpr uint8_t hash_version = 0;

/// Write an integer as big-endian so that nodes of different architectures
/// agree. Note that we always write sizeof(T) bytes irrespective of value.
template<std::integral T>
void update(incremental_xxh3_64& hash, T value) {
    const auto big_endian = ss::cpu_to_be(value);
    hash.update(std::as_bytes(std::span{&big_endian, 1}));
}

/// Writes the length before the bytes, so that the boundary between two strings
/// is part of the hash: "a" then "bc" differs from "ab" then "c".
void update(incremental_xxh3_64& hash, std::string_view value) {
    update(hash, static_cast<int32_t>(value.size()));
    hash.update(value);
}

void update(incremental_xxh3_64& hash, const uuid_t& value) {
    hash.update(std::as_bytes(std::span{value.uuid().begin(), uuid_t::length}));
}

} // namespace

topic_metadata_hasher::topic_metadata_hasher(
  model::topic_id id, std::string_view name, int32_t partition_count)
  : _partition_count(partition_count) {
    update(_hash, hash_version);
    update(_hash, id());
    update(_hash, name);
    update(_hash, partition_count);
}

void topic_metadata_hasher::partition(std::span<model::rack_id> racks) {
    vassert(
      _next_partition < _partition_count,
      "partition {} is past the {} counted at construction",
      _next_partition,
      _partition_count);

    update(_hash, _next_partition++);
    std::ranges::sort(racks);
    for (const auto& rack : racks) {
        update(_hash, std::string_view{rack()});
    }
}

int64_t topic_metadata_hasher::digest() {
    vassert(
      _next_partition == _partition_count,
      "hashed {} of {} partitions",
      _next_partition,
      _partition_count);

    return std::bit_cast<int64_t>(_hash.digest());
}

int64_t subscription_metadata_hash(const topic_metadata_hashes& topic_hashes) {
    if (topic_hashes.empty()) {
        return 0;
    }

    // topic hashes come sorted by topic_id, but all we care about is the value.
    incremental_xxh3_64 hash;
    for (const auto& [_, topic_hash] : topic_hashes) {
        update(hash, topic_hash);
    }
    return std::bit_cast<int64_t>(hash.digest());
}

} // namespace kafka
