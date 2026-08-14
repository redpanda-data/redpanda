// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/subscription_metadata_hash.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kafka {

namespace {

/// The coordinator shard recomputes a group's hash to find out whether the
/// metadata of its subscribed topics moved, and it holds the reactor while it
/// does. These cases sweep the partition count of one topic and the topic count
/// of one group.

constexpr size_t replication_factor = 3;

topic_metadata_hashes topic_hashes(size_t count) {
    topic_metadata_hashes hashes;
    for (size_t t = 0; t < count; ++t) {
        hashes.emplace(model::topic_id::create(), static_cast<int64_t>(t));
    }
    return hashes;
}

std::vector<model::topic> topic_names(size_t count) {
    std::vector<model::topic> names;
    names.reserve(count);
    for (size_t t = 0; t < count; ++t) {
        names.emplace_back(ss::sstring{"topic-"} + std::to_string(t));
    }
    return names;
}

std::vector<model::topic_id> topic_ids(size_t count) {
    std::vector<model::topic_id> ids;
    ids.reserve(count);
    for (size_t t = 0; t < count; ++t) {
        ids.push_back(model::topic_id::create());
    }
    return ids;
}

std::vector<model::rack_id> rack_names() {
    std::vector<model::rack_id> names;
    names.reserve(replication_factor);
    for (size_t r = 0; r < replication_factor; ++r) {
        names.emplace_back(ss::sstring{"rack-"} + std::to_string(r));
    }
    return names;
}

/// The rack names a lookup hands back, and one buffer to derive a partition's
/// racks into. A caller reuses the buffer across partitions and topics.
struct hash_input {
    model::topic_id id = model::topic_id::create();
    std::vector<model::rack_id> names = rack_names();
    std::vector<model::rack_id> racks{
      std::vector<model::rack_id>(replication_factor)};
};

int64_t hash_topic(
  model::topic_id id,
  std::string_view name,
  size_t partitions,
  hash_input& input) {
    topic_metadata_hasher hasher{id, name, static_cast<int32_t>(partitions)};
    for (size_t p = 0; p < partitions; ++p) {
        input.racks.clear();
        for (size_t r = 0; r < replication_factor; ++r) {
            input.racks.push_back(input.names[(p + r) % input.names.size()]);
        }
        hasher.partition(input.racks);
    }
    return hasher.digest();
}

struct group_topics {
    topic_metadata_hashes hundred = topic_hashes(100);
    topic_metadata_hashes ten_thousand = topic_hashes(10'000);
};

/// What a coordinator pays for one group when no topic hash is cached: hash
/// every subscribed topic, then hash the group over those values.
struct cold_subscription : hash_input {
    static constexpr size_t topics = 1'000;
    static constexpr size_t partitions = 100;

    std::vector<model::topic_id> ids = topic_ids(topics);
    std::vector<model::topic> names = topic_names(topics);
};

} // namespace

PERF_TEST_F(hash_input, hash_topic_1k_partitions) {
    auto hash = hash_topic(id, "benchmark-topic", 1'000, *this);
    perf_tests::do_not_optimize(hash);
}

PERF_TEST_F(hash_input, hash_topic_10k_partitions) {
    auto hash = hash_topic(id, "benchmark-topic", 10'000, *this);
    perf_tests::do_not_optimize(hash);
}

PERF_TEST_F(hash_input, hash_topic_100k_partitions) {
    auto hash = hash_topic(id, "benchmark-topic", 100'000, *this);
    perf_tests::do_not_optimize(hash);
}

PERF_TEST_F(group_topics, hash_group_100_topics) {
    auto hash = subscription_metadata_hash(hundred);
    perf_tests::do_not_optimize(hash);
}

PERF_TEST_F(group_topics, hash_group_10k_topics) {
    auto hash = subscription_metadata_hash(ten_thousand);
    perf_tests::do_not_optimize(hash);
}

PERF_TEST_F(cold_subscription, hash_1k_topics_of_100_partitions) {
    topic_metadata_hashes hashes;
    for (size_t t = 0; t < topics; ++t) {
        hashes.emplace(
          ids[t], hash_topic(ids[t], names[t](), partitions, *this));
    }
    auto hash = subscription_metadata_hash(hashes);
    perf_tests::do_not_optimize(hash);
}

} // namespace kafka
