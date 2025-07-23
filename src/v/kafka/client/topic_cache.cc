// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/topic_cache.h"

#include "container/fragmented_vector.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/types.h"
#include "kafka/protocol/metadata.h"

#include <seastar/core/future.hh>

namespace kafka::client {

void topic_cache::apply(
  const small_fragment_vector<metadata_response::topic>& topics) {
    topics_t new_cache;
    new_cache.reserve(topics.size());
    for (const auto& t : topics) {
        auto& cache_t = new_cache.emplace(t.name, topic_data{}).first->second;
        cache_t.partitions.reserve(t.partitions.size());
        for (const auto& p : t.partitions) {
            cache_t.partitions.emplace(
              p.partition_index, partition_data{.leader = p.leader_id});
        }
    }

    std::exchange(_topics, std::move(new_cache));
}

std::optional<model::node_id>
topic_cache::leader(const model::topic_partition& tp) const {
    auto topic_it = _topics.find(tp.topic);
    if (topic_it == _topics.end()) {
        return std::nullopt;
    }

    const auto& topic_partitions = topic_it->second.partitions;
    auto part_it = topic_partitions.find(tp.partition);
    if (part_it == topic_partitions.end()) {
        return std::nullopt;
    }
    const auto& p_data = part_it->second;
    if (p_data.leader == unknown_node_id) {
        return std::nullopt;
    }

    return p_data.leader;
}

} // namespace kafka::client
