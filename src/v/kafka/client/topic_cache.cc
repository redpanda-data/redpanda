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
    topics_t cache;
    cache.reserve(topics.size());
    for (const auto& t : topics) {
        auto& cache_t = cache.emplace(t.name, topic_data{}).first->second;
        cache_t.partitions.reserve(t.partitions.size());
        for (const auto& p : t.partitions) {
            cache_t.partitions.emplace(
              p.partition_index, partition_data{.leader = p.leader_id});
        }
        cache_t.partitions.rehash(0);
    }
    cache.rehash(0);
    std::exchange(_topics, std::move(cache));
}

model::node_id topic_cache::leader(model::topic_partition tp) const {
    if (auto topic_it = _topics.find(tp.topic); topic_it != _topics.end()) {
        const auto& parts = topic_it->second.partitions;
        if (auto part_it = parts.find(tp.partition); part_it != parts.end()) {
            const auto& part = part_it->second;
            if (part.leader == unknown_node_id) {
                throw partition_error(tp, error_code::leader_not_available);
            }
            return part.leader;
        }
    }
    throw partition_error(
      std::move(tp), error_code::unknown_topic_or_partition);
}

} // namespace kafka::client
