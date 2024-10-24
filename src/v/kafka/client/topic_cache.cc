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
#include "kafka/client/brokers.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/partitioners.h"
#include "kafka/protocol/metadata.h"
#include "random/generators.h"

#include <seastar/core/future.hh>

namespace kafka::client {

ss::future<>
topic_cache::apply(small_fragment_vector<metadata_response::topic>&& topics) {
    topics_t cache;
    cache.reserve(topics.size());
    for (const auto& t : topics) {
        const auto initial_partition_id = model::partition_id{
          random_generators::get_int<model::partition_id::type>(
            t.partitions.size())};
        topic_data topic_data{
          .partitioner_func = default_partitioner(initial_partition_id)};
        auto& cache_t
          = cache.emplace(t.name, std::move(topic_data)).first->second;
        cache_t.partitions.reserve(t.partitions.size());
        for (const auto& p : t.partitions) {
            cache_t.partitions.emplace(
              p.partition_index, partition_data{.leader = p.leader_id});
        }
        cache_t.partitions.rehash(0);
    }
    cache.rehash(0);
    std::exchange(_topics, std::move(cache));
    return ss::now();
}

ss::future<model::node_id>
topic_cache::leader(model::topic_partition tp) const {
    if (auto topic_it = _topics.find(tp.topic); topic_it != _topics.end()) {
        const auto& parts = topic_it->second.partitions;
        if (auto part_it = parts.find(tp.partition); part_it != parts.end()) {
            const auto& part = part_it->second;
            if (part.leader == unknown_node_id) {
                return ss::make_exception_future<model::node_id>(
                  partition_error(tp, error_code::leader_not_available));
            }
            return ss::make_ready_future<model::node_id>(part.leader);
        }
    }
    return ss::make_exception_future<model::node_id>(
      partition_error(std::move(tp), error_code::unknown_topic_or_partition));
}

ss::future<model::partition_id>
topic_cache::partition_for(model::topic_view tv, const record_essence& rec) {
    if (auto topic_it = _topics.find(tv); topic_it != _topics.end()) {
        auto& pd = topic_it->second;
        return ss::make_ready_future<model::partition_id>(
          *pd.partitioner_func(rec, pd.partitions.size()));
    }
    return ss::make_exception_future<model::partition_id>(
      topic_error(tv, error_code::unknown_topic_or_partition));
}

} // namespace kafka::client
