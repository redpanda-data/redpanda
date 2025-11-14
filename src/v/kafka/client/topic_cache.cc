// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/topic_cache.h"

#include "container/chunked_vector.h"
#include "kafka/client/types.h"
#include "kafka/client/utils.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

#include <ranges>

namespace kafka::client {

void topic_cache::apply(
  const chunked_vector<metadata_response::topic>& topics) {
    topics_t cache_update;
    cache_update.reserve(topics.size());

    auto now = ss::lowres_clock::now();
    const auto get_successful = std::views::filter([](const auto& resp) {
        return resp.error_code == kafka::error_code::none;
    });
    for (const auto& t : topics | get_successful) {
        static_assert(
          api_version_for(metadata_request::api_type::key) < api_version(12),
          "topic::name is nullable in v12+");
        auto& cache_t
          = cache_update.emplace(t.name.value(), topic_data{}).first->second;
        cache_t.last_seen_time = now;
        cache_t.authorized_operations = t.topic_authorized_operations;
        if (t.topic_id != model::topic_id{}) {
            cache_t.topic_id = t.topic_id;
        }
        if (!t.partitions.empty()) {
            cache_t.replication_factor = t.partitions[0].replica_nodes.size();
        }
        cache_t.partitions.reserve(t.partitions.size());
        for (const auto& p : t.partitions) {
            cache_t.partitions.emplace(
              p.partition_index,
              partition_data{
                .leader = p.leader_id, .leader_epoch = p.leader_epoch});
        }
    }

    merge_topics(std::move(cache_update));
    remove_timeout_topics();
}

topic_cache::topic_data topic_cache::merge_topic_data(
  topic_data&& cached_topic, topic_data&& updated_topic) {
    for (auto& [partition_id, updated_data] : updated_topic.partitions) {
        auto it = cached_topic.partitions.find(partition_id);
        if (it == cached_topic.partitions.end()) {
            // No cached data for this topic id
            continue;
        }

        auto& cached_data = it->second;
        if (cached_data.leader_epoch > updated_data.leader_epoch) {
            // Stale update. Keep old data
            updated_data = std::move(cached_data);
        }
    }

    return std::move(updated_topic);
}

void topic_cache::merge_topics(topics_t cache_update) {
    topics_t merged;

    for (auto& [topic, cached_topic] : _topics) {
        auto it = cache_update.find(topic);
        if (it != cache_update.end()) {
            // topic exist in update. It will be handled by the update loop
            continue;
        }

        // topic doesn't exist in update. Keep cached value
        merged.emplace(topic, std::move(cached_topic));
    }

    for (auto& [topic, updated_topic] : cache_update) {
        auto it = _topics.find(topic);
        if (it == _topics.end()) {
            // topic doesn't have cached data. Accept new value
            merged.emplace(topic, std::move(updated_topic));
            continue;
        }

        auto& cached_topic = it->second;
        if (cached_topic.topic_id != updated_topic.topic_id) {
            // topic has new id. Keep updated value
            merged.emplace(topic, std::move(updated_topic));
            continue;
        }

        // topic exists in both cache and update. Merge their data
        merged.emplace(
          topic,
          merge_topic_data(std::move(cached_topic), std::move(updated_topic)));
    }

    _topics = std::move(merged);
}

void topic_cache::remove_timeout_topics() {
    auto now = ss::lowres_clock::now();
    for (auto it = _topics.begin(); it != _topics.end();) {
        if (now - it->second.last_seen_time > _topic_timeout) {
            it = _topics.erase(it);
        } else {
            ++it;
        }
    }
}

std::optional<model::node_id>
topic_cache::leader(model::topic_partition_view tp) const {
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

std::optional<kafka::leader_epoch>
topic_cache::leader_epoch(model::topic_partition_view tp) const {
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

    return p_data.leader_epoch;
}

std::optional<model::topic_id>
topic_cache::topic_id_for_name(model::topic_view tp) const {
    auto topic_it = _topics.find(tp);
    if (topic_it == _topics.end()) {
        return std::nullopt;
    }
    return topic_it->second.topic_id;
}

std::optional<kafka::topic_authorized_operations>
topic_cache::authorized_operations_for_topic(model::topic_view tp) const {
    auto topic_it = _topics.find(tp);
    if (topic_it == _topics.end()) {
        return std::nullopt;
    }
    return topic_it->second.authorized_operations;
}

std::optional<int32_t>
topic_cache::partition_count(model::topic_view tp) const {
    auto topic_it = _topics.find(tp);
    if (topic_it == _topics.end()) {
        return std::nullopt;
    }
    return topic_it->second.partitions.size();
}

std::optional<int16_t>
topic_cache::replication_factor(model::topic_view tp) const {
    auto topic_it = _topics.find(tp);
    if (topic_it == _topics.end()) {
        return std::nullopt;
    }
    return topic_it->second.replication_factor;
}

const topic_cache::topics_t& topic_cache::cache() const noexcept {
    return _topics;
}

} // namespace kafka::client
