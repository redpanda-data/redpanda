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

#include "kafka/server/subscription_metadata_hash_cache.h"

namespace kafka {

subscription_metadata_hash_cache::subscription_metadata_hash_cache(
  topic_metadata_source& source)
  : _source(&source)
  , _notification(source.register_change_notification(
      [this](const model::topic& topic) { _cache.erase(topic); })) {}

subscription_metadata_hash_cache::~subscription_metadata_hash_cache() {
    _source->unregister_change_notification(_notification);
}

std::optional<hashed_topic>
subscription_metadata_hash_cache::topic_hash(const model::topic& topic) {
    if (auto it = _cache.find(topic); it != _cache.end()) {
        return it->second;
    }
    auto entry = _source->topic_hash(topic);
    if (!entry.has_value()) {
        return std::nullopt;
    }
    _cache.emplace(topic, *entry);
    return entry;
}

void subscription_metadata_hash_cache::invalidate(const model::topic& topic) {
    _cache.erase(topic);
}

} // namespace kafka
