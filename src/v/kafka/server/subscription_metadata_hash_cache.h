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

#include "container/chunked_hash_map.h"
#include "kafka/server/subscription_metadata_hash.h"
#include "model/fundamental.h"

#include <seastar/util/noncopyable_function.hh>

#include <concepts>
#include <cstdint>
#include <optional>
#include <ranges>

namespace kafka {

struct hashed_topic {
    model::topic_id id;
    int64_t hash;

    friend bool operator==(const hashed_topic&, const hashed_topic&) = default;
};

/// Interface through which the cache requests a topic's hash and subscribes to
/// metadata changes.
class topic_metadata_source {
public:
    using notification_id = int32_t;

    topic_metadata_source() = default;
    topic_metadata_source(const topic_metadata_source&) = delete;
    topic_metadata_source& operator=(const topic_metadata_source&) = delete;
    topic_metadata_source(topic_metadata_source&&) = delete;
    topic_metadata_source& operator=(topic_metadata_source&&) = delete;
    virtual ~topic_metadata_source() = default;

    /// Hashes the topic's metadata with `topic_metadata_hasher`. Returns
    /// nullopt for a topic that does not exist.
    virtual std::optional<hashed_topic> topic_hash(const model::topic&) = 0;

    /// Calls `on_change` with every topic a metadata delta changes or deletes.
    virtual notification_id register_change_notification(
      ss::noncopyable_function<void(const model::topic&)> on_change) = 0;

    virtual void unregister_change_notification(notification_id) = 0;
};

/// Holds one hash per topic. The cache drops a topic's entry when the source
/// reports a change to it, so after a change the first group to ask for that
/// topic pays for the hash, and every group after it reads the entry.
class subscription_metadata_hash_cache {
public:
    explicit subscription_metadata_hash_cache(topic_metadata_source& source);
    subscription_metadata_hash_cache(const subscription_metadata_hash_cache&)
      = delete;
    subscription_metadata_hash_cache&
    operator=(const subscription_metadata_hash_cache&) = delete;
    subscription_metadata_hash_cache(subscription_metadata_hash_cache&&)
      = delete;
    subscription_metadata_hash_cache&
    operator=(subscription_metadata_hash_cache&&) = delete;
    ~subscription_metadata_hash_cache();

    /// Returns the topic's id and hash, and builds the entry on a miss.
    /// Returns nullopt when the source has no metadata for the topic; the next
    /// call asks the source again.
    std::optional<hashed_topic> topic_hash(const model::topic& topic);

    /// Returns the hash of a whole subscription: the per-topic hashes, hashed
    /// in order of ascending topic id. Only the topics that have metadata
    /// count.
    template<std::ranges::input_range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, model::topic>
    int64_t subscription_hash(const Range& subscription) {
        topic_metadata_hashes hashes;
        for (const model::topic& topic : subscription) {
            if (auto entry = topic_hash(topic)) {
                hashes.emplace(entry->id, entry->hash);
            }
        }
        return subscription_metadata_hash(hashes);
    }

    /// Drops the topic's entry, for a topic that no group subscribes to any
    /// more.
    void invalidate(const model::topic& topic);

    size_t size() const { return _cache.size(); }

private:
    topic_metadata_source* _source;
    chunked_hash_map<model::topic, hashed_topic> _cache;
    topic_metadata_source::notification_id _notification;
};

} // namespace kafka
