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
#include "kafka/server/subscription_metadata_hash_cache.h"
#include "model/fundamental.h"
#include "test_utils/test.h"
#include "utils/uuid.h"

#include <seastar/util/noncopyable_function.hh>

#include <gtest/gtest.h>

#include <map>
#include <optional>
#include <vector>

namespace {

model::topic_id topic_id_of(uint8_t first_byte) {
    std::vector<uint8_t> bytes(uuid_t::length, 0);
    bytes.front() = first_byte;
    return model::topic_id{uuid_t{bytes}};
}

/// Answers from `hashes` and counts every read, so a case can tell a cache hit
/// from a rebuild. `change` fires the notification the cache registered.
struct fake_source final : kafka::topic_metadata_source {
    std::optional<kafka::hashed_topic>
    topic_hash(const model::topic& topic) override {
        ++reads;
        auto it = hashes.find(topic);
        if (it == hashes.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    notification_id register_change_notification(
      ss::noncopyable_function<void(const model::topic&)> cb) override {
        _on_change = std::move(cb);
        ++registrations;
        return 7;
    }

    void unregister_change_notification(notification_id id) override {
        EXPECT_EQ(id, 7);
        --registrations;
    }

    void change(const model::topic& topic) { _on_change(topic); }

    std::map<model::topic, kafka::hashed_topic> hashes;
    int reads = 0;
    int registrations = 0;

private:
    ss::noncopyable_function<void(const model::topic&)> _on_change;
};

model::topic topic_1() { return model::topic{"topic-1"}; }
model::topic topic_2() { return model::topic{"topic-2"}; }

kafka::hashed_topic hashed(uint8_t id, int64_t hash) {
    return kafka::hashed_topic{.id = topic_id_of(id), .hash = hash};
}

} // namespace

TEST(subscription_metadata_hash_cache, reads_the_source_once_then_hits) {
    fake_source source;
    source.hashes.emplace(topic_1(), hashed(1, 111));
    kafka::subscription_metadata_hash_cache cache{source};

    EXPECT_EQ(cache.topic_hash(topic_1()), hashed(1, 111));
    EXPECT_EQ(source.reads, 1);

    EXPECT_EQ(cache.topic_hash(topic_1()), hashed(1, 111));
    EXPECT_EQ(source.reads, 1);
    EXPECT_EQ(cache.size(), 1);
}

TEST(subscription_metadata_hash_cache, reads_the_source_per_absent_topic) {
    fake_source source;
    kafka::subscription_metadata_hash_cache cache{source};

    EXPECT_EQ(cache.topic_hash(topic_1()), std::nullopt);
    EXPECT_EQ(cache.topic_hash(topic_1()), std::nullopt);
    EXPECT_EQ(source.reads, 2);
    EXPECT_EQ(cache.size(), 0);
}

/// A change notification drops the entry, and the next read takes the new
/// value.
TEST(subscription_metadata_hash_cache, a_change_rereads_the_source) {
    fake_source source;
    source.hashes.emplace(topic_1(), hashed(1, 111));
    kafka::subscription_metadata_hash_cache cache{source};

    EXPECT_EQ(cache.topic_hash(topic_1()), hashed(1, 111));
    source.hashes.insert_or_assign(topic_1(), hashed(1, 222));
    EXPECT_EQ(cache.topic_hash(topic_1()), hashed(1, 111));
    EXPECT_EQ(source.reads, 1);

    source.change(topic_1());
    EXPECT_EQ(cache.size(), 0);
    EXPECT_EQ(cache.topic_hash(topic_1()), hashed(1, 222));
    EXPECT_EQ(source.reads, 2);
}

TEST(subscription_metadata_hash_cache, unregisters_on_destruction) {
    fake_source source;
    {
        kafka::subscription_metadata_hash_cache cache{source};
        EXPECT_EQ(source.registrations, 1);
    }
    EXPECT_EQ(source.registrations, 0);
}

/// `invalidate` drops the entry, and the next read takes the new value.
TEST(subscription_metadata_hash_cache, invalidate_rereads_the_source) {
    fake_source source;
    source.hashes.emplace(topic_1(), hashed(1, 111));
    kafka::subscription_metadata_hash_cache cache{source};

    EXPECT_EQ(cache.topic_hash(topic_1()), hashed(1, 111));
    source.hashes.insert_or_assign(topic_1(), hashed(1, 222));
    EXPECT_EQ(cache.topic_hash(topic_1()), hashed(1, 111));
    EXPECT_EQ(source.reads, 1);

    cache.invalidate(topic_1());
    EXPECT_EQ(cache.topic_hash(topic_1()), hashed(1, 222));
    EXPECT_EQ(source.reads, 2);
}

TEST(
  subscription_metadata_hash_cache,
  subscription_hash_covers_topics_with_metadata) {
    fake_source source;
    source.hashes.emplace(topic_1(), hashed(1, 111));
    source.hashes.emplace(topic_2(), hashed(2, 222));
    kafka::subscription_metadata_hash_cache cache{source};

    kafka::topic_metadata_hashes expected;
    expected.emplace(topic_id_of(1), 111);
    expected.emplace(topic_id_of(2), 222);

    const std::vector<model::topic> subscription{
      topic_1(), topic_2(), model::topic{"absent"}};
    EXPECT_EQ(
      cache.subscription_hash(subscription),
      kafka::subscription_metadata_hash(expected));

    const auto reads = source.reads;
    cache.subscription_hash(subscription);
    // the cache reads the source again for the absent topic
    EXPECT_EQ(source.reads, reads + 1);
}

TEST(subscription_metadata_hash_cache, empty_subscription_hashes_to_zero) {
    fake_source source;
    kafka::subscription_metadata_hash_cache cache{source};
    EXPECT_EQ(cache.subscription_hash(std::vector<model::topic>{}), 0);
    EXPECT_EQ(source.reads, 0);
}
