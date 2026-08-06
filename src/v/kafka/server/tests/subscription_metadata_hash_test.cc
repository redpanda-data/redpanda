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
#include "model/fundamental.h"
#include "model/metadata.h"
#include "test_utils/test.h"
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>

#include <gtest/gtest.h>

#include <string_view>
#include <vector>

namespace {

using racks = std::vector<model::rack_id>;

model::topic_id topic_id_of(uint8_t first_byte) {
    std::vector<uint8_t> bytes(uuid_t::length, 0);
    bytes.front() = first_byte;
    return model::topic_id{uuid_t{bytes}};
}

racks racks_of(const std::vector<std::string_view>& names) {
    racks result;
    result.reserve(names.size());
    for (const auto& name : names) {
        result.emplace_back(ss::sstring{name});
    }
    return result;
}

/// Feeds the partitions to a hasher in the order given.
/// Each rack list is sorted in place by the hasher, so we copy them at the call
/// site verbatim.
int64_t hash_of(
  const std::vector<racks>& partitions,
  model::topic_id id = topic_id_of(1),
  std::string_view name = "topic-1") {
    kafka::topic_metadata_hasher hasher{
      id, name, static_cast<int32_t>(partitions.size())};
    for (auto racks_here : partitions) {
        hasher.partition(racks_here);
    }
    return hasher.digest();
}

std::vector<racks> a_topic() {
    return {racks_of({"rack-a"}), racks_of({"rack-b"})};
}

} // namespace

/// topic_metadata_hasher::partition sorts each racks vector, so the order the
/// caller listed them in does not change the value.
TEST(subscription_metadata_hash, ignores_rack_order) {
    EXPECT_EQ(
      hash_of({racks_of({"rack-y", "rack-x", "rack-x"}), racks_of({"rack-b"})}),
      hash_of(
        {racks_of({"rack-x", "rack-x", "rack-y"}), racks_of({"rack-b"})}));
}

/// The hash writes every rack entry, so a partition that lists a rack twice
/// hashes differently from one that lists it once.
TEST(subscription_metadata_hash, counts_every_rack_entry) {
    EXPECT_NE(
      hash_of({racks_of({"rack-x", "rack-x"})}),
      hash_of({racks_of({"rack-x"})}));
}

TEST(subscription_metadata_hash, detects_every_input_change) {
    const auto baseline = hash_of(a_topic());

    EXPECT_NE(baseline, hash_of(a_topic(), topic_id_of(1), "topic-2"));

    EXPECT_NE(baseline, hash_of(a_topic(), topic_id_of(2)));

    EXPECT_NE(
      baseline,
      hash_of(
        {racks_of({"rack-a"}), racks_of({"rack-b"}), racks_of({"rack-c"})}));

    EXPECT_NE(baseline, hash_of({racks_of({"rack-z"}), racks_of({"rack-b"})}));

    EXPECT_NE(
      baseline,
      hash_of({racks_of({"rack-a", "rack-b"}), racks_of({"rack-b"})}));

    EXPECT_NE(baseline, hash_of({racks(), racks_of({"rack-b"})}));
}

TEST(subscription_metadata_hash, binds_racks_to_their_partition) {
    EXPECT_NE(
      hash_of({racks_of({"rack-a"}), racks_of({"rack-b"})}),
      hash_of({racks_of({"rack-b"}), racks_of({"rack-a"})}));
}

/// A length precedes each variable-length field, so two field lists that
/// concatenate to the same bytes give distinct hashes.
TEST(subscription_metadata_hash, distinguishes_regrouped_strings) {
    EXPECT_NE(
      hash_of({racks_of({"a", "bc"})}), hash_of({racks_of({"ab", "c"})}));

    EXPECT_NE(
      hash_of({racks_of({"rack-a"})}, topic_id_of(1), "topic-11"),
      hash_of({racks_of({"1rack-a"})}, topic_id_of(1), "topic-1"));
}

TEST(subscription_metadata_hash, empty_subscription_hashes_to_zero) {
    EXPECT_EQ(kafka::subscription_metadata_hash({}), 0);
}

TEST(subscription_metadata_hash, group_hash_follows_its_topics) {
    const auto topic_hash = hash_of(a_topic());

    kafka::topic_metadata_hashes one_topic;
    one_topic.emplace(topic_id_of(1), topic_hash);

    kafka::topic_metadata_hashes two_topics = one_topic;
    two_topics.emplace(topic_id_of(2), topic_hash);

    kafka::topic_metadata_hashes changed_topic;
    changed_topic.emplace(topic_id_of(1), topic_hash + 1);

    EXPECT_NE(
      kafka::subscription_metadata_hash(one_topic),
      kafka::subscription_metadata_hash(two_topics));
    EXPECT_NE(
      kafka::subscription_metadata_hash(one_topic),
      kafka::subscription_metadata_hash(changed_topic));
}
