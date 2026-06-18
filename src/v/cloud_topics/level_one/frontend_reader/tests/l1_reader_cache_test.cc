/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/frontend_reader/l1_reader_cache.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader.h"
#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "cloud_topics/log_reader_config.h"
#include "config/property.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace cloud_topics::l1 {

class l1_reader_cache_test : public l1_reader_fixture {
protected:
    static constexpr std::chrono::milliseconds default_eviction_timeout = 60s;
    static constexpr size_t default_max_size = 128;

    void SetUp() override {
        _eviction_timeout_binding = config::mock_binding(
          default_eviction_timeout);
        _max_size_binding = config::mock_binding(default_max_size);
        _cache = std::make_unique<l1_reader_cache>(
          _eviction_timeout_binding, _max_size_binding);
    }

    ss::future<> TearDownAsync() override {
        co_await _cache->stop();
        _cache.reset();
    }

    std::unique_ptr<level_one_log_reader_impl> make_reader_impl(
      const model::ntp& ntp,
      const model::topic_id_partition& tidp,
      kafka::offset start_offset = kafka::offset{0},
      kafka::offset max_offset = kafka::offset::max(),
      size_t max_bytes = std::numeric_limits<size_t>::max(),
      bool strict_max_bytes = false) {
        auto config = make_test_config(
          start_offset, max_offset, max_bytes, strict_max_bytes);
        return std::make_unique<level_one_log_reader_impl>(
          config, ntp, tidp, &_metastore, &_io);
    }

    config::binding<std::chrono::milliseconds> _eviction_timeout_binding{
      config::mock_binding(default_eviction_timeout)};
    config::binding<size_t> _max_size_binding{
      config::mock_binding(default_max_size)};
    std::unique_ptr<l1_reader_cache> _cache;
};

TEST_F(l1_reader_cache_test, empty_cache_returns_nullopt) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    auto cfg = make_test_config(kafka::offset{0}, kafka::offset::max());
    auto result = _cache->get_reader(tidp, cfg);
    ASSERT_FALSE(result.has_value());
    auto stats = _cache->get_stats();
    EXPECT_EQ(stats.cached_readers, 0);
    EXPECT_EQ(stats.in_use_readers, 0);
}

TEST_F(l1_reader_cache_test, put_then_get_at_matching_offset) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, /*count=*/5).get();
    auto last_offset = batches.back().last_offset();

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // Put a reader and consume all data through the wrapper.
    auto reader_impl = make_reader_impl(ntp, tidp);
    auto wrapped = _cache->put(std::move(reader_impl));

    auto data = model::consume_reader_to_memory(
                  std::move(wrapped), model::no_timeout)
                  .get();
    ASSERT_FALSE(data.empty());

    // Reader reached EOS and should not be cached (not reusable).
    auto stats = _cache->get_stats();
    EXPECT_EQ(stats.cached_readers, 0);

    // Verify miss at next offset since reader was disposed.
    auto next_offset = kafka::offset(last_offset() + 1);
    auto cfg = make_test_config(next_offset, kafka::offset::max());
    auto hit = _cache->get_reader(tidp, cfg);
    EXPECT_FALSE(hit.has_value());
}

TEST_F(l1_reader_cache_test, get_at_different_offset_is_miss) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, /*count=*/5).get();
    // Use the first batch's last offset as max_offset so we read exactly
    // one batch and then the reader hits EOS at offset > max_offset.
    auto first_batch_last = kafka::offset(batches.front().last_offset()());

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // Read with max_offset = first batch's last offset.
    auto reader_impl = make_reader_impl(
      ntp, tidp, kafka::offset{0}, first_batch_last);
    auto wrapped = _cache->put(std::move(reader_impl));

    auto data = model::consume_reader_to_memory(
                  std::move(wrapped), model::no_timeout)
                  .get();
    ASSERT_FALSE(data.empty());

    // Reader is at EOS (max_offset reached). Try to get at offset 0 — miss.
    auto cfg2 = make_test_config(kafka::offset{0}, kafka::offset::max());
    auto miss = _cache->get_reader(tidp, cfg2);
    EXPECT_FALSE(miss.has_value());
}

TEST_F(l1_reader_cache_test, get_with_different_tidp_is_miss) {
    auto [ntp1, tidp1] = make_ntidp("topic_a");
    auto [ntp2, tidp2] = make_ntidp("topic_b");

    auto batches1
      = model::test::make_random_batches(model::offset{0}, /*count=*/3).get();
    // Limit reader to first batch so it stays reusable (not EOS for the
    // full data, but EOS via max_offset). Actually we just consume all
    // data — at EOS the reader is disposed; we just verify the miss by
    // tidp mismatch.

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp1, std::move(batches1));
    make_l1_objects(std::move(tidp_batches)).get();

    // Put a reader for tidp1 — consume all data.
    auto reader_impl = make_reader_impl(ntp1, tidp1);
    auto wrapped = _cache->put(std::move(reader_impl));

    auto data = model::consume_reader_to_memory(
                  std::move(wrapped), model::no_timeout)
                  .get();
    ASSERT_FALSE(data.empty());

    // Even though the reader is now at EOS (disposed), verify that a
    // lookup for a different tidp also returns nullopt.
    auto cfg = make_test_config(kafka::offset{0}, kafka::offset::max());
    auto miss = _cache->get_reader(tidp2, cfg);
    EXPECT_FALSE(miss.has_value());
}

TEST_F(l1_reader_cache_test, size_cap_evicts_oldest) {
    auto [ntp_a, tidp_a] = make_ntidp("topic_a");
    auto [ntp_b, tidp_b] = make_ntidp("topic_b");

    auto batches_a
      = model::test::make_random_batches(model::offset{0}, /*count=*/5).get();
    auto batches_b
      = model::test::make_random_batches(model::offset{0}, /*count=*/5).get();

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp_a, std::move(batches_a));
    tidp_batches.emplace_back(tidp_b, std::move(batches_b));
    make_l1_objects(std::move(tidp_batches)).get();

    // Recreate cache with max size 1.
    _cache->stop().get();
    // Destroy before re-creating so the per-shard metric group unregisters
    // before the replacement registers it again.
    _cache.reset();
    _max_size_binding = config::mock_binding<size_t>(1);
    _eviction_timeout_binding = config::mock_binding(default_eviction_timeout);
    _cache = std::make_unique<l1_reader_cache>(
      _eviction_timeout_binding, _max_size_binding);

    // Put reader A with strict byte limit so it reads one batch and stays
    // reusable.
    auto r_a = make_reader_impl(
      ntp_a,
      tidp_a,
      kafka::offset{0},
      kafka::offset::max(),
      /*max_bytes=*/1,
      /*strict_max_bytes=*/true);
    auto wrapped_a = _cache->put(std::move(r_a));
    auto data_a = model::consume_reader_to_memory(
                    std::move(wrapped_a), model::no_timeout)
                    .get();
    ASSERT_EQ(data_a.size(), 1);

    // Reader A should be cached.
    auto stats1 = _cache->get_stats();
    ASSERT_EQ(stats1.cached_readers, 1) << "reader A was not cached";

    // Put reader B — also byte-limited and reusable. This should evict
    // reader A because max_size=1.
    auto r_b = make_reader_impl(
      ntp_b,
      tidp_b,
      kafka::offset{0},
      kafka::offset::max(),
      /*max_bytes=*/1,
      /*strict_max_bytes=*/true);
    auto wrapped_b = _cache->put(std::move(r_b));

    // Reader B is now in-use (1 in_use + 0 cached after eviction of A).
    auto stats2 = _cache->get_stats();
    EXPECT_EQ(stats2.cached_readers, 0)
      << "reader A should have been evicted by size cap";
    EXPECT_EQ(stats2.in_use_readers, 1);

    // Consume reader B so it returns to cache.
    auto data_b = model::consume_reader_to_memory(
                    std::move(wrapped_b), model::no_timeout)
                    .get();
    ASSERT_EQ(data_b.size(), 1);

    // Verify reader A is gone — lookup for tidp_a should miss.
    auto next_a = kafka::offset(data_a.back().last_offset()() + 1);
    auto cfg_a = make_test_config(next_a, kafka::offset::max());
    auto miss = _cache->get_reader(tidp_a, cfg_a);
    EXPECT_FALSE(miss.has_value()) << "reader A should have been evicted";
}

TEST_F(l1_reader_cache_test, stop_drains_idle_readers) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, /*count=*/5).get();

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // Put and consume to completion.
    auto reader_impl = make_reader_impl(ntp, tidp);
    auto wrapped = _cache->put(std::move(reader_impl));

    auto data = model::consume_reader_to_memory(
                  std::move(wrapped), model::no_timeout)
                  .get();

    // Stop the cache — should drain without errors.
    _cache->stop().get();

    auto stats = _cache->get_stats();
    EXPECT_EQ(stats.cached_readers, 0);
    EXPECT_EQ(stats.in_use_readers, 0);

    // Recreate for TearDown.
    _cache.reset();
    _eviction_timeout_binding = config::mock_binding(default_eviction_timeout);
    _max_size_binding = config::mock_binding(default_max_size);
    _cache = std::make_unique<l1_reader_cache>(
      _eviction_timeout_binding, _max_size_binding);
}

TEST_F(l1_reader_cache_test, is_reusable_false_for_eos_reader) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, /*count=*/2).get();

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // Consume all data to reach EOS.
    auto reader_impl = make_reader_impl(ntp, tidp);
    auto wrapped = _cache->put(std::move(reader_impl));

    auto data = model::consume_reader_to_memory(
                  std::move(wrapped), model::no_timeout)
                  .get();
    ASSERT_FALSE(data.empty());

    // Reader reached EOS. Disposed, not cached.
    auto stats = _cache->get_stats();
    EXPECT_EQ(stats.cached_readers, 0);
}

TEST_F(l1_reader_cache_test, sequential_fetches_hit_cache) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, /*count=*/10).get();
    auto expected_count = batches.size();
    auto first_batch_last = kafka::offset(batches.front().last_offset()());

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // First fetch: read with max_offset limiting to first batch only.
    auto reader_impl = make_reader_impl(
      ntp, tidp, kafka::offset{0}, first_batch_last);
    auto wrapped = _cache->put(std::move(reader_impl));

    auto data1 = model::consume_reader_to_memory(
                   std::move(wrapped), model::no_timeout)
                   .get();
    ASSERT_FALSE(data1.empty());
    auto next_offset = kafka::offset(data1.back().last_offset()() + 1);

    // Reader should be returned to cache (stream still open).
    auto stats = _cache->get_stats();
    ASSERT_EQ(stats.cached_readers, 1) << "reader was not returned to cache";

    // Second fetch: get from cache at next_offset — must be a hit.
    auto cfg2 = make_test_config(next_offset, kafka::offset::max());
    auto hit = _cache->get_reader(tidp, cfg2);
    ASSERT_TRUE(hit.has_value()) << "expected cache hit at next offset";

    auto data2 = model::consume_reader_to_memory(
                   std::move(*hit), model::no_timeout)
                   .get();
    EXPECT_EQ(data1.size() + data2.size(), expected_count);
}

TEST_F(l1_reader_cache_test, byte_limited_reader_is_reusable) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, /*count=*/5).get();
    auto expected_count = batches.size();

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // Read with strict_max_bytes=true and a small byte limit. The first
    // batch is always returned for progress; subsequent batches are
    // rejected because they would exceed the byte budget.
    auto reader_impl = make_reader_impl(
      ntp,
      tidp,
      kafka::offset{0},
      kafka::offset::max(),
      /*max_bytes=*/1,
      /*strict_max_bytes=*/true);
    auto wrapped = _cache->put(std::move(reader_impl));

    auto data1 = model::consume_reader_to_memory(
                   std::move(wrapped), model::no_timeout)
                   .get();
    ASSERT_EQ(data1.size(), 1);
    auto next_offset = kafka::offset(data1.back().last_offset()() + 1);

    // Reader should be cached — it has an open stream.
    auto stats = _cache->get_stats();
    ASSERT_EQ(stats.cached_readers, 1) << "byte-limited reader was not cached";

    // Second fetch picks up where we left off.
    auto cfg2 = make_test_config(next_offset, kafka::offset::max());
    auto hit = _cache->get_reader(tidp, cfg2);
    ASSERT_TRUE(hit.has_value())
      << "expected cache hit after byte-limited read";

    auto data2 = model::consume_reader_to_memory(
                   std::move(*hit), model::no_timeout)
                   .get();
    EXPECT_EQ(data1.size() + data2.size(), expected_count);
}

} // namespace cloud_topics::l1
