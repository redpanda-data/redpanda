// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/reader/placeholder_extent.h"
#include "cloud_topics/reader/tests/placeholder_extent_fixture.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/util/later.hh>

#include <chrono>
#include <exception>
#include <queue>

namespace cloud_topics = experimental::cloud_topics;
ss::logger test_log("placeholder_extent_test_log");

TEST_F_CORO(placeholder_extent_fixture, materialize_from_cache) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(true, 1);

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_FALSE_CORO(res.has_error());

    fragmented_vector<model::record_batch> actual;
    actual.emplace_back(make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_EQ_CORO(actual, expected);
}

TEST_F_CORO(placeholder_extent_fixture, cache_get_fails) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.cache_get = injected_cache_get_failure::return_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::cache_read_error);
}

TEST_F_CORO(placeholder_extent_fixture, cache_get_throws) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.cache_get = injected_cache_get_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::cache_read_error);
}

TEST_F_CORO(placeholder_extent_fixture, cache_get_shutdown) {
    // Test situation when the
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.cache_get = injected_cache_get_failure::throw_shutdown}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());

    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
}

TEST_F_CORO(placeholder_extent_fixture, is_cached_throws) {
    // Test situation when the
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());

    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::cache_read_error);
}

TEST_F_CORO(placeholder_extent_fixture, is_cached_throws_shutdown) {
    // Test situation when the
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::throw_shutdown}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());

    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
}

TEST_F_CORO(placeholder_extent_fixture, is_cached_stall_then_success) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::stall_then_ok}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_FALSE_CORO(res.has_error());

    fragmented_vector<model::record_batch> actual;
    actual.emplace_back(cloud_topics::make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_EQ_CORO(actual, expected);
}

TEST_F_CORO(placeholder_extent_fixture, is_cached_stall_then_timeout) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::noop}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 100ms, 1ms, retry_strategy::backoff);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());

    co_await ss::sleep(100ms);
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::timeout);
}

TEST_F_CORO(placeholder_extent_fixture, materialize_from_cloud) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(false, 1);

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_FALSE_CORO(res.has_error());

    fragmented_vector<model::record_batch> actual;
    actual.emplace_back(cloud_topics::make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_EQ_CORO(actual, expected);
}

TEST_F_CORO(placeholder_extent_fixture, cloud_get_return_failure) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::return_failure}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::download_failure);
}

TEST_F_CORO(placeholder_extent_fixture, cloud_get_throw_shutdown) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::throw_shutdown}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
}

TEST_F_CORO(placeholder_extent_fixture, cloud_get_return_notfound) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::return_notfound}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::download_not_found);
}

TEST_F_CORO(placeholder_extent_fixture, cloud_get_return_timeout) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::return_timeout}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::timeout);
}

TEST_F_CORO(placeholder_extent_fixture, cloud_get_throw_error) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::unexpected_failure);
}

TEST_F_CORO(placeholder_extent_fixture, cache_reserve_space_throws) {
    // If we fail to reserve space the request should still succeed
    // but 'cache.put' can't be invoked.
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cache_rsv = injected_cache_rsv_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_FALSE_CORO(res.has_error());

    fragmented_vector<model::record_batch> actual;
    actual.emplace_back(cloud_topics::make_raft_data_batch(extent));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_EQ_CORO(actual, expected);
}

TEST_F_CORO(placeholder_extent_fixture, cache_reserve_space_throws_shutdown) {
    // If we fail to reserve space because of the shutdown the result
    // should be an errc::shutdown code.
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cache_rsv = injected_cache_rsv_failure::throw_shutdown}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
}

TEST_F_CORO(placeholder_extent_fixture, cache_put_throws) {
    // If we fail to put element into the cache the request should still succeed
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cache_put = injected_cache_put_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_FALSE_CORO(res.has_error());

    fragmented_vector<model::record_batch> actual;
    actual.emplace_back(cloud_topics::make_raft_data_batch(extent));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_EQ_CORO(actual, expected);
}

TEST_F_CORO(placeholder_extent_fixture, cache_put_throws_shutdown) {
    // If we fail to put because of the shutdown the result
    // should be an errc::shutdown code.
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cache_put = injected_cache_put_failure::throw_shutdown}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = cloud_topics::make_placeholder_extent(
      partition.front().copy());
    auto res = co_await cloud_topics::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc);

    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
}
