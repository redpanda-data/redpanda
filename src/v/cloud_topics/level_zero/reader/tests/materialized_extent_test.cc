/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/common/micro_probe.h"
#include "cloud_topics/level_zero/reader/materialized_extent.h"
#include "cloud_topics/level_zero/reader/tests/materialized_extent_fixture.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/util/later.hh>

#include <queue>

ss::logger test_log("materialized_extent_test_log");

TEST_F_CORO(materialized_extent_fixture, materialize_from_cache) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(true, 1);

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(res.has_value());

    chunked_vector<model::record_batch> actual;
    actual.emplace_back(make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
    ASSERT_EQ_CORO(probe.num_cache_reads, 1);
}

TEST_F_CORO(materialized_extent_fixture, cache_get_fails) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.cache_get = injected_cache_get_failure::return_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::cache_read_error);
    ASSERT_EQ_CORO(probe.num_cache_reads, 1);
}

TEST_F_CORO(materialized_extent_fixture, cache_get_throws) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.cache_get = injected_cache_get_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::cache_read_error);
    ASSERT_EQ_CORO(probe.num_cache_reads, 1);
}

TEST_F_CORO(materialized_extent_fixture, cache_get_shutdown) {
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

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
    ASSERT_EQ_CORO(probe.num_cache_reads, 1);
}

TEST_F_CORO(materialized_extent_fixture, is_cached_throws) {
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

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::cache_read_error);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, is_cached_throws_shutdown) {
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

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, is_cached_stall_then_success) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::stall_then_ok}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(res.has_value());

    chunked_vector<model::record_batch> actual;
    actual.emplace_back(
      cloud_topics::l0::make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
    ASSERT_EQ_CORO(probe.num_cache_reads, 1);
}

TEST_F_CORO(materialized_extent_fixture, is_cached_stall_then_timeout) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::noop}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 100ms, 1ms, retry_strategy::backoff);

    auto extent = make_materialized_extent(partition.front().copy());

    co_await ss::sleep(100ms);
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::timeout);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, materialize_from_cloud) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(false, 1);

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(res.has_value());

    chunked_vector<model::record_batch> actual;
    actual.emplace_back(
      cloud_topics::l0::make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
    // NOTE: the cloud_io::remote is mocked so the callbacks
    // that update cloud_* metrics are not invoked. But we can
    // at least register cache writes which happen after successful
    // object download.
    ASSERT_EQ_CORO(probe.num_cache_writes, 1);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_return_failure) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::return_failure}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::download_failure);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_throw_shutdown) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::throw_shutdown}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_return_notfound) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::return_notfound}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::download_not_found);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_return_timeout) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::return_timeout}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::timeout);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_throw_error) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::unexpected_failure);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cache_reserve_space_throws) {
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

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(res.has_value());

    chunked_vector<model::record_batch> actual;
    actual.emplace_back(
      cloud_topics::l0::make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cache_reserve_space_throws_shutdown) {
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

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cache_put_throws) {
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

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(res.has_value());

    chunked_vector<model::record_batch> actual;
    actual.emplace_back(
      cloud_topics::l0::make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
}

TEST_F_CORO(materialized_extent_fixture, cache_put_throws_shutdown) {
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

    cloud_topics::l0::micro_probe probe;
    auto extent = make_materialized_extent(partition.front().copy());
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
}
