/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vlog.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/reader/materialized_extent_reader.h"
#include "cloud_topics/level_zero/reader/tests/materialized_extent_fixture.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "test_utils/test.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

inline ss::logger test_log("materialized_extent_reader_gtest");

using namespace std::chrono_literals;

static chunked_vector<cloud_topics::extent_meta>
convert_placeholders(const chunked_vector<model::record_batch>& batches) {
    chunked_vector<cloud_topics::extent_meta> res;
    for (const auto& b : batches) {
        auto mext = materialized_extent_fixture::make_materialized_extent(
          b.copy());
        res.push_back(mext.meta);
    }
    return res;
}

TEST_F_CORO(materialized_extent_fixture, full_scan_test) {
    const int num_batches = 10;
    co_await add_random_batches(num_batches);
    produce_placeholders(true, 1);
    auto underlying = convert_placeholders(make_underlying());
    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 100ms);
    retry_chain_logger logger(test_log, rtc, "materialized_extent_reader_test");
    auto [actual, probe] = co_await cloud_topics::l0::materialize_placeholders(
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      remote,
      cache,
      cloud_topics::allow_materialization_failure::no,
      rtc,
      logger);
    ASSERT_EQ_CORO(actual.value().size(), expected.size());
    ASSERT_TRUE_CORO(actual.value() == expected);
}

// Same as 'full_scan_test' but the range can be arbitrary
ss::future<> test_aggregated_log_partial_scan(
  materialized_extent_fixture* fx, int num_batches, int begin, int end) {
    co_await fx->add_random_batches(num_batches);
    fx->produce_placeholders(true, 1, {}, begin, end);

    // Copy batches that we expect to read
    model::offset base;
    model::offset last;
    chunked_vector<model::record_batch> expected_view;
    for (int i = begin; i <= end; i++) {
        const auto& hdr = fx->expected[i];
        if (base == model::offset{}) {
            base = hdr.base_offset();
        }
        last = hdr.last_offset();
        expected_view.push_back(fx->expected[i].copy());
    }
    vlog(
      test_log.info,
      "Fetching offsets {} to {}, expect to read {} batches",
      base,
      last,
      expected_view.size());
    for (const auto& b : expected_view) {
        vlog(
          test_log.info,
          "Expect fetching {}:{}",
          b.base_offset(),
          b.last_offset());
    }

    auto underlying = convert_placeholders(fx->make_underlying());

    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 100ms);
    retry_chain_logger logger(test_log, rtc, "materialized_extent_reader_test");

    auto [actual, _] = co_await cloud_topics::l0::materialize_placeholders(
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      fx->remote,
      fx->cache,
      cloud_topics::allow_materialization_failure::no,
      rtc,
      logger);

    ASSERT_EQ_CORO(actual.value().size(), expected_view.size());
    ASSERT_TRUE_CORO(actual.value() == expected_view);
}

TEST_F_CORO(materialized_extent_fixture, scan_range) {
    co_await test_aggregated_log_partial_scan(this, 10, 0, 0);
}

TEST_F_CORO(materialized_extent_fixture, timeout_test) {
    // The test validates that the error is properly propagated
    // from the materialize_placeholders function.
    const int num_batches = 1;
    co_await add_random_batches(num_batches);

    std::queue<injected_failure> failures;
    failures.push({.cloud_get = injected_cloud_get_failure::return_timeout});
    produce_placeholders(false, 1, failures);

    auto underlying = convert_placeholders(make_underlying());
    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 100ms);
    retry_chain_logger logger(test_log, rtc, "materialized_extent_reader_test");

    auto [actual, probe] = co_await cloud_topics::l0::materialize_placeholders(
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      remote,
      cache,
      cloud_topics::allow_materialization_failure::no,
      rtc,
      logger);

    ASSERT_TRUE_CORO(!actual.has_value());
    ASSERT_TRUE_CORO(actual.error() == cloud_topics::errc::timeout);
}

TEST_F_CORO(
  materialized_extent_fixture, allow_materialization_failure_skips_notfound) {
    // When allow_materialization_failure is set and an extent's object returns
    // 404, that extent should be skipped and the result should be empty.
    co_await add_random_batches(1);

    std::queue<injected_failure> failures;
    failures.push({.cloud_get = injected_cloud_get_failure::return_notfound});
    produce_placeholders(false, 1, std::move(failures));

    auto underlying = convert_placeholders(make_underlying());
    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 100ms);
    retry_chain_logger logger(test_log, rtc, "materialized_extent_reader_test");

    auto [actual, probe] = co_await cloud_topics::l0::materialize_placeholders(
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      remote,
      cache,
      cloud_topics::allow_materialization_failure::yes,
      rtc,
      logger);

    ASSERT_TRUE_CORO(actual.has_value());
    ASSERT_EQ_CORO(actual.value().size(), 0);
}

TEST_F_CORO(
  materialized_extent_fixture, notfound_propagates_without_failure_flag) {
    // Without allow_materialization_failure, a 404 should propagate as an
    // error, same as any other download failure.
    co_await add_random_batches(1);

    std::queue<injected_failure> failures;
    failures.push({.cloud_get = injected_cloud_get_failure::return_notfound});
    produce_placeholders(false, 1, std::move(failures));

    auto underlying = convert_placeholders(make_underlying());
    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 100ms);
    retry_chain_logger logger(test_log, rtc, "materialized_extent_reader_test");

    auto [actual, probe] = co_await cloud_topics::l0::materialize_placeholders(
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      remote,
      cache,
      cloud_topics::allow_materialization_failure::no,
      rtc,
      logger);

    ASSERT_TRUE_CORO(!actual.has_value());
    ASSERT_TRUE_CORO(actual.error() == cloud_topics::errc::download_not_found);
}

TEST_F_CORO(
  materialized_extent_fixture, non_404_error_propagates_with_failure_flag) {
    // Only 404 errors are tolerated. A timeout should still propagate
    // even when allow_materialization_failure is set.
    co_await add_random_batches(1);

    std::queue<injected_failure> failures;
    failures.push({.cloud_get = injected_cloud_get_failure::return_timeout});
    produce_placeholders(false, 1, std::move(failures));

    auto underlying = convert_placeholders(make_underlying());
    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 100ms);
    retry_chain_logger logger(test_log, rtc, "materialized_extent_reader_test");

    auto [actual, probe] = co_await cloud_topics::l0::materialize_placeholders(
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      remote,
      cache,
      cloud_topics::allow_materialization_failure::yes,
      rtc,
      logger);

    ASSERT_TRUE_CORO(!actual.has_value());
    ASSERT_TRUE_CORO(actual.error() == cloud_topics::errc::timeout);
}

TEST_F_CORO(materialized_extent_fixture, skip_one_missing_extent_among_many) {
    // With multiple extents, only the missing one should be skipped.
    // The other extents should materialize normally.
    co_await add_random_batches(3);

    // Single failure in the queue: exactly one of the three L0 objects
    // will return notfound, the other two will succeed.
    std::queue<injected_failure> failures;
    failures.push({.cloud_get = injected_cloud_get_failure::return_notfound});
    produce_placeholders(false, 1, std::move(failures));

    auto underlying = convert_placeholders(make_underlying());
    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 100ms);
    retry_chain_logger logger(test_log, rtc, "materialized_extent_reader_test");

    auto [actual, probe] = co_await cloud_topics::l0::materialize_placeholders(
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      remote,
      cache,
      cloud_topics::allow_materialization_failure::yes,
      rtc,
      logger);

    ASSERT_TRUE_CORO(actual.has_value());
    ASSERT_EQ_CORO(actual.value().size(), 2);
}
