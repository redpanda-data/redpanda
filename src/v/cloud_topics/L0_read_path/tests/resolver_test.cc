// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/L0_read_path/resolver.h"
#include "cloud_topics/L0_read_path/tests/placeholder_extent_fixture.h"
#include "cloud_topics/core/read_pipeline.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/interfaces/tests/mocks.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/later.hh>

#include <chrono>
#include <exception>
#include <queue>
#include <stdexcept>

namespace cloud_topics = experimental::cloud_topics;

ss::logger test_log("resolver_test_log");

struct fragmented_vector_consumer {
    ss::future<ss::stop_iteration> operator()(model::record_batch rb) {
        target->push_back(std::move(rb));
        co_return ss::stop_iteration::no;
    }

    void end_of_stream() {}

    fragmented_vector<model::record_batch>* target;
};

TEST_F_CORO(placeholder_extent_fixture, resolver_test) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(true, 1);

    auto ntp = model::controller_ntp;
    auto base = partition.front().base_offset();
    auto last = partition.front().last_offset();

    auto part = ss::make_shared<partition_mock>();
    auto pm = ss::make_shared<partition_manager_mock>();

    model::tx_range expected_tx(
      model::producer_identity(10, 0), model::offset(42), model::offset(137));

    part->expect_aborted_transactions(
      base, last, fragmented_vector<model::tx_range>({expected_tx}));
    part->expect_make_reader(make_log_reader());

    pm->expect_get_partition(ntp, part);

    cloud_topics::core::read_pipeline<> pipeline;

    cloud_topics::resolver resolver(
      &pipeline,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      std::move(pm));

    co_await resolver.start();

    auto cfg = storage::log_reader_config(
      base, last, ss::default_priority_class());

    auto reader_tx = co_await pipeline.make_reader(ntp, cfg, 1s);

    ASSERT_TRUE_CORO(reader_tx.has_value());
    ASSERT_TRUE_CORO(reader_tx.value().tx.back() == expected_tx);

    fragmented_vector<model::record_batch> actual;
    fragmented_vector_consumer consumer{
      .target = &actual,
    };
    co_await std::move(reader_tx.value().reader)
      .consume(consumer, model::no_timeout);

    ASSERT_TRUE_CORO(actual.back() == expected.back());

    co_await pipeline.stop();
    co_await resolver.stop();
}

ss::future<> aborted_tx_failure_test(
  placeholder_extent_fixture& fx,
  std::exception_ptr injected_error,
  experimental::cloud_topics::errc resulting_error) {
    const int num_batches = 1;
    co_await fx.add_random_batches(num_batches);
    fx.produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::noop}}));

    auto ntp = model::controller_ntp;
    auto base = fx.partition.front().base_offset();
    auto last = fx.partition.front().last_offset();

    auto part = ss::make_shared<partition_mock>();
    auto pm = ss::make_shared<partition_manager_mock>();

    part->expect_aborted_transactions_fail(base, last, injected_error);
    part->expect_make_reader(fx.make_log_reader());

    pm->expect_get_partition(ntp, part);

    cloud_topics::core::read_pipeline<> pipeline;

    cloud_topics::resolver resolver(
      &pipeline,
      cloud_storage_clients::bucket_name("foo"),
      &fx.remote,
      &fx.cache,
      std::move(pm));

    co_await resolver.start();

    auto cfg = storage::log_reader_config(
      base, last, ss::default_priority_class());

    auto reader_tx = co_await pipeline.make_reader(ntp, cfg, 1s);

    ASSERT_TRUE_CORO(reader_tx.has_error());
    ASSERT_TRUE_CORO(reader_tx.error() == resulting_error);

    co_await pipeline.stop();
    co_await resolver.stop();
}

TEST_F_CORO(placeholder_extent_fixture, aborted_transactions_failed) {
    co_await aborted_tx_failure_test(
      *this,
      std::make_exception_ptr(std::runtime_error("fiasco")),
      experimental::cloud_topics::errc::unexpected_failure);
}

TEST_F_CORO(placeholder_extent_fixture, aborted_transactions_shutdown) {
    co_await aborted_tx_failure_test(
      *this,
      std::make_exception_ptr(
        experimental::cloud_topics::core::pipeline_abort_requested()),
      experimental::cloud_topics::errc::shutting_down);
}

ss::future<>
get_partition_failure(placeholder_extent_fixture& fx, bool shutdown) {
    const int num_batches = 1;
    co_await fx.add_random_batches(num_batches);
    fx.produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::noop}}));

    auto ntp = model::controller_ntp;
    auto base = fx.partition.front().base_offset();
    auto last = fx.partition.front().last_offset();

    auto pm = ss::make_shared<partition_manager_mock>();

    if (shutdown) {
        pm->expect_get_partition_fail(ntp, ss::gate_closed_exception());
    } else {
        pm->expect_get_partition(ntp, nullptr);
    }

    cloud_topics::core::read_pipeline<> pipeline;

    cloud_topics::resolver resolver(
      &pipeline,
      cloud_storage_clients::bucket_name("foo"),
      &fx.remote,
      &fx.cache,
      std::move(pm));

    co_await resolver.start();

    auto cfg = storage::log_reader_config(
      base, last, ss::default_priority_class());

    auto reader_tx = co_await pipeline.make_reader(ntp, cfg, 1s);

    ASSERT_TRUE_CORO(reader_tx.has_error());
    if (shutdown) {
        ASSERT_TRUE_CORO(
          reader_tx.error() == experimental::cloud_topics::errc::shutting_down);
    } else {
        ASSERT_TRUE_CORO(
          reader_tx.error()
          == experimental::cloud_topics::errc::unexpected_failure);
    }

    co_await pipeline.stop();
    co_await resolver.stop();
}

TEST_F_CORO(placeholder_extent_fixture, partition_moved) {
    co_await get_partition_failure(*this, false);
}

TEST_F_CORO(placeholder_extent_fixture, partition_manager_shutdown) {
    co_await get_partition_failure(*this, true);
}

ss::future<> make_reader_failed(
  placeholder_extent_fixture& fx,
  std::exception_ptr actual_exception,
  experimental::cloud_topics::errc expected_error) {
    const int num_batches = 1;
    co_await fx.add_random_batches(num_batches);
    fx.produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::noop}}));

    auto ntp = model::controller_ntp;
    auto base = fx.partition.front().base_offset();
    auto last = fx.partition.front().last_offset();

    auto part = ss::make_shared<partition_mock>();
    auto pm = ss::make_shared<partition_manager_mock>();

    part->expect_make_reader_fail(actual_exception);

    pm->expect_get_partition(ntp, part);

    cloud_topics::core::read_pipeline<> pipeline;

    cloud_topics::resolver resolver(
      &pipeline,
      cloud_storage_clients::bucket_name("foo"),
      &fx.remote,
      &fx.cache,
      std::move(pm));

    co_await resolver.start();

    auto cfg = storage::log_reader_config(
      base, last, ss::default_priority_class());

    auto reader_tx = co_await pipeline.make_reader(ntp, cfg, 1s);

    ASSERT_TRUE_CORO(reader_tx.has_error());
    ASSERT_TRUE_CORO(reader_tx.error() == expected_error);

    co_await pipeline.stop();
    co_await resolver.stop();
}

TEST_F_CORO(placeholder_extent_fixture, partition_make_reader_failure) {
    co_await make_reader_failed(
      *this,
      std::make_exception_ptr(std::runtime_error("fiasco")),
      experimental::cloud_topics::errc::unexpected_failure);
}

TEST_F_CORO(placeholder_extent_fixture, partition_make_reader_shutdown) {
    co_await make_reader_failed(
      *this,
      std::make_exception_ptr(ss::gate_closed_exception()),
      experimental::cloud_topics::errc::shutting_down);
}

// TODO: add cancel request test
// fut = pipeline.make_reader is called
// pipeline.stop() is called
// co_await fut should throw shutdown error

TEST_F_CORO(placeholder_extent_fixture, request_cancel_test) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      true,
      1,
      std::queue<injected_failure>(
        {{.is_cached = injected_is_cached_failure::noop}}));

    auto ntp = model::controller_ntp;
    auto base = partition.front().base_offset();
    auto last = partition.front().last_offset();

    auto part = ss::make_shared<partition_mock>();
    auto pm = ss::make_shared<partition_manager_mock>();

    model::tx_range expected_tx(
      model::producer_identity(10, 0), model::offset(42), model::offset(137));

    part->expect_aborted_transactions(
      base, last, fragmented_vector<model::tx_range>({expected_tx}));
    part->expect_make_reader(make_log_reader());

    pm->expect_get_partition(ntp, part);

    cloud_topics::core::read_pipeline<> pipeline;

    cloud_topics::resolver resolver(
      &pipeline,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache,
      std::move(pm));

    co_await resolver.start();

    auto cfg = storage::log_reader_config(
      base, last, ss::default_priority_class());

    auto reader_fut = pipeline.make_reader(ntp, cfg, 1s);

    co_await pipeline.stop();

    // Check that the request is cancelled
    auto res = co_await std::move(reader_fut);
    ASSERT_TRUE_CORO(res.has_error());

    co_await resolver.stop();
}
