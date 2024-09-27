// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "archival/archiver_scheduler_api.h"
#include "archival/archiver_scheduler_impl.h"
#include "archival/logger.h"
#include "archival/types.h"
#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "storage/record_batch_utils.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <gmock/gmock.h>

#include <exception>
#include <memory>
#include <system_error>

inline ss::logger test_log("arch_scheduler_impl_test");

using namespace cloud_storage;
using namespace std::chrono_literals;

namespace archival {

TEST_CORO(archiver_scheduler_impl_test, test_backoff_on_error) {
    // Check the following behavior:
    // - on failure (arg.err is true) maybe_suspend_upload
    //   returns suspended future which becomes ready after
    //   backoff interval
    // - the quota is computed correctly
    // - subsequent calls to maybe_suspend_upload are bumping
    //   up the backoff interval
    // - backoff interval is not growing indefinitely but reaches
    //   limit of 300ms
    std::chrono::milliseconds initial_backoff = 100ms;
    std::chrono::milliseconds max_backoff = 300ms;
    model::ntp expected_ntp(
      model::kafka_namespace,
      model::topic("panda-topic"),
      model::partition_id(137));
    ss::abort_source as;
    basic_retry_chain_node<ss::manual_clock> rtcnode(as);
    scoped_config cfg;
    cfg.get("cloud_storage_upload_loop_initial_backoff_ms")
      .set_value(initial_backoff);
    cfg.get("cloud_storage_upload_loop_max_backoff_ms").set_value(max_backoff);

    archiver_scheduler<ss::manual_clock> scheduler(100, 10);

    co_await scheduler.start();

    co_await scheduler.create_ntp_state(expected_ntp);

    // First call
    auto fut1 = scheduler.maybe_suspend_upload(
      upload_resource_usage<ss::manual_clock>{
        .ntp = expected_ntp,
        .put_requests_used = 2,
        .uploaded_bytes = 20,
        .errc = error_outcome::not_enough_data,
        .archiver_rtc = std::ref(rtcnode),
      });
    ASSERT_FALSE_CORO(fut1.available());

    ss::manual_clock::advance(initial_backoff - 1ms);
    ASSERT_FALSE_CORO(fut1.available());

    ss::manual_clock::advance(3ms);
    auto quota1 = co_await std::move(fut1);

    ASSERT_TRUE_CORO(quota1.has_value());
    ASSERT_EQ_CORO(quota1.value().upload_size_quota, 80);
    ASSERT_EQ_CORO(quota1.value().requests_quota, 8);

    // second call
    auto fut2 = scheduler.maybe_suspend_upload(
      upload_resource_usage<ss::manual_clock>{
        .ntp = expected_ntp,
        .put_requests_used = 3,
        .uploaded_bytes = 30,
        .errc = error_outcome::not_enough_data,
        .archiver_rtc = std::ref(rtcnode),
      });
    ASSERT_FALSE_CORO(fut2.available());

    ss::manual_clock::advance(initial_backoff * 2 - 1ms);
    ASSERT_FALSE_CORO(fut2.available());

    ss::manual_clock::advance(3ms);
    auto quota2 = co_await std::move(fut2);

    ASSERT_TRUE_CORO(quota2.has_value());
    // Some units will be returned
    ASSERT_GT_CORO(quota2.value().upload_size_quota, 50);
    ASSERT_GT_CORO(quota2.value().requests_quota, 5);

    // third call
    // at this stage we should reach max backoff interval of 300ms
    auto fut3 = scheduler.maybe_suspend_upload(
      upload_resource_usage<ss::manual_clock>{
        .ntp = expected_ntp,
        .put_requests_used = 4,
        .uploaded_bytes = 40,
        .errc = error_outcome::not_enough_data,
        .archiver_rtc = std::ref(rtcnode),
      });
    ASSERT_FALSE_CORO(fut3.available());

    ss::manual_clock::advance(initial_backoff * 3 - 1ms);
    ASSERT_FALSE_CORO(fut3.available());

    ss::manual_clock::advance(3ms);
    auto quota3 = co_await std::move(fut3);

    ASSERT_TRUE_CORO(quota3.has_value());
    // Some units will be returned
    ASSERT_GT_CORO(quota3.value().upload_size_quota, 10);
    ASSERT_GT_CORO(quota3.value().requests_quota, 1);

    co_await scheduler.dispose_ntp_state(expected_ntp);

    co_await scheduler.stop();

    co_return;
}

TEST_CORO(archiver_scheduler_impl_test, test_bypass_on_success) {
    // Check the following behavior:
    // - on success (arg.err is false) maybe_suspend_upload
    //   returns ready future
    // - the quota is computed correctly
    std::chrono::milliseconds initial_backoff = 100ms;
    std::chrono::milliseconds max_backoff = 300ms;
    model::ntp expected_ntp(
      model::kafka_namespace,
      model::topic("panda-topic"),
      model::partition_id(137));
    ss::abort_source as;
    basic_retry_chain_node<ss::manual_clock> rtcnode(as);
    scoped_config cfg;
    cfg.get("cloud_storage_upload_loop_initial_backoff_ms")
      .set_value(initial_backoff);
    cfg.get("cloud_storage_upload_loop_max_backoff_ms").set_value(max_backoff);

    archiver_scheduler<ss::manual_clock> scheduler(100, 10);

    co_await scheduler.start();

    co_await scheduler.create_ntp_state(expected_ntp);

    auto fut = scheduler.maybe_suspend_upload(
      upload_resource_usage<ss::manual_clock>{
        .ntp = expected_ntp,
        .put_requests_used = 2,
        .uploaded_bytes = 20,
        .errc = {},
        .archiver_rtc = std::ref(rtcnode),
      });

    // no need to advance time
    auto quota = co_await std::move(fut);

    ASSERT_TRUE_CORO(quota.has_value());
    ASSERT_EQ_CORO(quota.value().upload_size_quota, 80);
    ASSERT_EQ_CORO(quota.value().requests_quota, 8);

    co_await scheduler.dispose_ntp_state(expected_ntp);

    co_await scheduler.stop();

    co_return;
}

TEST_CORO(archiver_scheduler_impl_test, test_saturation_on_success) {
    // Check the following behavior:
    // - on success (arg.err is false) maybe_suspend_upload
    //   is invoked with high enough usage to saturate token bucket
    // - the returned future is not available immediately but
    //   becomes available when the TB is refreshed
    // - the quota is computed correctly
    std::chrono::milliseconds initial_backoff = 100ms;
    std::chrono::milliseconds max_backoff = 300ms;
    model::ntp expected_ntp(
      model::kafka_namespace,
      model::topic("panda-topic"),
      model::partition_id(137));
    ss::abort_source as;
    basic_retry_chain_node<ss::manual_clock> rtcnode(as);
    scoped_config cfg;
    cfg.get("cloud_storage_upload_loop_initial_backoff_ms")
      .set_value(initial_backoff);
    cfg.get("cloud_storage_upload_loop_max_backoff_ms").set_value(max_backoff);

    archiver_scheduler<ss::manual_clock> scheduler(100, 10);

    co_await scheduler.start();

    co_await scheduler.create_ntp_state(expected_ntp);

    auto fut = scheduler.maybe_suspend_upload(
      upload_resource_usage<ss::manual_clock>{
        .ntp = expected_ntp,
        // This is larger than 10 rps rate limit
        .put_requests_used = 20,
        .uploaded_bytes = 0,
        .errc = {},
        .archiver_rtc = std::ref(rtcnode),
      });

    // Waiting for the requests token bucket
    ASSERT_FALSE_CORO(fut.available());

    // Enough time to refill TB
    ss::manual_clock::advance(2s);
    auto quota = co_await std::move(fut);

    ASSERT_TRUE_CORO(quota.has_value());
    ASSERT_EQ_CORO(quota.value().upload_size_quota, 100);
    ASSERT_GT_CORO(quota.value().requests_quota, 0);

    co_await scheduler.dispose_ntp_state(expected_ntp);

    co_await scheduler.stop();

    co_return;
}

TEST_CORO(archiver_scheduler_impl_test, test_abort_requested_while_sleeping) {
    // Check the following behavior:
    // - on error the scheduler suspends the caller for initial_backoff ms
    // - while its suspended the caller triggers abort
    // - the maybe_suspend call returns with shutdown error
    std::chrono::milliseconds initial_backoff = 100ms;
    std::chrono::milliseconds max_backoff = 300ms;
    model::ntp expected_ntp(
      model::kafka_namespace,
      model::topic("panda-topic"),
      model::partition_id(137));
    ss::abort_source as;
    basic_retry_chain_node<ss::manual_clock> rtcnode(as);
    scoped_config cfg;
    cfg.get("cloud_storage_upload_loop_initial_backoff_ms")
      .set_value(initial_backoff);
    cfg.get("cloud_storage_upload_loop_max_backoff_ms").set_value(max_backoff);

    archiver_scheduler<ss::manual_clock> scheduler(100, 10);

    co_await scheduler.start();

    co_await scheduler.create_ntp_state(expected_ntp);

    auto fut = scheduler.maybe_suspend_upload(
      upload_resource_usage<ss::manual_clock>{
        .ntp = expected_ntp,
        .put_requests_used = 1,
        .uploaded_bytes = 1,
        .errc = error_outcome::not_enough_data,
        .archiver_rtc = std::ref(rtcnode),
      });

    ASSERT_FALSE_CORO(fut.available());

    as.request_abort();

    // note that clock is not advanced
    auto err = co_await std::move(fut);
    ASSERT_TRUE_CORO(err.has_error());
    ASSERT_TRUE_CORO(err.error() == error_outcome::shutting_down);

    co_await scheduler.dispose_ntp_state(expected_ntp);

    co_await scheduler.stop();

    co_return;
}

TEST_CORO(archiver_scheduler_impl_test, test_abort_requested_while_throttling) {
    // Check the following behavior:
    // - on success (arg.err is false) maybe_suspend_upload
    //   is invoked with high enough usage to saturate token bucket
    // - the returned future is not available immediately but
    //   becomes available when abort is requested
    std::chrono::milliseconds initial_backoff = 100ms;
    std::chrono::milliseconds max_backoff = 300ms;
    model::ntp expected_ntp(
      model::kafka_namespace,
      model::topic("panda-topic"),
      model::partition_id(137));
    ss::abort_source as;
    basic_retry_chain_node<ss::manual_clock> rtcnode(as);
    scoped_config cfg;
    cfg.get("cloud_storage_upload_loop_initial_backoff_ms")
      .set_value(initial_backoff);
    cfg.get("cloud_storage_upload_loop_max_backoff_ms").set_value(max_backoff);

    archiver_scheduler<ss::manual_clock> scheduler(100, 10);

    co_await scheduler.start();

    co_await scheduler.create_ntp_state(expected_ntp);

    auto fut = scheduler.maybe_suspend_upload(
      upload_resource_usage<ss::manual_clock>{
        .ntp = expected_ntp,
        // This is larger than 10 rps rate limit
        .put_requests_used = 20,
        .uploaded_bytes = 0,
        .errc = {},
        .archiver_rtc = std::ref(rtcnode),
      });

    // Waiting for the requests token bucket
    ASSERT_FALSE_CORO(fut.available());

    as.request_abort();
    auto err = co_await std::move(fut);
    ASSERT_TRUE_CORO(err.has_error());
    ASSERT_TRUE_CORO(err.error() == error_outcome::shutting_down);

    co_await scheduler.dispose_ntp_state(expected_ntp);

    co_await scheduler.stop();

    co_return;
}

} // namespace archival
