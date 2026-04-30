/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/tests/client_pool_builder.h"
#include "config/configuration.h"
#include "test_utils/async.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <boost/test/tools/interface.hpp>

using namespace std::chrono_literals;
using namespace cloud_storage_clients::tests;

static const cloud_storage_clients::bucket_name_parts test_bucket{
  .name = cloud_storage_clients::plain_bucket_name("test-bucket"),
};

ss::logger test_log("test-log");
static const uint16_t httpd_port_number = 4434;
static constexpr const char* httpd_host_name = "localhost";

static cloud_storage_clients::s3_configuration client_configuration() {
    net::unresolved_address server_addr(httpd_host_name, httpd_port_number);
    cloud_storage_clients::s3_configuration conf;
    conf.uri = cloud_storage_clients::access_point_uri(httpd_host_name);
    conf.access_key = cloud_roles::public_key_str("access-key");
    conf.secret_key = cloud_roles::private_key_str("secret-key");
    conf.region = cloud_roles::aws_region_name("us-east-1");
    conf.service = cloud_roles::aws_service_name("s3");
    conf.url_style = cloud_storage_clients::s3_url_style::virtual_host;
    conf.server_addr = server_addr;
    return conf;
}

static const client_pool_builder test_pool_builder{client_configuration()};

namespace {
static void wait_for_capped_waiters(
  cloud_storage_clients::client_pool& pool, size_t expected) {
    RPTEST_REQUIRE_EVENTUALLY(
      2s, [&] { return pool.capped_waiters_count() == expected; });
}
} // namespace

SEASTAR_THREAD_TEST_CASE(test_client_pool_acquire_abortable) {
    constexpr size_t num_connections_per_shard = 0;

    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto stop_guard = test_pool_builder
                        .connections_per_shard(num_connections_per_shard)
                        .overdraft_policy(
                          cloud_storage_clients::client_pool_overdraft_policy::
                            borrow_if_empty)
                        .build(pool)
                        .get();
    ss::abort_source as;

    auto f = pool.local().acquire(test_bucket, as);
    while (!pool.local().has_waiters()) {
        ss::yield().get();
    }

    BOOST_TEST_REQUIRE(
      !f.available(), "acquire should be blocked as pool is empty");

    as.request_abort();

    BOOST_REQUIRE_THROW(f.get(), ss::abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(test_client_pool_acquire_with_timeout) {
    constexpr size_t num_connections_per_shard = 1;

    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto stop_guard
      = test_pool_builder.connections_per_shard(num_connections_per_shard)
          .overdraft_policy(
            cloud_storage_clients::client_pool_overdraft_policy::wait_if_empty)
          .build(pool)
          .get();

    auto pool_stop = ss::defer([&pool] { pool.stop().get(); });

    ss::abort_source as;
    using namespace std::chrono_literals;

    {
        auto lease
          = pool.local().acquire_with_timeout(test_bucket, as, 100ms).get();

        // The request should fail w/in 500ms due to lease expiry
        // Note that the default timeout for the request itself is 5s
        auto res = ss::with_timeout(
                     ss::lowres_clock::now() + 500ms,
                     lease.client->list_objects(
                       random_test_plain_bucket_name()))
                     .get();

        BOOST_REQUIRE(res.has_error());
        BOOST_REQUIRE_EQUAL(
          res.error(), cloud_storage_clients::error_outcome::retry);

        // return the lease to the pool
    }

    {
        auto lease = pool.local().acquire(test_bucket, as).get();

        auto f = ss::with_timeout(
          ss::lowres_clock::now() + 500ms,
          lease.client->list_objects(random_test_plain_bucket_name()));

        // This time the lease never expires, so internally we should keep
        // trying to connect for at least 500ms.
        BOOST_REQUIRE_THROW(f.get(), ss::timed_out_error);
    }

    {
        // check that passing time_point::max for timeout will skip watchdog
        // creation to avoid overflow in sleep_abortable

        auto lease = pool.local()
                       .acquire_with_timeout(
                         test_bucket, as, ss::lowres_clock::duration::max())
                       .get();

        BOOST_REQUIRE_EQUAL(lease._wd, nullptr);
    }
}

SEASTAR_THREAD_TEST_CASE(test_client_pool_acquire_timeout) {
    constexpr size_t num_connections_per_shard = 0;
    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto stop_guard = test_pool_builder
                        .connections_per_shard(num_connections_per_shard)
                        .overdraft_policy(
                          cloud_storage_clients::client_pool_overdraft_policy::
                            borrow_if_empty)
                        .build(pool)
                        .get();

    {
        // acquire should time out. no abort required.
        ss::abort_source as;

        auto f = pool.local().acquire(
          test_bucket, as, ss::lowres_clock::now() + 100ms);
        while (!pool.local().has_waiters()) {
            ss::yield().get();
        }

        BOOST_TEST_REQUIRE(
          !f.available(), "acquire should be blocked as pool is empty");

        BOOST_REQUIRE_THROW(f.get(), ss::timed_out_error);
    }

    {
        // time_point::max should be fine here. we just won't time out anytime
        // soon.
        ss::abort_source as;

        auto f = pool.local().acquire(
          test_bucket, as, ss::lowres_clock::time_point::max());
        while (!pool.local().has_waiters()) {
            ss::yield().get();
        }

        BOOST_TEST_REQUIRE(
          !f.available(), "acquire should be blocked as pool is empty");

        as.request_abort();

        BOOST_REQUIRE_THROW(f.get(), ss::abort_requested_exception);
    }

    {
        // call through acquire_with_timeout
        // NOTE: we currently don't propage a deadline into pool::acquire, so
        // the idea here is that acquire_with_timeout should hang indefinitely
        // (because the pool is fully subscribed). So even after exceeding the
        // provided lease timeout, only the abort source can short circuit
        // client acquisition.

        ss::abort_source as;
        auto f = pool.local().acquire_with_timeout(test_bucket, as, 100ms);
        ss::sleep(500ms).get();
        as.request_abort();
        BOOST_REQUIRE_THROW(f.get(), ss::abort_requested_exception);
    }

    {
        // passing a deadline in the past should behave sanely
        ss::abort_source as;
        BOOST_REQUIRE_THROW(
          pool.local()
            .acquire(test_bucket, as, ss::lowres_clock::time_point::min())
            .get(),
          ss::timed_out_error);
    }
}

SEASTAR_THREAD_TEST_CASE(test_client_pool_acquire_self_configure_deadline) {
    // Test that acquire times out within the specified deadline while waiting
    // for self-configuration to complete.
    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto stop_guard = test_pool_builder.skip_start(true).build(pool).get();

    ss::abort_source as;
    auto f = pool.local().acquire(
      test_bucket, as, ss::lowres_clock::now() - 100ms);

    ss::with_timeout(
      ss::lowres_clock::now() + 1s,
      [&]() {
          BOOST_REQUIRE_THROW(f.get(), ss::timed_out_error);

          return ss::now();
      }())
      .get();
}

SEASTAR_THREAD_TEST_CASE(test_client_pool_acquire_self_configure_abortable) {
    // Test that acquire can be aborted while waiting for self-configuration to
    // complete.
    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto stop_guard = test_pool_builder.skip_start(true).build(pool).get();

    ss::abort_source as;
    auto f = pool.local().acquire(test_bucket, as);

    while (!pool.local().has_waiters()) {
        ss::yield().get();
    }

    as.request_abort();

    BOOST_REQUIRE_THROW(f.get(), ss::abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(test_client_pool_max_upstreams_limit) {
    // The upstream registry enforces a maximum of 10 upstreams. The default
    // upstream (empty endpoint/region) counts as one, leaving room for 9
    // dynamic upstreams.

    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto stop_guard = test_pool_builder.build(pool).get();

    ss::abort_source as;

    // Create 9 dynamic upstreams (default upstream already exists)
    for (size_t i = 0; i < 9; ++i) {
        cloud_storage_clients::bucket_name_parts bucket{
          .name = cloud_storage_clients::plain_bucket_name("test-bucket"),
          .params = {{"endpoint", fmt::format("endpoint-{}.localhost", i)}},
        };
        auto lease = pool.local().acquire(bucket, as).get();
    }

    // The 10th dynamic upstream should fail
    cloud_storage_clients::bucket_name_parts bucket_over_limit{
      .name = cloud_storage_clients::plain_bucket_name("test-bucket"),
      .params = {{"endpoint", "endpoint-over-limit.localhost"}},
    };

    BOOST_REQUIRE_EXCEPTION(
      pool.local().acquire(bucket_over_limit, as).get(),
      std::exception,
      [](const std::exception& e) {
          return std::string_view(e.what()).find("registry entry limit")
                 != std::string_view::npos;
      });
}

SEASTAR_THREAD_TEST_CASE(test_capped_class_caps_concurrent_leases) {
    // pool size 10, pct=20 -> capped_capacity=2. Three capped acquires;
    // only 2 progress at once. Priority callers bypass the
    // gate and consume the remaining 8 pool slots without queuing.
    constexpr size_t conn_per_shard = 10;
    auto& pct = config::shard_local_cfg().cloud_storage_max_capped_pool_pct;
    pct.set_value(20);
    auto cfg_reset = ss::defer([&pct] { pct.reset(); });

    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto stop_guard = test_pool_builder.connections_per_shard(conn_per_shard)
                        .build(pool)
                        .get();

    ss::abort_source as;
    std::deque<cloud_storage_clients::client_pool::client_lease> c_leases;
    c_leases.push_back(pool.local()
                         .acquire(
                           test_bucket,
                           as,
                           std::nullopt,
                           cloud_storage_clients::lease_class::capped)
                         .get());
    c_leases.push_back(pool.local()
                         .acquire(
                           test_bucket,
                           as,
                           std::nullopt,
                           cloud_storage_clients::lease_class::capped)
                         .get());

    auto f3 = pool.local().acquire(
      test_bucket,
      as,
      std::nullopt,
      cloud_storage_clients::lease_class::capped);

    // capacity exhausted, f3 should wait
    wait_for_capped_waiters(pool.local(), 1);
    BOOST_TEST_REQUIRE(
      !f3.available(),
      "third capped acquire must wait at capped-budget semaphore");

    // Priority bypass works: pool still has 8 idle clients.
    auto ls = pool.local()
                .acquire(
                  test_bucket,
                  as,
                  std::nullopt,
                  cloud_storage_clients::lease_class::priority)
                .get();

    // Drop l1 -> f3 unblocks.
    c_leases.pop_back();
    auto c_ls = std::move(f3).get();
}

SEASTAR_THREAD_TEST_CASE(test_capped_waiter_wakes_on_shutdown) {
    // pool size 4, pct=25 -> capped_capacity=1. Hold the only unit,
    // try to acquire another, then call shutdown_connections().
    // Waiter should wake with gate_closed_exception.
    constexpr size_t conn_per_shard = 4;
    auto& pct = config::shard_local_cfg().cloud_storage_max_capped_pool_pct;
    pct.set_value(25);
    auto cfg_reset = ss::defer([&pct] { pct.reset(); });

    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto stop_guard = test_pool_builder.connections_per_shard(conn_per_shard)
                        .build(pool)
                        .get();

    ss::abort_source as;
    auto held = pool.local()
                  .acquire(
                    test_bucket,
                    as,
                    std::nullopt,
                    cloud_storage_clients::lease_class::capped)
                  .get();

    auto waiter = pool.local().acquire(
      test_bucket,
      as,
      std::nullopt,
      cloud_storage_clients::lease_class::capped);

    wait_for_capped_waiters(pool.local(), 1);
    BOOST_TEST_REQUIRE(!waiter.available());

    pool.local().shutdown_connections();
    BOOST_REQUIRE_THROW(waiter.get(), ss::gate_closed_exception);
}

SEASTAR_THREAD_TEST_CASE(test_capped_pct_grows_capacity_via_signal) {
    // pool size 4, pct=25 -> capped_capacity=1.
    // - Hold the only unit
    // - Try to acquire another (blocks)
    // - then bump pct to 50 -> capped_capacity=2.
    // - Waiter should unblock.
    constexpr size_t conn_per_shard = 4;
    auto& pct = config::shard_local_cfg().cloud_storage_max_capped_pool_pct;
    pct.set_value(25);
    auto cfg_reset = ss::defer([&pct] { pct.reset(); });

    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto stop_guard = test_pool_builder.connections_per_shard(conn_per_shard)
                        .build(pool)
                        .get();

    ss::abort_source as;
    auto held = pool.local()
                  .acquire(
                    test_bucket,
                    as,
                    std::nullopt,
                    cloud_storage_clients::lease_class::capped)
                  .get();
    auto waiter = pool.local().acquire(
      test_bucket,
      as,
      std::nullopt,
      cloud_storage_clients::lease_class::capped);

    wait_for_capped_waiters(pool.local(), 1);
    BOOST_TEST_REQUIRE(!waiter.available());

    // watch callback fires synchronously with set_value and immediately signals
    // capped_budget semaphore.
    pct.set_value(50);

    auto woken
      = ss::with_timeout(ss::lowres_clock::now() + 1s, std::move(waiter)).get();
}

SEASTAR_THREAD_TEST_CASE(test_capped_pct_shrinks_capacity_via_consume) {
    // pool size 4, pct=100 -> capped_capacity=4.
    // - Hold all 4
    // - Decrease pct to 25
    // - capped_capacity=1
    // - the semaphore counter goes negative (0 - 3 = -3)
    // - a fresh capped acquire blocks until enough leases
    //   drop to bring it positive.
    constexpr size_t conn_per_shard = 4;
    auto& pct = config::shard_local_cfg().cloud_storage_max_capped_pool_pct;
    pct.set_value(100);
    auto cfg_reset = ss::defer([&pct] { pct.reset(); });

    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto stop_guard = test_pool_builder.connections_per_shard(conn_per_shard)
                        .build(pool)
                        .get();

    ss::abort_source as;
    auto acquire_capped = [&] {
        return pool.local()
          .acquire(
            test_bucket,
            as,
            std::nullopt,
            cloud_storage_clients::lease_class::capped)
          .get();
    };
    std::deque<cloud_storage_clients::client_pool::client_lease> c_leases;
    c_leases.push_back(acquire_capped());
    c_leases.push_back(acquire_capped());
    c_leases.push_back(acquire_capped());
    c_leases.push_back(acquire_capped());

    // Counter goes to -3
    pct.set_value(25);

    auto pending = pool.local().acquire(
      test_bucket,
      as,
      std::nullopt,
      cloud_storage_clients::lease_class::capped);
    wait_for_capped_waiters(pool.local(), 1);
    BOOST_TEST_REQUIRE(!pending.available());

    // Releasing 3 of the 4 leases brings the (negative) counter back
    // to zero; the pending acquire should still be blocked.
    c_leases.pop_back();
    c_leases.pop_back();
    c_leases.pop_back();

    // budget sem gets signaled from deleter, but count is still 0 at this point
    BOOST_TEST_REQUIRE(pool.local().capped_waiters_count() == 1);
    BOOST_TEST_REQUIRE(!pending.available());

    // Dropping the 4th lease bumps the counter to 1; waiter unblocks.
    c_leases.pop_back();
    auto unblocked = std::move(pending).get();
}
