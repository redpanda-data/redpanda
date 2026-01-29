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
