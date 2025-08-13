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

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <boost/test/tools/interface.hpp>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

ss::logger test_log("test-log");
static const uint16_t httpd_port_number = 4434;
static constexpr const char* httpd_host_name = "localhost";

static cloud_storage_clients::s3_configuration transport_configuration() {
    net::unresolved_address server_addr(httpd_host_name, httpd_port_number);
    cloud_storage_clients::s3_configuration conf;
    conf.uri = cloud_storage_clients::access_point_uri(httpd_host_name);
    conf.access_key = cloud_roles::public_key_str("access-key");
    conf.secret_key = cloud_roles::private_key_str("secret-key");
    conf.region = cloud_roles::aws_region_name("us-east-1");
    conf.service = cloud_roles::aws_service_name("s3");
    conf.url_style = cloud_storage_clients::s3_url_style::virtual_host;
    conf.server_addr = server_addr;
    conf._probe = ss::make_shared<cloud_storage_clients::client_probe>(
      net::metrics_disabled::yes,
      net::public_metrics_disabled::yes,
      cloud_roles::aws_region_name{"region"},
      cloud_storage_clients::endpoint_url{"endpoint"});
    return conf;
}

SEASTAR_THREAD_TEST_CASE(test_client_pool_acquire_abortable) {
    auto sconf = ss::sharded_parameter([] {
        auto conf = transport_configuration();
        return conf;
    });
    auto conf = transport_configuration();

    ss::sharded<cloud_storage_clients::client_pool> pool;
    size_t num_connections_per_shard = 0;
    pool
      .start(
        num_connections_per_shard,
        sconf,
        cloud_storage_clients::client_pool_overdraft_policy::borrow_if_empty)
      .get();

    pool
      .invoke_on_all([&conf](cloud_storage_clients::client_pool& p) {
          auto cred = cloud_roles::aws_credentials{
            conf.access_key.value(),
            conf.secret_key.value(),
            std::nullopt,
            conf.region,
            cloud_roles::aws_service_name{"s3"}};
          p.load_credentials(cred);
      })
      .get();
    auto pool_stop = ss::defer([&pool] { pool.stop().get(); });

    ss::abort_source as;

    auto f = pool.local().acquire(as);
    while (!pool.local().has_waiters()) {
        ss::yield().get();
    }

    BOOST_TEST_REQUIRE(
      !f.available(), "acquire should be blocked as pool is empty");

    as.request_abort();

    BOOST_REQUIRE_THROW(f.get(), ss::abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(test_client_pool_acquire_with_timeout) {
    auto sconf = ss::sharded_parameter([] {
        auto conf = transport_configuration();
        return conf;
    });
    auto conf = transport_configuration();

    ss::sharded<cloud_storage_clients::client_pool> pool;
    size_t num_connections_per_shard = 1;
    pool
      .start(
        num_connections_per_shard,
        sconf,
        cloud_storage_clients::client_pool_overdraft_policy::wait_if_empty)
      .get();

    pool
      .invoke_on_all([&conf](cloud_storage_clients::client_pool& p) {
          auto cred = cloud_roles::aws_credentials{
            conf.access_key.value(),
            conf.secret_key.value(),
            std::nullopt,
            conf.region};
          p.load_credentials(cred);
      })
      .get();
    auto pool_stop = ss::defer([&pool] { pool.stop().get(); });

    ss::abort_source as;
    using namespace std::chrono_literals;

    {
        auto lease = pool.local()
                       .acquire_with_timeout(
                         as, ss::lowres_clock::now() + 100ms)
                       .get();

        // The request should fail w/in 500ms due to lease expiry
        // Note that the default timeout for the request itself is 5s
        auto res = ss::with_timeout(
                     ss::lowres_clock::now() + 500ms,
                     lease.client->list_objects(random_test_bucket_name()))
                     .get();

        BOOST_REQUIRE(res.has_error());
        BOOST_REQUIRE_EQUAL(
          res.error(), cloud_storage_clients::error_outcome::retry);

        // return the lease to the pool
    }

    {
        auto lease = pool.local().acquire(as).get();

        auto f = ss::with_timeout(
          ss::lowres_clock::now() + 500ms,
          lease.client->list_objects(random_test_bucket_name()));

        // This time the lease never expires, so internally we should keep
        // trying to connect for at least 500ms.
        BOOST_REQUIRE_THROW(f.get(), ss::timed_out_error);
    }

    {
        // check that passing time_point::max for timeout will skip watchdog
        // creation to avoid overflow in sleep_abortable

        auto lease = pool.local()
                       .acquire_with_timeout(
                         as, ss::lowres_clock::time_point::max())
                       .get();

        BOOST_REQUIRE_EQUAL(lease._wd, nullptr);
    }
}

SEASTAR_THREAD_TEST_CASE(test_client_pool_acquire_timeout) {
    auto sconf = ss::sharded_parameter([] {
        auto conf = transport_configuration();
        return conf;
    });
    auto conf = transport_configuration();

    ss::sharded<cloud_storage_clients::client_pool> pool;
    size_t num_connections_per_shard = 0;
    pool
      .start(
        num_connections_per_shard,
        sconf,
        cloud_storage_clients::client_pool_overdraft_policy::borrow_if_empty)
      .get();

    pool
      .invoke_on_all([&conf](cloud_storage_clients::client_pool& p) {
          auto cred = cloud_roles::aws_credentials{
            conf.access_key.value(),
            conf.secret_key.value(),
            std::nullopt,
            conf.region};
          p.load_credentials(cred);
      })
      .get();
    auto pool_stop = ss::defer([&pool] { pool.stop().get(); });

    {
        // acquire should time out. no abort required.
        ss::abort_source as;

        auto f = pool.local().acquire(as, ss::lowres_clock::now() + 100ms);
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

        auto f = pool.local().acquire(as, ss::lowres_clock::time_point::max());
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
        auto f = pool.local().acquire_with_timeout(
          as, ss::lowres_clock::now() + 100ms);
        ss::sleep(500ms).get();
        as.request_abort();
        BOOST_REQUIRE_THROW(f.get(), ss::abort_requested_exception);
    }

    {
        // passing a deadline in the past should behave sanely
        ss::abort_source as;
        BOOST_REQUIRE_THROW(
          pool.local().acquire(as, ss::lowres_clock::time_point::min()).get(),
          ss::timed_out_error);
    }
}
