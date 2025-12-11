/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/tests/client_pool_builder.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <deque>

using namespace std::chrono_literals;
using namespace cloud_storage_clients::tests;

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

static const cloud_storage_clients::bucket_params test_bucket_params{
  .plain_name = cloud_storage_clients::plain_bucket_name("test-bucket"),
};

SEASTAR_THREAD_TEST_CASE(test_client_pool_acquire_blocked_on_another_shard) {
    BOOST_REQUIRE(ss::smp::count == 2);

    constexpr size_t num_connections_per_shard = 4;

    ss::sharded<cloud_storage_clients::client_pool> pool;

    auto stop_guard = test_pool_builder
                        .connections_per_shard(num_connections_per_shard)
                        .overdraft_policy(
                          cloud_storage_clients::client_pool_overdraft_policy::
                            borrow_if_empty)
                        .build(pool)
                        .get();

    ss::abort_source as;

    // Barrier to ensure that all shards are ready to deal connections.
    // Avoids an implementation detail where if the remote shard pool is not
    // ready we will not attempt to borrow from it and instead wait locally.
    pool
      .invoke_on_all([&](cloud_storage_clients::client_pool& p) {
          return p.acquire(test_bucket_params, as).discard_result();
      })
      .get();

    auto pool_stop = ss::defer([&pool] { pool.stop().get(); });

    // Barrier to ensure that all shards are ready to deal connections.
    // Avoids an implementation detail where if the remote shard pool is not
    // ready we will not attempt to borrow from it and instead wait locally.
    pool
      .invoke_on_all([&](cloud_storage_clients::client_pool& p) {
          return p.acquire(test_bucket_params, as).discard_result();
      })
      .get();

    vlog(test_log.debug, "use own connections");
    // deplete own connections
    std::deque<cloud_storage_clients::client_pool::client_lease> leases;
    for (size_t i = 0; i < num_connections_per_shard; i++) {
        leases.push_back(pool.local().acquire(test_bucket_params, as).get());
    }

    vlog(test_log.debug, "borrow connections from others");
    // deplete others connections
    for (size_t i = 0; i < num_connections_per_shard; i++) {
        leases.push_back(pool.local().acquire(test_bucket_params, as).get());
    }

    auto fut = ss::smp::invoke_on_others([&pool] {
        return ss::async([&pool] {
            ss::abort_source local_as;
            vlog(test_log.debug, "acquire extra connection on the other shard");
            std::ignore
              = pool.local().acquire(test_bucket_params, local_as).get();
            vlog(test_log.debug, "connection acquired");
        });
    });

    // Wait for the above future to get scheduled and block.
    pool
      .invoke_on_others([](cloud_storage_clients::client_pool& pool) {
          while (!pool.has_waiters()) {
              return ss::yield();
          }
          return ss::now();
      })
      .get();

    vlog(test_log.debug, "return lease to the current shard");
    leases.pop_front();

    pool
      .invoke_on_all([](cloud_storage_clients::client_pool& pool) {
          while (pool.has_background_operations()) {
              return ss::yield();
          }
          return ss::now();
      })
      .get();
    ;

    // Since we returned to the current shard the future that
    // await for the 'acquire' method to be completed on another shard
    // souldn't become available.
    BOOST_REQUIRE(fut.available() == false);

    vlog(test_log.debug, "return lease to another shard");
    leases.pop_back();
    // The future is supposed to become available.
    try {
        ss::with_timeout(ss::lowres_clock::now() + 1s, std::move(fut)).get();
    } catch (const ss::timed_out_error&) {
        BOOST_FAIL("Timed out");
    }

    vlog(test_log.debug, "return remaining leases");
    leases.clear();
}

SEASTAR_THREAD_TEST_CASE(test_client_pool_acquire_blocked_on_this_shard) {
    BOOST_REQUIRE(ss::smp::count == 2);

    constexpr size_t num_connections_per_shard = 4;

    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto pool_stop = test_pool_builder
                       .connections_per_shard(num_connections_per_shard)
                       .overdraft_policy(
                         cloud_storage_clients::client_pool_overdraft_policy::
                           borrow_if_empty)
                       .build(pool)
                       .get();

    ss::abort_source as;

    struct shard_leases {
        ss::abort_source as;
        std::deque<cloud_storage_clients::client_pool::client_lease> leases;
    };

    ss::sharded<shard_leases> leases;
    leases.start().get();
    auto leases_stop = ss::defer([&leases] { leases.stop().get(); });
    // deplete all connections
    leases
      .invoke_on_all([&pool](shard_leases& sl) mutable {
          return ss::async([&] {
              for (size_t i = 0; i < num_connections_per_shard; i++) {
                  sl.leases.push_back(
                    pool.local().acquire(test_bucket_params, sl.as).get());
              }
          });
      })
      .get();

    vlog(test_log.debug, "connections depleted");
    vlog(test_log.debug, "free resources on other shards");
    // Releasing resources on other shards shouldn't unblock this shard
    leases
      .invoke_on_others([](shard_leases& sl) mutable { sl.leases.pop_back(); })
      .get();

    // Free local resources which should become available to the prevoiusly
    // created future.
    auto fut = pool.local().acquire(test_bucket_params, leases.local().as);
    try {
        ss::with_timeout(ss::lowres_clock::now() + 1s, std::move(fut)).get();
    } catch (const ss::timed_out_error&) {
        BOOST_FAIL("Timed out");
    }
}

SEASTAR_THREAD_TEST_CASE(test_client_pool_acquire_after_leasing_all) {
    BOOST_REQUIRE(ss::smp::count == 2);
    constexpr size_t num_connections_per_shard = 4;

    ss::sharded<cloud_storage_clients::client_pool> pool;

    auto pool_stop = test_pool_builder
                       .connections_per_shard(num_connections_per_shard)
                       .overdraft_policy(
                         cloud_storage_clients::client_pool_overdraft_policy::
                           borrow_if_empty)
                       .build(pool)
                       .get();

    auto pool_no_bg_ops = [&pool] {
        return pool.invoke_on_all([](cloud_storage_clients::client_pool& pool) {
            while (pool.has_background_operations()) {
                return ss::yield();
            }
            return ss::now();
        });
    };

    ss::abort_source as;

    struct shard_leases {
        ss::abort_source as;
        std::deque<cloud_storage_clients::client_pool::client_lease> leases;
    };
    ss::sharded<shard_leases> leases;
    leases.start().get();
    auto leases_stop = ss::defer([&leases] { leases.stop().get(); });

    // Lease all connections from all the shards.
    for (size_t i = 0; i < ss::smp::count * num_connections_per_shard; i++) {
        leases.local().leases.push_back(
          pool.local().acquire(test_bucket_params, leases.local().as).get());
    }

    vlog(test_log.debug, "connections depleted");

    // Release clients from local shard. They are the first in the queue.
    for (size_t i = 0; i < num_connections_per_shard; i++) {
        leases.local().leases.pop_front();
    }

    vlog(test_log.debug, "done returning local leases");

    // Lease local connections from the remote shard.
    leases
      .invoke_on(
        1,
        [&pool](shard_leases& sl) {
            return ss::async([&sl, &pool] {
                for (size_t i = 0; i < num_connections_per_shard; i++) {
                    sl.leases.push_back(
                      pool.local().acquire(test_bucket_params, sl.as).get());
                }
            });
        })
      .get();

    vlog(test_log.debug, "done borrowing leases");

    auto pending_acquire = pool.local().acquire(test_bucket_params, as);
    ss::yield().get();

    if (pending_acquire.available()) {
        BOOST_FAIL("Expecting to get blocked on acquire");
    }

    // Return all connections from the remote shard.
    leases
      .invoke_on(
        1,
        [&pool_no_bg_ops](shard_leases& sl) {
            return ss::async([&sl, &pool_no_bg_ops] {
                for (size_t i = 0; i < num_connections_per_shard; i++) {
                    sl.leases.pop_back();
                    pool_no_bg_ops().get();
                }
            });
        })
      .get();

    // Return connections from the local shard.
    for (size_t i = 0; i < num_connections_per_shard; i++) {
        leases.local().leases.pop_front();
        pool_no_bg_ops().get();
    }
    vlog(test_log.debug, "done returning everything, will try to acquire now");

    try {
        ss::with_timeout(
          ss::lowres_clock::now() + 1s, std::move(pending_acquire))
          .get();
    } catch (const ss::timed_out_error&) {
        BOOST_FAIL("Timed out");
    }
}

SEASTAR_THREAD_TEST_CASE(test_client_pool_multiple_upstreams) {
    // Smoke test.

    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto pool_stop = test_pool_builder.copy().build(pool).get();

    cloud_storage_clients::bucket_params key1{
      .plain_name = cloud_storage_clients::plain_bucket_name("test-bucket-1"),
      .upstream_opts = ada::url_search_params(
        "region=us-east-1&endpoint=my-east-1.localhost"),
    };

    cloud_storage_clients::bucket_params key2{
      .plain_name = cloud_storage_clients::plain_bucket_name("test-bucket-2"),
      .upstream_opts = ada::url_search_params(
        "region=us-west-2&endpoint=my-west-2.localhost"),
    };

    // Acquire concurrently
    // - from multiple shards
    // - from multiple upstreams
    pool
      .invoke_on_all([&](cloud_storage_clients::client_pool& p) {
          return ss::async([&] {
              ss::abort_source as;
              auto fut1 = p.acquire(test_bucket_params, as);
              auto fut2 = p.acquire(key1, as);
              auto fut3 = p.acquire(key2, as);

              // TODO: Verify that clients actually have different upstreams.
              auto r = ss::when_all_succeed(
                         std::move(fut1), std::move(fut2), std::move(fut3))
                         .get();

              BOOST_REQUIRE_EQUAL(
                fmt::format("{}", std::get<0>(r).client),
                "S3 client{{host: localhost, port: 4434}}");
              BOOST_REQUIRE_EQUAL(
                fmt::format("{}", std::get<1>(r).client),
                "S3 client{{host: my-east-1.localhost, port: 4434}}");
              BOOST_REQUIRE_EQUAL(
                fmt::format("{}", std::get<2>(r).client),
                "S3 client{{host: my-west-2.localhost, port: 4434}}");
          });
      })
      .get();

    std::vector<cloud_storage_clients::bucket_params> concurrent_requests{};
    concurrent_requests.reserve(1000);
    for (size_t i = 0; i < 1000; i++) {
        cloud_storage_clients::bucket_params params{
          .plain_name = cloud_storage_clients::plain_bucket_name(
            fmt::format("test-bucket-{}", i)),
          .upstream_opts = ada::url_search_params(
            i % 2 == 0 ? "region=us-east-1&endpoint=my-east-1.localhost"
                       : "region=us-west-2&endpoint=my-west-2.localhost"),
        };
        concurrent_requests.push_back(std::move(params));
    }

    for (int i = 0; i < 3; i++) {
        pool
          .invoke_on_all([&](cloud_storage_clients::client_pool& p) {
              return ss::parallel_for_each(
                concurrent_requests,
                [&](cloud_storage_clients::bucket_params& params) {
                    return ss::async([&] {
                        ss::abort_source as;
                        auto lease = p.acquire(params, as).get();

                        BOOST_REQUIRE_EQUAL(
                          fmt::format("{}", lease.client),
                          params.upstream_opts.get("endpoint").value()
                              == "my-east-1.localhost"
                            ? "S3 client{{host: "
                              "my-east-1.localhost, port: "
                              "4434}}"
                            : "S3 client{{host: "
                              "my-west-2.localhost, port: "
                              "4434}}");

                        // Hold the lease for a bit to increase
                        // chance of contention.
                        ss::sleep(3ms).get();
                    });
                });
          })
          .get();

        ss::yield().get();
    }

    BOOST_REQUIRE_EQUAL(pool.local().idle_count(), pool.local().capacity());
}
