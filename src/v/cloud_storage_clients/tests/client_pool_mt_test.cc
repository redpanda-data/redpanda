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
#include "random/generators.h"
#include "test_utils/async.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <boost/test/tools/old/interface.hpp>

#include <deque>
#include <random>

static const cloud_storage_clients::bucket_name_parts test_bucket{
  .name = cloud_storage_clients::plain_bucket_name("test-bucket"),
};

using namespace std::chrono_literals;
using namespace cloud_storage_clients::tests;

using client_pool = cloud_storage_clients::client_pool;

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

    vlog(test_log.debug, "use own connections");
    // deplete own connections
    std::deque<cloud_storage_clients::client_pool::client_lease> leases;
    for (size_t i = 0; i < num_connections_per_shard; i++) {
        leases.push_back(pool.local().acquire(test_bucket, as).get());
    }

    vlog(test_log.debug, "borrow connections from others");
    // deplete others connections
    for (size_t i = 0; i < num_connections_per_shard; i++) {
        leases.push_back(pool.local().acquire(test_bucket, as).get());
    }

    auto fut = ss::smp::invoke_on_others([&pool] {
        return ss::async([&pool] {
            ss::abort_source local_as;
            vlog(test_log.debug, "acquire extra connection on the other shard");
            std::ignore = pool.local().acquire(test_bucket, local_as).get();
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
                    pool.local().acquire(test_bucket, sl.as).get());
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
    auto fut = pool.local().acquire(test_bucket, leases.local().as);
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
          pool.local().acquire(test_bucket, leases.local().as).get());
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
                      pool.local().acquire(test_bucket, sl.as).get());
                }
            });
        })
      .get();

    vlog(test_log.debug, "done borrowing leases");

    auto pending_acquire = pool.local().acquire(test_bucket, as);
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

SEASTAR_THREAD_TEST_CASE(test_client_pool_concurrent_acquire_release) {
    BOOST_REQUIRE(ss::smp::count == 2);

    constexpr size_t num_connections_per_shard = 4;
    constexpr size_t num_workers_per_shard = 3;
    constexpr size_t num_iterations_per_worker = 100;
    constexpr size_t max_leases_per_worker = 3;

    ss::sharded<client_pool> pool;
    auto pool_stop = test_pool_builder
                       .connections_per_shard(num_connections_per_shard)
                       .overdraft_policy(
                         cloud_storage_clients::client_pool_overdraft_policy::
                           borrow_if_empty)
                       .build(pool)
                       .get();

    struct shard_state {
        ss::abort_source as;
    };
    ss::sharded<shard_state> state;
    state.start().get();
    auto state_stop = ss::defer([&state] { state.stop().get(); });

    using lease_t = client_pool::client_lease;
    using leases_t = std::vector<lease_t>;

    // Try to acquire a client with timeout. Returns nullopt on timeout.
    auto try_acquire = [&pool,
                        &state](this auto, ss::lowres_clock::duration timeout)
      -> ss::future<std::optional<lease_t>> {
        auto deadline = ss::lowres_clock::now() + timeout;
        auto f = co_await ss::coroutine::as_future(
          pool.local().acquire(test_bucket, state.local().as, deadline));
        if (f.failed()) {
            f.ignore_ready_future();
            co_return std::nullopt;
        }
        co_return f.get();
    };

    // Release random lease(s) from the vector.
    auto release_random = [](
                            this auto,
                            leases_t& leases,
                            random_generators::rng& gen,
                            size_t count) {
        for (size_t i = 0; i < count && !leases.empty(); i++) {
            auto idx = gen.get_int<size_t>(0, leases.size() - 1);
            leases.erase(leases.begin() + idx);
        }
    };

    // Worker coroutine: repeatedly acquires and releases clients.
    auto worker = [&](this auto)
      -> ss::
        future<> {
            leases_t leases;

            random_generators::rng gen = random_generators::global();
            std::bernoulli_distribution acquire_dist(0.7);
            std::bernoulli_distribution batch_release_dist(0.2);

            for (size_t i = 0; i < num_iterations_per_worker; i++) {
                bool should_acquire
              = leases.empty()
                || (leases.size() < max_leases_per_worker
                    && acquire_dist(gen.engine()));

                if (!should_acquire) {
                    size_t count = (batch_release_dist(gen.engine())
                                    && leases.size() > 1)
                                     ? 2
                                     : 1;
                    release_random(leases, gen, count);
                    co_await ss::yield();
                    continue;
                }

                // Acquire with retry: on timeout, release one lease to relieve
                // pressure.
                while (true) {
                    auto lease = co_await try_acquire(10ms);
                    if (lease) {
                        leases.push_back(std::move(*lease));
                        break;
                    }
                    release_random(leases, gen, 1);
                    co_await ss::sleep(1ms);
                }

                // Simulate work.
                auto work_us = gen.get_int<size_t>(0, 200);
                if (work_us > 0) {
                    co_await ss::sleep(std::chrono::microseconds(work_us));
                } else {
                    co_await ss::yield();
                }
            }

            // Drain remaining leases.
            while (!leases.empty()) {
                leases.pop_back();
                co_await ss::yield();
            }
        };

    // Run concurrent workers on each shard.
    pool
      .invoke_on_all([&](client_pool&) {
          std::vector<ss::future<>> workers;
          workers.reserve(num_workers_per_shard);
          for (size_t w = 0; w < num_workers_per_shard; w++) {
              workers.push_back(worker());
          }
          return ss::when_all_succeed(workers.begin(), workers.end());
      })
      .get();

    // Verify all clients returned to the pool.
    pool
      .invoke_on_all([&](this auto, client_pool& p) -> ss::future<> {
          try {
              co_await tests::cooperative_spin_wait_with_timeout(5s, [&p] {
                  return p.idle_count() == num_connections_per_shard;
              });
          } catch (const ss::timed_out_error&) {
              BOOST_FAIL(
                ssx::sformat(
                  "Timed out waiting for all clients to return to the "
                  "pool on shard {}. Idle: {}, Capacity: {}",
                  ss::this_shard_id(),
                  p.idle_count(),
                  num_connections_per_shard));
          }
      })
      .get();
}
