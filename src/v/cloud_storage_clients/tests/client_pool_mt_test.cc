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
#include "config/configuration.h"
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

#include <algorithm>
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
    conf.url_style = cloud_storage_clients::s3_url_style::path;
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

SEASTAR_THREAD_TEST_CASE(test_capped_does_not_borrow_from_peer) {
    // capped leases must stay on their own shard. Saturate the
    // local pool with priority leases (leaving the capped-budget
    // untouched) and confirm a capped acquire parks at the cvar instead of
    // borrowing across shards. A priority acquire in the same
    // state still borrows, by way of comparison.
    BOOST_REQUIRE(ss::smp::count == 2);

    constexpr size_t num_connections_per_shard = 2;
    auto& pct = config::shard_local_cfg().cloud_storage_max_capped_pool_pct;
    pct.set_value(50); // capped_capacity = 1
    auto cfg_reset = ss::defer([&pct] { pct.reset(); });

    ss::sharded<client_pool> pool;
    auto stop_guard = test_pool_builder
                        .connections_per_shard(num_connections_per_shard)
                        .overdraft_policy(
                          cloud_storage_clients::client_pool_overdraft_policy::
                            borrow_if_empty)
                        .build(pool)
                        .get();

    // Wait for both shards' pools to populate.
    pool
      .invoke_on_all([N = num_connections_per_shard](client_pool& p) {
          return ss::async([&p, N] {
              while (p.idle_count() != N) {
                  ss::yield().get();
              }
          });
      })
      .get();

    ss::abort_source as;

    // Saturate shard 0's local pool with priority leases.
    // capped-budget remains untouched (capacity=1, in-flight=0).
    std::deque<client_pool::client_lease> ls_leases;
    for (size_t i = 0; i < num_connections_per_shard; i++) {
        ls_leases.push_back(pool.local()
                              .acquire(
                                test_bucket,
                                as,
                                std::nullopt,
                                cloud_storage_clients::lease_class::priority)
                              .get());
    }

    // A capped acquire on shard 0 has cap budget but no idle local clients.
    // Without the cross-shard restriction it would borrow from shard 1.
    // With the restriction it must wait at the cvar.
    auto capped_fut = pool.local().acquire(
      test_bucket,
      as,
      std::nullopt,
      cloud_storage_clients::lease_class::capped);

    // Poll until the capped lease reaches the cvar. has_waiters()
    // covers the capped-budget semaphore as well as the cvar /
    // pool-ready-barrier, so we sanity-check the capped waiters count
    // post facto.
    pool
      .invoke_on(
        0,
        [](client_pool& p) {
            return ss::async([&p] {
                while (!p.has_waiters()) {
                    ss::yield().get();
                }
            });
        })
      .get();

    BOOST_TEST_REQUIRE(
      !capped_fut.available(),
      "capped must wait locally rather than borrow from peer");
    BOOST_TEST_REQUIRE(
      pool.local().capped_waiters_count() == 0,
      "capped must be parked at the cvar, not at the capped-budget gate");

    // Shard 1's pool should be fully idle: the capped lease did not pull a
    // connection across shards.
    auto shard1_idle
      = pool.invoke_on(1, [](client_pool& other) { return other.idle_count(); })
          .get();
    BOOST_REQUIRE_EQUAL(shard1_idle, num_connections_per_shard);

    // Sanity: a priority acquire on shard 0 borrows from shard 1.
    auto ls_borrow = pool.local()
                       .acquire(
                         test_bucket,
                         as,
                         std::nullopt,
                         cloud_storage_clients::lease_class::priority)
                       .get();

    shard1_idle
      = pool.invoke_on(1, [](client_pool& other) { return other.idle_count(); })
          .get();
    BOOST_REQUIRE_EQUAL(shard1_idle, num_connections_per_shard - 1);

    // Drop one local lease, capped should now complete via the cvar wake.
    ls_leases.pop_front();
    try {
        auto capped_lease = ss::with_timeout(
                              ss::lowres_clock::now() + 1s,
                              std::move(capped_fut))
                              .get();
    } catch (const ss::timed_out_error&) {
        BOOST_FAIL(
          "capped acquire timed out after local lease released; cvar wake "
          "did not reach the capped waiter");
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

SEASTAR_THREAD_TEST_CASE(test_client_pool_multiple_upstreams) {
    // Smoke test for multiple upstreams support in the client pool.

    constexpr size_t num_connections_per_shard = 4;

    ss::sharded<cloud_storage_clients::client_pool> pool;
    auto pool_stop = test_pool_builder.copy()
                       .connections_per_shard(num_connections_per_shard)
                       .build(pool)
                       .get();

    struct shard_stats {
        size_t current{0};
        size_t max{0};

        void acquired() {
            ++current;
            max = std::max(max, current);
        }
        void released() { --current; }
    };
    ss::sharded<shard_stats> stats;
    stats.start().get();
    auto stats_stop = ss::defer([&stats] { stats.stop().get(); });

    cloud_storage_clients::bucket_name_parts bucket1{
      .name = cloud_storage_clients::plain_bucket_name("test-bucket-1"),
      .params = {{"region", "us-east-1"}, {"endpoint", "my-east-1.localhost"}},
    };

    cloud_storage_clients::bucket_name_parts bucket2{
      .name = cloud_storage_clients::plain_bucket_name("test-bucket-2"),
      .params = {{"region", "us-west-2"}, {"endpoint", "my-west-2.localhost"}},
    };

    // Acquire concurrently
    // - from multiple shards
    // - from multiple upstreams
    pool
      .invoke_on_all([&](cloud_storage_clients::client_pool& p) {
          return ss::async([&] {
              ss::abort_source as;
              auto fut1 = p.acquire(test_bucket, as);
              auto fut2 = p.acquire(bucket1, as);
              auto fut3 = p.acquire(bucket2, as);

              auto r = ss::when_all_succeed(
                         std::move(fut1), std::move(fut2), std::move(fut3))
                         .get();

              BOOST_REQUIRE_EQUAL(
                fmt::format("{}", std::get<0>(r).client),
                "S3Client{{host: localhost, port: 4434}}");
              BOOST_REQUIRE_EQUAL(
                fmt::format("{}", std::get<1>(r).client),
                "S3Client{{host: my-east-1.localhost, port: 4434}}");
              BOOST_REQUIRE_EQUAL(
                fmt::format("{}", std::get<2>(r).client),
                "S3Client{{host: my-west-2.localhost, port: 4434}}");
          });
      })
      .get();

    std::vector<cloud_storage_clients::bucket_name_parts> concurrent_requests{};
    concurrent_requests.reserve(1000);
    for (size_t i = 0; i < 1000; i++) {
        cloud_storage_clients::bucket_name_parts bucket;
        switch (i % 3) {
        case 0:
            // Default upstream (no params).
            bucket.name = cloud_storage_clients::plain_bucket_name(
              fmt::format("test-bucket-{}", i));
            break;
        case 1:
            bucket.name = cloud_storage_clients::plain_bucket_name(
              fmt::format("test-bucket-{}", i));
            bucket.params = {
              {"region", "us-east-1"}, {"endpoint", "my-east-1.localhost"}};
            break;
        case 2:
            bucket.name = cloud_storage_clients::plain_bucket_name(
              fmt::format("test-bucket-{}", i));
            bucket.params = {
              {"region", "us-west-2"}, {"endpoint", "my-west-2.localhost"}};
            break;
        }
        concurrent_requests.push_back(std::move(bucket));
    }

    auto expected_host =
      [](const cloud_storage_clients::bucket_name_parts& b) -> std::string {
        if (b.params.empty()) {
            return "localhost";
        }
        auto it = std::ranges::find_if(
          b.params, [](const auto& kv) { return kv.first == "endpoint"; });
        if (it != b.params.end()) {
            return it->second;
        }
        throw std::runtime_error("endpoint param not found");
    };

    for (int i = 0; i < 3; i++) {
        pool
          .invoke_on_all([&](cloud_storage_clients::client_pool& p) {
              return ss::parallel_for_each(
                concurrent_requests,
                [&](cloud_storage_clients::bucket_name_parts& bucket) {
                    return ss::async([&] {
                        ss::abort_source as;
                        auto lease = p.acquire(bucket, as).get();
                        stats.local().acquired();
                        auto release_guard = ss::defer(
                          [&] { stats.local().released(); });

                        BOOST_REQUIRE_EQUAL(
                          fmt::format("{}", lease.client),
                          fmt::format(
                            "S3Client{{{{host: {}, port: 4434}}}}",
                            expected_host(bucket)));

                        // Hold the lease for a bit to increase
                        // chance of contention.
                        ss::sleep(random_generators::get_int(3) * 1ms).get();
                    });
                });
          })
          .get();

        ss::yield().get();
    }

    stats
      .invoke_on_all([=](shard_stats& s) {
          BOOST_REQUIRE_LE(s.max, num_connections_per_shard);
      })
      .get();

    BOOST_REQUIRE_EQUAL(pool.local().idle_count(), pool.local().capacity());
}
