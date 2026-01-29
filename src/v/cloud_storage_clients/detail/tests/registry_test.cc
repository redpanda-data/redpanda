/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/format_to.h"
#include "base/vlog.h"
#include "cloud_storage_clients/detail/registry.h"
#include "cloud_storage_clients/detail/registry_def.h" // IWYU pragma: keep
#include "test_utils/async.h"

#include <seastar/core/smp.hh>
#include <seastar/util/defer.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using namespace std::chrono_literals;
using namespace cloud_storage_clients;
using namespace cloud_storage_clients::detail;
using namespace testing;

ss::logger test_log("test");

struct test_svc {
    ss::shard_id host_shard{ss::this_shard_id()};
    size_t instance_id{next_instance_id++};
    bool prepare_stop_called{false};

    void prepare_stop() { prepare_stop_called = true; }

    static inline size_t next_instance_id{0};
};

struct test_throwing_svc {
    test_throwing_svc() { throw std::runtime_error("failed to start"); }
    void prepare_stop() {}
};

struct test_throwing_prepare_stop_svc {
    void prepare_stop() { throw std::runtime_error("prepare_stop failed"); }
};

struct test_key {
    int id{0};

    auto operator<=>(const test_key&) const = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "test_key{{{}}}", id);
    }
};

class test_registry final
  : public basic_registry<test_svc, test_key, test_registry>
  , public ss::peering_sharded_service<test_registry> {
public:
    using basic_registry<test_svc, test_key, test_registry>::basic_registry;

protected:
    ss::future<> start_svc(sharded_constructor& ctr, const test_key&) final {
        return ctr.start().discard_result();
    }
};

class throwing_registry final
  : public basic_registry<test_throwing_svc, test_key, throwing_registry>
  , public ss::peering_sharded_service<throwing_registry> {
public:
    using basic_registry<test_throwing_svc, test_key, throwing_registry>::
      basic_registry;

    std::chrono::milliseconds start_delay{0};

protected:
    ss::future<> start_svc(sharded_constructor& svc, const test_key&) final {
        co_await svc.start();
        if (start_delay.count() > 0) {
            co_await ss::sleep(start_delay);
        }
    }
};

} // namespace

TEST(UpstreamRegistry, MultipleKeys) {
    test_registry registry(test_log, test_registry::no_entry_limit);
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    // Different keys return different services.
    auto h0 = registry.get(test_key{.id = 0}).get();
    auto h1 = registry.get(test_key{.id = 1}).get();
    auto h2 = registry.get(test_key{.id = 2}).get();

    EXPECT_NE(&h0.get(), &h1.get());
    EXPECT_NE(&h0.get(), &h2.get());
    EXPECT_NE(&h1.get(), &h2.get());

    // Same key returns same service.
    auto h0_again = registry.get(test_key{.id = 0}).get();
    EXPECT_EQ(&h0.get(), &h0_again.get());
    // Concurrent gets for same key return same service.
    std::vector<ss::future<test_registry::handle>> futures;
    constexpr size_t n = 10;
    futures.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        futures.push_back(registry.get(test_key{.id = 1}));
    }
    auto handles = ss::when_all_succeed(futures.begin(), futures.end()).get();
    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(&h1.get(), &handles[i].get());
    }
}

TEST(UpstreamRegistry, ThrowingStart) {
    throwing_registry registry(test_log, throwing_registry::no_entry_limit);
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    EXPECT_THROW(registry.get(test_key{}).get(), std::runtime_error);
    EXPECT_THROW(registry.get(test_key{}).get(), std::runtime_error);
}

TEST(UpstreamRegistry, DelayedThrowingStartWithConcurrentStop) {
    throwing_registry registry(test_log, throwing_registry::no_entry_limit);
    registry.start_delay = std::chrono::milliseconds(100);

    auto get_fut = registry.get(test_key{});
    ss::sleep(std::chrono::milliseconds(10)).get();
    registry.stop().get();

    EXPECT_THROW(get_fut.get(), std::runtime_error);
}

TEST(UpstreamRegistry, DelayedThrowingStartWithMultipleWaiters) {
    throwing_registry registry(test_log, throwing_registry::no_entry_limit);
    registry.start_delay = std::chrono::milliseconds(100);
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    // Launch two concurrent get() calls. First enters start_svc() which sleeps,
    // second finds entry and blocks on semaphore (sem=0 during init).
    auto get_fut1 = registry.get(test_key{});
    auto get_fut2 = registry.get(test_key{});

    // Both should receive the exception when start_svc() throws.
    // The broken semaphore propagates the exception to all waiters.
    EXPECT_THAT(
      [&] { get_fut1.get(); },
      ThrowsMessage<std::runtime_error>(StrEq("failed to start")));
    EXPECT_THAT(
      [&] { get_fut2.get(); },
      ThrowsMessage<std::runtime_error>(StrEq("failed to start")));
}

TEST(UpstreamRegistry, CrossShardConcurrentGet) {
    ASSERT_GE(ss::smp::count, 2);

    ss::sharded<test_registry> registry;
    registry.start(std::ref(test_log), test_registry::no_entry_limit).get();
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    auto host_shards
      = registry
          .map([](test_registry& r) {
              return r.get(test_key{}).then([](test_registry::handle h) {
                  return h->host_shard;
              });
          })
          .get();

    for (ss::shard_id s{0}; s < ss::smp::count; ++s) {
        EXPECT_EQ(host_shards[s], s);
    }
}

TEST(UpstreamRegistry, CrossShardThrowingStart) {
    ASSERT_GE(ss::smp::count, 2);

    ss::sharded<throwing_registry> registry;
    registry.start(std::ref(test_log), test_registry::no_entry_limit).get();
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    // Request from peer shard - exception should propagate from coordinator.
    EXPECT_THROW(
      registry
        .invoke_on(
          ss::shard_id{1},
          [](throwing_registry& r) {
              return r.get(test_key{}).discard_result();
          })
        .get(),
      std::runtime_error);

    // Retry should also throw (entry was cleaned up).
    EXPECT_THROW(
      registry
        .invoke_on(
          ss::shard_id{1},
          [](throwing_registry& r) {
              return r.get(test_key{}).discard_result();
          })
        .get(),
      std::runtime_error);
}

TEST(UpstreamRegistry, EvictSkipsActiveEntry) {
    test_registry registry(test_log, test_registry::no_entry_limit);
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    // Hold handle while trying to evict.
    auto h = registry.get(test_key{}).get();

    EXPECT_EQ(registry.entry_count(), 1);

    // Try to evict - should skip because handle is held.
    auto evicted = registry
                     .evict_if_older_than(
                       ss::lowres_clock::now() + std::chrono::hours{1})
                     .get();

    EXPECT_EQ(evicted, 0);
    EXPECT_EQ(registry.entry_count(), 1);
}

TEST(UpstreamRegistry, EvictRespectsThreshold) {
    test_registry registry(test_log, test_registry::no_entry_limit);
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    {
        auto h = registry.get(test_key{}).get();
    }

    EXPECT_EQ(registry.entry_count(), 1);
    auto evicted = registry
                     .evict_if_older_than(
                       ss::lowres_clock::now() - std::chrono::hours{1})
                     .get();

    EXPECT_EQ(evicted, 0);
    EXPECT_EQ(registry.entry_count(), 1);
}

TEST(UpstreamRegistry, EvictAndRecreate) {
    test_registry registry(test_log, test_registry::no_entry_limit);
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    size_t first_instance_id = 0;
    {
        auto h = registry.get(test_key{}).get();
        first_instance_id = h->instance_id;
    }
    EXPECT_EQ(registry.entry_count(), 1);

    auto evicted = registry
                     .evict_if_older_than(
                       ss::lowres_clock::now() + std::chrono::hours{1})
                     .get();
    EXPECT_EQ(evicted, 1);
    EXPECT_EQ(registry.entry_count(), 0);

    auto h = registry.get(test_key{}).get();
    EXPECT_NE(h->instance_id, first_instance_id);
}

TEST(UpstreamRegistry, EvictorLoopEvictsIdleEntries) {
    test_registry registry(test_log, test_registry::no_entry_limit);
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    // Create entry and release handle.
    size_t first_instance_id = 0;
    {
        auto h = registry.get(test_key{}).get();
        first_instance_id = h->instance_id;
    }
    EXPECT_EQ(registry.entry_count(), 1);

    // Start evictor with short interval and zero max_idle_time (evict all).
    constexpr auto interval = std::chrono::milliseconds{50};
    registry.start_evictor(interval, std::chrono::milliseconds{0});

    // Wait for evictor to run.
    ss::sleep(interval * 3).get();
    EXPECT_EQ(registry.entry_count(), 0);

    // Entry recreated on next get.
    auto h = registry.get(test_key{}).get();
    EXPECT_NE(h->instance_id, first_instance_id);
}

TEST(UpstreamRegistry, EntryLimitEnforced) {
    ASSERT_GE(ss::smp::count, 2);

    constexpr size_t max_entries = 2;
    ss::sharded<test_registry> registry;
    registry.start(std::ref(test_log), max_entries).get();
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    // Fill up to limit on coordinator.
    auto h1 = registry.local().get(test_key{.id = 1}).get();
    auto h2 = registry.local().get(test_key{.id = 2}).get();
    EXPECT_EQ(registry.local().entry_count(), max_entries);

    // Exceeding limit from coordinator throws.
    EXPECT_THROW(
      registry.local().get(test_key{.id = 3}).get(), registry_full_error);
    EXPECT_EQ(registry.local().entry_count(), max_entries);

    // Exceeding limit from peer shard also throws.
    EXPECT_THROW(
      registry
        .invoke_on(
          1, [](auto& r) { return r.get(test_key{.id = 4}).discard_result(); })
        .get(),
      registry_full_error);

    // Entry count unchanged on both shards.
    EXPECT_EQ(registry.local().entry_count(), max_entries);
    auto peer_count
      = registry.invoke_on(1, [](auto& r) { return r.entry_count(); }).get();
    EXPECT_EQ(peer_count, 0);

    // Existing entries can still be retrieved from both shards.
    auto h1_again = registry.local().get(test_key{.id = 1}).get();
    EXPECT_EQ(&h1.get(), &h1_again.get());

    auto peer_h1_sharded = registry
                             .invoke_on(
                               1,
                               [](auto& r) {
                                   return r.get(test_key{.id = 1})
                                     .then([](auto h) { return &h.sharded(); });
                               })
                             .get();

    EXPECT_EQ(&h1.sharded(), peer_h1_sharded);

    peer_count
      = registry.invoke_on(1, [](auto& r) { return r.entry_count(); }).get();
    EXPECT_EQ(peer_count, 1);
}

TEST(UpstreamRegistry, CrossShardEviction) {
    ASSERT_GE(ss::smp::count, 2);
    constexpr auto evict_threshold = ss::lowres_clock::time_point::max();

    ss::sharded<test_registry> registry;
    registry.start(std::ref(test_log), test_registry::no_entry_limit).get();
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    // Create entry from peer shard.
    size_t first_instance_id
      = registry
          .invoke_on(
            ss::shard_id{1},
            [](test_registry& r) {
                return r.get(test_key{}).then([](test_registry::handle h) {
                    return h->instance_id;
                });
            })
          .get();

    EXPECT_EQ(registry.local().entry_count(), 1);
    auto peer_count = registry
                        .invoke_on(
                          ss::shard_id{1},
                          [](test_registry& r) { return r.entry_count(); })
                        .get();
    EXPECT_EQ(peer_count, 1);

    // Evicting from coordinator shard should skip because peer holds ref.
    registry.local().evict_if_older_than(evict_threshold).get();
    EXPECT_EQ(registry.local().entry_count(), 1);

    // Evict on peer first (releases coordinator's semaphore units).
    registry
      .invoke_on(
        ss::shard_id{1},
        [evict_threshold](test_registry& r) {
            return r.evict_if_older_than(evict_threshold);
        })
      .get();

    // Peer should have 0 entries now.
    peer_count = registry
                   .invoke_on(
                     ss::shard_id{1},
                     [](test_registry& r) { return r.entry_count(); })
                   .get();
    EXPECT_EQ(peer_count, 0);

    // Coordinator still has entry (but now can evict since peer released).
    EXPECT_EQ(registry.local().entry_count(), 1);

    registry.local().evict_if_older_than(evict_threshold).get();
    EXPECT_EQ(registry.local().entry_count(), 0);

    // Recreate from peer - should be a new instance.
    size_t second_instance_id
      = registry
          .invoke_on(
            ss::shard_id{1},
            [](test_registry& r) {
                return r.get(test_key{}).then([](test_registry::handle h) {
                    return h->instance_id;
                });
            })
          .get();

    EXPECT_NE(first_instance_id, second_instance_id);
}

TEST(UpstreamRegistry, StressConcurrentGetAndEvict) {
    test_registry registry(test_log, test_registry::no_entry_limit);
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    constexpr int num_keys = 100;
    constexpr int keys_to_keep = num_keys / 2;

    // Create entries for keys we'll keep.
    std::vector<ss::future<test_registry::handle>> keep_futures;
    keep_futures.reserve(keys_to_keep);
    for (int i = 0; i < keys_to_keep; ++i) {
        keep_futures.push_back(registry.get(test_key{.id = i}));
    }
    auto kept_handles
      = ss::when_all_succeed(keep_futures.begin(), keep_futures.end()).get();

    // Create entries for keys we'll release.
    {
        std::vector<ss::future<test_registry::handle>> release_futures;
        release_futures.reserve(num_keys - keys_to_keep);
        for (int i = keys_to_keep; i < num_keys; ++i) {
            release_futures.push_back(registry.get(test_key{.id = i}));
        }
        // Handles released when scope exits.
        ss::when_all_succeed(release_futures.begin(), release_futures.end())
          .get();
    }

    EXPECT_EQ(registry.entry_count(), num_keys);

    // Evict - should only evict released entries.
    auto evicted = registry
                     .evict_if_older_than(
                       ss::lowres_clock::now() + std::chrono::hours{1})
                     .get();

    EXPECT_EQ(evicted, num_keys - keys_to_keep);
    EXPECT_EQ(registry.entry_count(), keys_to_keep);
}

TEST(UpstreamRegistry, StressCrossShardConcurrentCreateEvict) {
    ASSERT_GE(ss::smp::count, 2);

    ss::sharded<test_registry> registry;
    registry.start(std::ref(test_log), test_registry::no_entry_limit).get();
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    constexpr int keys_per_shard = 20;
    auto threshold = ss::lowres_clock::now() + std::chrono::hours{1};

    // All shards concurrently create entries for different keys.
    registry
      .invoke_on_all([](test_registry& r) {
          std::vector<ss::future<test_registry::handle>> futures;
          futures.reserve(keys_per_shard);
          auto base = static_cast<int>(ss::this_shard_id()) * 1000;
          for (int i = 0; i < keys_per_shard; ++i) {
              futures.push_back(r.get(test_key{.id = base + i}));
          }
          return ss::when_all_succeed(futures.begin(), futures.end())
            .discard_result();
      })
      .get();

    // Verify entries created - coordinator has all keys.
    auto counts
      = registry.map([](test_registry& r) { return r.entry_count(); }).get();
    EXPECT_EQ(counts[0], keys_per_shard * ss::smp::count);

    // Evict on peers first (releases coordinator's semaphore units).
    registry
      .invoke_on_all([threshold](test_registry& r) {
          if (ss::this_shard_id() != 0) {
              return r.evict_if_older_than(threshold).discard_result();
          }
          return ss::now();
      })
      .get();

    // Then evict on coordinator.
    registry
      .invoke_on(
        0,
        [threshold](test_registry& r) {
            return r.evict_if_older_than(threshold).discard_result();
        })
      .get();

    // All entries should be evicted.
    counts
      = registry.map([](test_registry& r) { return r.entry_count(); }).get();
    for (auto count : counts) {
        EXPECT_EQ(count, 0);
    }
}

TEST(UpstreamRegistry, StressConcurrentGetAndEvictWorkers) {
    ASSERT_GE(ss::smp::count, 2);

    ss::sharded<test_registry> registry;
    registry.start(std::ref(test_log), test_registry::no_entry_limit).get();

    constexpr int num_keys = 10;
    constexpr auto chaos_duration = std::chrono::seconds{5};

    auto make_get_worker = [](
                             test_registry& r, int multiplier) -> ss::future<> {
        for (int i = 0;; ++i) {
            try {
                auto h = co_await r.get(
                  test_key{.id = (i * multiplier) % num_keys});
            } catch (const ss::gate_closed_exception&) {
                vlog(
                  test_log.trace,
                  "Get worker on shard {} exiting due to gate closure",
                  ss::this_shard_id());
                co_return;
            } catch (const ss::abort_requested_exception&) {
                vlog(
                  test_log.trace,
                  "Get worker on shard {} exiting due to abort requested",
                  ss::this_shard_id());
                co_return;
            }
        }
    };

    auto make_evict_worker = [](test_registry& r) -> ss::future<> {
        constexpr auto evict_threshold = ss::lowres_clock::time_point::max();
        for (;;) {
            try {
                co_await r.evict_if_older_than(evict_threshold);
            } catch (const ss::gate_closed_exception&) {
                vlog(
                  test_log.trace,
                  "Evict worker on shard {} exiting due to gate closure",
                  ss::this_shard_id());
                co_return;
            }
        }
    };

    // Start workers on all shards.
    auto workers_fut = registry.invoke_on_all(
      [make_get_worker, make_evict_worker](test_registry& r) {
          std::vector<ss::future<>> workers;
          // Different multipliers create different key access patterns to
          // increase contention (e.g., 1: 0,1,2,... vs 7: 0,7,4,1,...).
          workers.push_back(make_get_worker(r, 1));
          workers.push_back(make_get_worker(r, 7));
          workers.push_back(make_evict_worker(r));
          return ss::when_all_succeed(workers.begin(), workers.end())
            .discard_result();
      });

    // Let chaos run for test_duration, then stop registry to terminate workers.
    ss::sleep(chaos_duration).get();
    registry.stop().get();
    workers_fut.get();

    // Test passes if no crashes/hangs.
}

TEST(UpstreamRegistry, PrepareStopCallsServicePrepareStop) {
    ASSERT_GE(ss::smp::count, 2);

    ss::sharded<test_registry> registry;
    registry.start(std::ref(test_log), test_registry::no_entry_limit).get();
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    auto h = registry.local().get(test_key{}).get();

    registry.invoke_on_all([](test_registry& r) { r.prepare_stop(); }).get();

    RPTEST_REQUIRE_EVENTUALLY(3s, [&h] {
        return h.sharded().map_reduce0(
          [](test_svc& svc) { return svc.prepare_stop_called; },
          true,
          std::logical_and<bool>{});
    });
}

TEST(UpstreamRegistry, PrepareStopBlocksNewEntries) {
    test_registry registry(test_log, test_registry::no_entry_limit);
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    registry.prepare_stop();

    EXPECT_THROW(registry.get(test_key{}).get(), ss::abort_requested_exception);
}

TEST(UpstreamRegistry, PrepareStopBlocksGetForExistingEntries) {
    test_registry registry(test_log, test_registry::no_entry_limit);
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    auto h = registry.get(test_key{}).get();

    registry.prepare_stop();

    EXPECT_THROW(registry.get(test_key{}).get(), ss::abort_requested_exception);
}

TEST(UpstreamRegistry, DelayedThrowingStartWithConcurrentPrepareStop) {
    throwing_registry registry(test_log, throwing_registry::no_entry_limit);
    registry.start_delay = std::chrono::milliseconds(100);
    auto stop = ss::defer([&registry]() { registry.stop().get(); });

    auto get_fut = registry.get(test_key{});
    ss::sleep(std::chrono::milliseconds(10)).get();
    registry.prepare_stop();

    EXPECT_THROW(get_fut.get(), std::runtime_error);
}
