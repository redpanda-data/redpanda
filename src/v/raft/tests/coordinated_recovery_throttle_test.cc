// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/mock_property.h"
#include "raft/coordinated_recovery_throttle.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <chrono>

using throttler = raft::coordinated_recovery_throttle;

/// Boiler plate for starting and cleaning up a sharded throttler.
struct test_fixture {
    // This value is carefully chosen to be lcm[0,,,num_shard-1].
    // An lcm guarantees that there are no rounding issues in
    // coordination ticks when distributing bandwidth, making the
    // test logic simple.
    static constexpr size_t initial_rate_per_shard = 420;
    static constexpr std::chrono::seconds timeout{5};

    test_fixture() {
        BOOST_REQUIRE_GT(ss::smp::count, 1);
        BOOST_REQUIRE_EQUAL(ss::this_shard_id(), 0);
        vlog(logger.info, "using smp count: {}", ss::smp::count);
        _as.start().get();
        // Here we don't trigger the coordinator timer intentionally so that
        // the test can step thru the ticks manually as needed using
        // coordinator_tick(). That gives more control over the test state.
        _config_rate.start(ss::smp::count * initial_rate_per_shard).get();
        _config_use_static.start(false).get();
        _throttler
          .start(
            ss::sharded_parameter(
              [this] { return _config_rate.local().bind(); }),
            ss::sharded_parameter(
              [this] { return _config_use_static.local().bind(); }))
          .get();
        check_available_all_shards(initial_rate_per_shard);
    }

    ~test_fixture() {
        _throttler.invoke_on_all(&throttler::shutdown).get();
        _throttler.stop().get();
        _config_rate.stop().get();
        _config_use_static.stop().get();
        _as.stop().get();
    }

    void update_rate(size_t new_rate) {
        _config_rate
          .invoke_on_all(
            [new_rate](auto& local) mutable { local.update(size_t(new_rate)); })
          .get();
    }

    throttler& local() { return _throttler.local(); }

    ss::future<std::vector<size_t>> all_available() {
        return local().container().map(
          [](auto& local) { return local.available(); });
    }

    ss::future<std::vector<size_t>> all_waiting() {
        return local().container().map(
          [](auto& local) { return local.waiting_bytes(); });
    }

    ss::future<std::vector<size_t>> all_admitted() {
        return local().container().map(
          [](auto& local) { return local.admitted_bytes(); });
    }

    ss::future<> coordinator_tick() { return local().tick_for_testing(); }

    void check_available_all_shards(size_t expected) {
        auto available = all_available().get();
        BOOST_REQUIRE(std::all_of(
          available.begin(), available.end(), [expected](auto current) {
              return current == expected;
          }));
    }

    void check_available(ss::shard_id shard, size_t expected) {
        auto available
          = local()
              .container()
              .invoke_on(shard, [](auto& local) { return local.available(); })
              .get();
        BOOST_REQUIRE_EQUAL(expected, available);
    }

    ss::future<> throttle_on_shard(ss::shard_id shard, size_t bytes) {
        return local().container().invoke_on(shard, [bytes, this](auto& local) {
            return local.throttle(bytes, _as.local());
        });
    }

    template<class Predicate>
    void wait_until(Predicate&& pred) {
        tests::cooperative_spin_wait_with_timeout(
          timeout, std::forward<Predicate>(pred))
          .get();
    }

    ss::logger logger{"throttler_test"};
    ss::sharded<ss::abort_source> _as{};
    ss::sharded<config::mock_property<size_t>> _config_rate;
    ss::sharded<config::mock_property<bool>> _config_use_static;
    ss::sharded<throttler> _throttler;
};

FIXTURE_TEST(throttler_test_simple, test_fixture) {
    BOOST_REQUIRE_EQUAL(local().waiting_bytes(), 0);
    BOOST_REQUIRE_EQUAL(local().admitted_bytes(), 0);

    // consume half
    auto half = initial_rate_per_shard / 2;
    local().throttle(half, _as.local()).get();

    BOOST_REQUIRE_EQUAL(local().waiting_bytes(), 0);
    BOOST_REQUIRE_EQUAL(local().admitted_bytes(), half);

    // consume second half, bucket is empty at this point.
    local().throttle(half, _as.local()).get();

    BOOST_REQUIRE_EQUAL(local().waiting_bytes(), 0);
    BOOST_REQUIRE_EQUAL(local().admitted_bytes(), 2 * half);

    // half more, not enough bytes left, should block until the capacity is
    // refilled.
    auto f = local().throttle(half, _as.local());

    wait_until([this, half] {
        return local().waiting_bytes() == half
               && local().admitted_bytes() == 2 * half;
    });

    // force a tick, this refills the bucket.
    // Resets admitted counters.
    coordinator_tick().get();

    // New bytes admitted and nothing should wait.
    f.get();

    BOOST_REQUIRE_EQUAL(local().waiting_bytes(), 0);
    BOOST_REQUIRE_EQUAL(local().admitted_bytes(), half);

    // Multiple ticks to reset the state.
    wait_until([this] {
        return coordinator_tick().then(
          [this] { return local().admitted_bytes() == 0; });
    });

    // Rate allocation should be back to fair rate.
    check_available(ss::this_shard_id(), initial_rate_per_shard);
}

FIXTURE_TEST(throttler_test_rebalancing, test_fixture) {
    for (auto i : boost::irange(ss::smp::count - 1)) {
        vlog(logger.info, "Running iteration: {}", i);

        // Each iteration of this loop consumes the entirety of bandwidth
        // among shards [0, i]. [i+1, n) remain idle and their unused
        // bandwidth should be shared among [0, i] resulting in progress.

        // step 1: Consume all rate on [0, i]
        auto total_rate = local().available() * ss::smp::count;
        auto throttle_per_shard = total_rate / (i + 1);
        std::vector<ss::future<>> throttled;
        for (auto j : boost::irange(i + 1)) {
            throttled.emplace_back(throttle_on_shard(j, throttle_per_shard));
        }

        // Ensure the throttled fibers are accounted for.
        wait_until([this, throttle_per_shard, i] {
            return all_waiting().then(
              [throttle_per_shard, i](std::vector<size_t> waiting) {
                  return std::all_of(
                    waiting.begin(),
                    waiting.begin() + i + 1,
                    [throttle_per_shard](auto curr) {
                        return curr == throttle_per_shard;
                    });
              });
        });

        wait_until([this, i] {
            return all_admitted().then([i](std::vector<size_t> admitted) {
                return std::all_of(
                  admitted.begin(), admitted.begin() + i + 1, [](auto curr) {
                      return curr == 0;
                  });
            });
        });

        // Step 2: Trigger rebalancing of available bandwidth.
        coordinator_tick().get();

        // Step 3: Wait for the throttled fibers to finish.
        ss::when_all_succeed(throttled.begin(), throttled.end()).get();

        // Everything is consumed at this point, ensure that is reflected
        // in the available capacity.
        wait_until([this] {
            return all_available().then([](auto available) {
                return std::all_of(
                  available.begin(), available.end(), [](size_t current) {
                      return current == 0;
                  });
            });
        });

        // Make sure the throttling fiber actually made progress.
        wait_until([this, i] {
            return all_waiting().then([i](std::vector<size_t> waiting) {
                return std::all_of(
                  waiting.begin(), waiting.begin() + i + 1, [](auto curr) {
                      return curr == 0;
                  });
            });
        });

        wait_until([this, throttle_per_shard, i] {
            return all_admitted().then(
              [throttle_per_shard, i](std::vector<size_t> admitted) {
                  return std::all_of(
                    admitted.begin(),
                    admitted.begin() + i + 1,
                    [throttle_per_shard](auto curr) {
                        return curr == throttle_per_shard;
                    });
              });
        });

        // In a few ticks, all shards should be back to fair rate.
        wait_until([this] {
            return coordinator_tick().then([this] {
                return local().waiting_bytes() == 0
                       && local().admitted_bytes() == 0
                       && local().available() == initial_rate_per_shard;
            });
        });

        check_available_all_shards(initial_rate_per_shard);
    }
}

FIXTURE_TEST(throttler_rate_update, test_fixture) {
    auto current_shard_rate = local().available();
    for (auto curr : {current_shard_rate / 2, current_shard_rate * 2}) {
        auto new_rate = curr * ss::smp::count;
        update_rate(new_rate);
        tests::cooperative_spin_wait_with_timeout(timeout, [this, curr] {
            return coordinator_tick().then(
              [this, curr] { return local().available() == curr; });
        }).get();
    }
}
