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
#include "ssx/when_all.h"
#include "test_utils/async.h"
#include "test_utils/boost_fixture.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/range/irange.hpp>

#include <algorithm>
#include <chrono>
#include <ranges>

using throttler = raft::coordinated_recovery_throttle;

/// Boiler plate for starting and cleaning up a sharded throttler.
struct test_fixture {
    // This value is carefully chosen to be lcm[0,,,num_shard-1].
    // An lcm guarantees that there are no rounding issues in
    // coordination ticks when distributing bandwidth, making the
    // test logic simple.
    static constexpr ssize_t initial_rate_per_shard = 420;
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

    ss::future<std::vector<ssize_t>> all_available() {
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

    void check_available_all_shards(ssize_t expected) {
        auto available = all_available().get();
        BOOST_REQUIRE(
          std::all_of(
            available.begin(), available.end(), [expected](auto current) {
                return current == expected;
            }));
    }

    void check_available(ss::shard_id shard, ssize_t expected) {
        auto available
          = local()
              .container()
              .invoke_on(shard, [](auto& local) { return local.available(); })
              .get();
        BOOST_REQUIRE_EQUAL(expected, available);
    }

    void check_admitted(ss::shard_id shard, ssize_t expected) {
        auto available = local()
                           .container()
                           .invoke_on(
                             shard,
                             [](auto& local) { return local.admitted_bytes(); })
                           .get();
        BOOST_REQUIRE_EQUAL(expected, available);
    }

    void check_waiting(ss::shard_id shard, ssize_t expected) {
        auto waiting = local()
                         .container()
                         .invoke_on(
                           shard,
                           [](auto& local) { return local.waiting_bytes(); })
                         .get();
        BOOST_REQUIRE_EQUAL(expected, waiting);
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
    BOOST_REQUIRE_EQUAL(local().waiting_bytes(), half);
    BOOST_REQUIRE_EQUAL(local().admitted_bytes(), 2 * half);

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
    for (auto i : std::views::iota(ss::shard_id{1}, ss::smp::count)) {
        vlog(logger.info, "Running iteration: {}", i);

        // Each iteration of this loop consumes the entirety of bandwidth
        // among shards [0, i). [i+1, n) remain idle and their unused
        // bandwidth should be shared among [0, i) resulting in progress.

        // step 1: Consume all rate on [0, i)
        auto total_rate = local().available() * ss::smp::count;
        auto throttle_per_shard = total_rate / i;
        std::vector<ss::future<>> throttled;
        for (auto j : boost::irange(i)) {
            // consume 1 byte first
            throttle_on_shard(j, 1).get();
            // will block, as only the first request in the tick can overuse
            throttled.emplace_back(
              throttle_on_shard(j, throttle_per_shard - 1));
        }
        // Ensure the throttled fibers are accounted for.
        wait_until([this, throttle_per_shard, i] {
            return all_waiting().then(
              [throttle_per_shard, i](std::vector<size_t> waiting) {
                  return std::all_of(
                    waiting.begin(),
                    waiting.begin() + i,
                    [throttle_per_shard](auto curr) {
                        return static_cast<ssize_t>(curr)
                               == throttle_per_shard - 1;
                    });
              });
        });
        wait_until([this, i] {
            return all_admitted().then([i](std::vector<size_t> admitted) {
                return std::all_of(
                  admitted.begin(), admitted.begin() + i, [](auto curr) {
                      return curr == 1;
                  });
            });
        });

        // Step 2: Trigger rebalancing of available bandwidth.
        coordinator_tick().get();

        // Step 3: Wait for the throttled fibers to finish.
        ssx::when_all_succeed(std::move(throttled)).get();
        // No more than rounding errors should remain in available
        // bandwidth after rebalancing.
        ssize_t total_available = 0;
        for (auto current : all_available().get()) {
            BOOST_REQUIRE(std::abs(current) < ss::smp::count);
            total_available += current;
        }
        BOOST_REQUIRE(std::abs(total_available) < ss::smp::count);

        // Nothing waiting
        BOOST_REQUIRE_EQUAL(
          std::ranges::count(all_waiting().get(), 0), ss::smp::count);
        // All admitted bytes accounted for.
        wait_until([this, throttle_per_shard, i] {
            return all_admitted().then([throttle_per_shard,
                                        i](std::vector<size_t> admitted) {
                return std::ranges::count(
                         admitted | std::views::take(i), throttle_per_shard - 1)
                         == i
                       && std::ranges::count(admitted | std::views::drop(i), 0)
                            == ss::smp::count - i;
            });
        });

        // Step 4: Trigger rebalancing again.
        coordinator_tick().get();
        // Observe that active shards get all the units.
        ss::shard_id shard_id = 0;
        for (auto current : all_available().get()) {
            if (shard_id < i) {
                BOOST_REQUIRE(current > throttle_per_shard - ss::smp::count);
            } else {
                BOOST_REQUIRE(current < ss::smp::count);
            }
            ++shard_id;
        }
        BOOST_REQUIRE(std::abs(total_available) < ss::smp::count);

        // Step 5: In a few ticks, all shards should be back to fair rate.
        wait_until([this] {
            return coordinator_tick()
              .then([this] { return all_available(); })
              .then([](std::vector<ssize_t> available) {
                  return std::ranges::count(available, initial_rate_per_shard)
                         == ss::smp::count;
              });
        });
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

FIXTURE_TEST(overusing, test_fixture) {
    // Consume only one byte on shard 0
    throttle_on_shard(0, 1).get();

    check_available(0, initial_rate_per_shard - 1);
    for (auto shard_id : std::views::iota(ss::shard_id{1}, ss::smp::count)) {
        check_available(shard_id, initial_rate_per_shard);
    }

    // Attempt to borrow from future capacity on shard 0.
    auto oversized_req = initial_rate_per_shard * 2;
    auto f = throttle_on_shard(0, oversized_req);

    // Ensure the throttling is registered but not satisfied.
    ss::yield().get();
    check_waiting(0, oversized_req);
    for (auto shard_id : std::views::iota(ss::shard_id{1}, ss::smp::count)) {
        check_waiting(shard_id, 0);
    }
    BOOST_REQUIRE(!f.available());

    // cancel it
    _as.invoke_on(0, std::mem_fn(&ss::abort_source::request_abort)).get();
    f.wait();
    f.get_exception();
    BOOST_REQUIRE(std::ranges::count(all_waiting().get(), 0) == ss::smp::count);

    // Trigger a tick to refill the buckets.
    coordinator_tick().get();
    check_available(0, initial_rate_per_shard);
    for (auto shard_id : std::views::iota(ss::shard_id{1}, ss::smp::count)) {
        check_available(shard_id, initial_rate_per_shard);
    }

    // Ensure the overusing throttle completes.
    throttle_on_shard(0, oversized_req).get();

    // Check that the admitted bytes include the borrowed amount.
    check_admitted(0, oversized_req);
    for (auto shard_id : std::views::iota(ss::shard_id{1}, ss::smp::count)) {
        check_admitted(shard_id, 0);
    }

    // Check that available capacity is negative
    check_available(0, -initial_rate_per_shard);
    for (auto shard_id : std::views::iota(ss::shard_id{1}, ss::smp::count)) {
        check_available(shard_id, initial_rate_per_shard);
    }

    // Trigger a tick to refill the buckets.
    coordinator_tick().get();

    // Check that available capacity is back to normal.
    // and balance is non-negative on all shards
    auto available = all_available().get();
    BOOST_REQUIRE(std::ranges::all_of(available, [](auto current) {
        return current >= 0;
    }));
    auto sum = std::ranges::fold_left(available, ssize_t{0}, std::plus{});
    auto deviation_from_full = std::abs(
      sum - ss::smp::count * initial_rate_per_shard);
    // allow for rounding errors
    BOOST_REQUIRE(deviation_from_full <= ss::smp::count);
}

FIXTURE_TEST(overusing_a_lot, test_fixture) {
    // make a lot of throttle requests, each larger than full rate
    size_t size_multiplier = 10;
    int requests_per_shard = 10;
    auto request_on_shard =
      [this, requests_per_shard, size_multiplier](ss::shard_id shard_id) {
          return ssx::when_all_succeed(
            std::views::iota(0, requests_per_shard)
            | std::views::transform([this, shard_id, size_multiplier](int) {
                  return throttle_on_shard(
                    shard_id,
                    initial_rate_per_shard * ss::smp::count * size_multiplier);
              }));
      };
    auto all_throttle_f = ssx::when_all_succeed(
      std::views::iota(ss::shard_id{0}, ss::smp::count)
      | std::views::transform(request_on_shard));

    auto ticks = size_multiplier * requests_per_shard * ss::smp::count + 1;
    for (size_t i = 0; i <= ticks; ++i) {
        coordinator_tick().get();
    }

    // make sure all requests completed
    all_throttle_f.get();

    // and balance is non-negative on all shards
    all_available()
      .then([](auto available) {
          return std::ranges::all_of(
            available, [](auto current) { return current >= 0; });
      })
      .get();
}

FIXTURE_TEST(overusing_on_zero_rate, test_fixture) {
    update_rate(0);
    coordinator_tick().get();

    // even a small request won't get through, not even after a couple of ticks
    auto f = throttle_on_shard(0, 1);
    coordinator_tick().get();
    coordinator_tick().get();
    check_waiting(0, 1);
    BOOST_ASSERT(!f.available());

    update_rate(ss::smp::count);
    coordinator_tick().get();
    f.get();
}

FIXTURE_TEST(allowance_staying_negative, test_fixture) {
    ssize_t original_rate = 100 * ss::smp::count;
    ssize_t reduced_rate = 10 * ss::smp::count;
    ssize_t oversized_request = 10000 * ss::smp::count;

    update_rate(original_rate);
    coordinator_tick().get();

    throttle_on_shard(0, oversized_request).get();
    update_rate(reduced_rate);
    coordinator_tick().get();

    auto available = all_available().get();
    BOOST_REQUIRE(std::ranges::all_of(available, [this](auto current) {
        vlog(logger.info, "current: {}", current);
        return current < 0;
    }));
    auto sum = std::ranges::fold_left(available, ssize_t{0}, std::plus{});
    auto expected_sum = original_rate + reduced_rate - oversized_request;

    auto deviation = std::abs(sum - expected_sum);
    // allow for rounding errors
    BOOST_REQUIRE(deviation <= ss::smp::count);
}
