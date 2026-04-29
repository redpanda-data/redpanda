/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/gc/epoch_barrier.h"
#include "cloud_topics/level_zero/gc/tests/epoch_barrier_test_utils.h"
#include "cluster/cluster_epoch_service.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/sharded.hh>

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <system_error>

using namespace cloud_topics;
using namespace cloud_topics::l0::gc;
using namespace cloud_topics::l0::gc::testing;

namespace {

// -- Fixture -----------------------------------------------------------------

struct epoch_barrier_test : public seastar_test {
    ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>> epoch_svc;
    mock_inflight_write_tracker tracker;
    ss::sharded<epoch_barrier> barrier;
    mock_partition_source* ps{nullptr};

    ss::future<> SetUpAsync() override {
        co_await epoch_svc.start(
          [](ss::abort_source*)
            -> ss::future<std::expected<int64_t, std::error_code>> {
              co_return int64_t{1};
          });
        co_await epoch_svc.invoke_on_all(
          &cluster::cluster_epoch_service<ss::lowres_clock>::start);

        co_await barrier.start(
          std::ref(epoch_svc),
          std::ref(tracker),
          ss::sharded_parameter([this] {
              auto p = std::make_unique<mock_partition_source>();
              if (ss::this_shard_id() == 0) {
                  ps = p.get();
              }
              return p;
          }),
          ss::sharded_parameter([] {
              return std::make_unique<mock_node_source>(model::node_id{0});
          }),
          nullptr);
    }

    ss::future<> TearDownAsync() override {
        co_await barrier.stop();
        co_await epoch_svc.stop();
    }

    void add_partition(
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::term_id term) {
        ps->partitions[make_ntp(topic, pid)]
          = mock_partition_source::partition_state{
            .term = term,
            .is_leader = true,
          };
    }

    void set_leader(
      const ss::sstring& topic, model::partition_id::type pid, bool is_leader) {
        auto it = ps->partitions.find(make_ntp(topic, pid));
        vassert(it != ps->partitions.end(), "partition not found");
        it->second.is_leader = is_leader;
    }

    void set_fail_writes(
      const ss::sstring& topic, model::partition_id::type pid, bool fail) {
        auto it = ps->partitions.find(make_ntp(topic, pid));
        vassert(it != ps->partitions.end(), "partition not found");
        it->second.fail_writes = fail;
    }

    void
    remove_partition(const ss::sstring& topic, model::partition_id::type pid) {
        ps->partitions.erase(make_ntp(topic, pid));
    }

    std::optional<cluster_epoch>
    written_epoch(const ss::sstring& topic, model::partition_id::type pid) {
        auto it = ps->partitions.find(make_ntp(topic, pid));
        if (it == ps->partitions.end()) {
            return std::nullopt;
        }
        return it->second.written_gc_epoch;
    }

    /// Advance through the drain -> ready cycle.
    ///   1. new round + drain kicked off -> pending
    ///   2. drain complete -> ready
    ss::future<bool> advance(
      cluster_epoch e, std::optional<cluster_epoch> prev_safe = std::nullopt) {
        co_await barrier.local().handle_barrier(e, prev_safe);
        co_await tests::drain_task_queue();
        auto result = co_await barrier.local().handle_barrier(e);
        co_return result == epoch_barrier::barrier_status::ready;
    }

    /// Single poll of an existing round.
    ss::future<bool> check(cluster_epoch e) {
        auto result = co_await barrier.local().handle_barrier(e);
        co_return result == epoch_barrier::barrier_status::ready;
    }
};

} // namespace

// -- Core state machine tests ------------------------------------------------

TEST_F_CORO(epoch_barrier_test, deferred_publish_writes_on_next_round) {
    add_partition("t0", 0, model::term_id(1));
    add_partition("t0", 1, model::term_id(1));

    // Round 1: drain only, no writes yet (no prev_safe_epoch).
    auto result = co_await advance(cluster_epoch(5));
    EXPECT_TRUE(result);
    EXPECT_EQ(written_epoch("t0", 0), std::nullopt);
    EXPECT_EQ(written_epoch("t0", 1), std::nullopt);

    // Round 2: passes prev_safe_epoch=5 from the completed round 1.
    // This triggers the publish — advance_gc_epoch(5) written to both.
    result = co_await advance(cluster_epoch(6), cluster_epoch(5));
    EXPECT_TRUE(result);

    co_await tests::drain_task_queue();
    EXPECT_EQ(written_epoch("t0", 0), cluster_epoch(5));
    EXPECT_EQ(written_epoch("t0", 1), cluster_epoch(5));
}

TEST_F_CORO(epoch_barrier_test, non_leader_partitions_skipped) {
    add_partition("t0", 0, model::term_id(1));
    add_partition("t0", 1, model::term_id(1));
    set_leader("t0", 1, false);

    // Round 1: drain.
    auto result = co_await advance(cluster_epoch(5));
    EXPECT_TRUE(result);

    // Round 2: publish with prev_safe=5. Only the leader partition gets it.
    result = co_await advance(cluster_epoch(6), cluster_epoch(5));
    EXPECT_TRUE(result);

    co_await tests::drain_task_queue();
    EXPECT_EQ(written_epoch("t0", 0), cluster_epoch(5));
    EXPECT_EQ(written_epoch("t0", 1), std::nullopt);
}

TEST_F_CORO(epoch_barrier_test, publish_failure_is_not_fatal) {
    add_partition("t0", 0, model::term_id(1));
    set_fail_writes("t0", 0, true);

    // Round 1: drain.
    auto result = co_await advance(cluster_epoch(5));
    EXPECT_TRUE(result);

    // Round 2: publish with prev_safe=5. Write fails — but the round
    // still succeeds (publish failure is fire-and-forget).
    result = co_await advance(cluster_epoch(6), cluster_epoch(5));
    EXPECT_TRUE(result);
    EXPECT_EQ(written_epoch("t0", 0), std::nullopt);

    // Round 3: retry publish with prev_safe=6. Writes now succeed.
    set_fail_writes("t0", 0, false);
    result = co_await advance(cluster_epoch(7), cluster_epoch(6));
    EXPECT_TRUE(result);

    co_await tests::drain_task_queue();
    EXPECT_EQ(written_epoch("t0", 0), cluster_epoch(6));
}

TEST_F_CORO(epoch_barrier_test, no_partitions_returns_true) {
    auto result = co_await advance(cluster_epoch(5));
    EXPECT_TRUE(result) << "No partitions: vacuously ready";
}

TEST_F_CORO(epoch_barrier_test, new_candidate_resets_and_redrains) {
    add_partition("t0", 0, model::term_id(1));

    auto result = co_await advance(cluster_epoch(5));
    EXPECT_TRUE(result);
    EXPECT_EQ(tracker.drain_count, 1);

    // New candidate epoch should trigger a new drain.
    result = co_await advance(cluster_epoch(6), cluster_epoch(5));
    EXPECT_EQ(tracker.drain_count, 2);
    EXPECT_TRUE(result);

    // The publish writes prev_safe=5, not the current candidate 6.
    co_await tests::drain_task_queue();
    EXPECT_EQ(written_epoch("t0", 0), cluster_epoch(5));
}

TEST_F_CORO(epoch_barrier_test, repeated_poll_no_redrain) {
    add_partition("t0", 0, model::term_id(1));

    co_await advance(cluster_epoch(5));
    EXPECT_EQ(tracker.drain_count, 1);

    // Subsequent polls for the same epoch should not re-drain.
    co_await check(cluster_epoch(5));
    EXPECT_EQ(tracker.drain_count, 1);

    co_await check(cluster_epoch(5));
    EXPECT_EQ(tracker.drain_count, 1);
}

TEST_F_CORO(epoch_barrier_test, new_candidate_waits_for_old_drain) {
    add_partition("t0", 0, model::term_id(1));

    // Block the drain so it doesn't resolve immediately.
    tracker.drain_gate.emplace();

    // Start round 1 — drain kicks off but blocks on the gate.
    auto status = co_await barrier.local().handle_barrier(cluster_epoch(5));
    EXPECT_EQ(status, epoch_barrier::barrier_status::pending);
    EXPECT_EQ(tracker.drain_count, 1);

    // Poll — drain still blocked, should stay pending.
    status = co_await barrier.local().handle_barrier(cluster_epoch(5));
    EXPECT_EQ(status, epoch_barrier::barrier_status::pending);

    // New candidate arrives while drain is in progress. This triggers the
    // when_all path: the old drain future is combined with a new drain.
    status = co_await barrier.local().handle_barrier(
      cluster_epoch(6), cluster_epoch(5));
    EXPECT_EQ(status, epoch_barrier::barrier_status::pending);
    EXPECT_EQ(tracker.drain_count, 2);

    // Poll round 2 — the combined drain is still blocked (old drain
    // hasn't resolved yet), so we should stay pending.
    status = co_await barrier.local().handle_barrier(cluster_epoch(6));
    EXPECT_EQ(status, epoch_barrier::barrier_status::pending);

    // Release the gate — both drain futures resolve.
    tracker.drain_gate->set_value();
    co_await tests::drain_task_queue();

    // Now round 2 should complete.
    status = co_await barrier.local().handle_barrier(cluster_epoch(6));
    EXPECT_EQ(status, epoch_barrier::barrier_status::ready);
}

// -- Loop integration tests --------------------------------------------------

// Fixture that wires up the leader loop with a single-node mock,
// exercising the full path: loop -> fan_out -> advance_local ->
// handle_barrier -> drain -> write -> ready.
struct epoch_barrier_loop_test : public seastar_test {
    ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>> epoch_svc;
    mock_inflight_write_tracker tracker;
    ss::sharded<epoch_barrier> barrier;
    mock_partition_source* ps{nullptr};
    // Controls the cluster epoch returned by the epoch service.
    std::atomic<int64_t> cluster_epoch_val{0};

    ss::future<> SetUpAsync() override {
        co_await epoch_svc.start(
          [this](ss::abort_source*)
            -> ss::future<std::expected<int64_t, std::error_code>> {
              return ss::make_ready_future<
                std::expected<int64_t, std::error_code>>(
                cluster_epoch_val.load());
          });
        co_await epoch_svc.invoke_on_all(
          &cluster::cluster_epoch_service<ss::lowres_clock>::start);

        co_await barrier.start(
          std::ref(epoch_svc),
          std::ref(tracker),
          ss::sharded_parameter([this] {
              auto p = std::make_unique<mock_partition_source>();
              if (ss::this_shard_id() == 0) {
                  ps = p.get();
              }
              return p;
          }),
          ss::sharded_parameter([] {
              return std::make_unique<mock_node_source>(model::node_id{0});
          }),
          nullptr);
    }

    ss::future<> TearDownAsync() override {
        co_await barrier.stop();
        co_await epoch_svc.stop();
    }
};

TEST_F_CORO(epoch_barrier_loop_test, loop_converges_single_node) {
    // Leader partition present on shard 0.
    ps->partitions[make_ntp("t0", 0)] = mock_partition_source::partition_state{
      .term = model::term_id(1),
      .is_leader = true,
    };

    cluster_epoch_val = 5;
    co_await barrier.local().set_leader(true);

    // With deferred publish, the gc epoch is written on the SECOND round.
    // Round 1 completes for candidate=5, setting confirmed_epoch=5.
    // We need the candidate to advance so a new round starts and
    // publishes prev_safe_epoch=5. Bump the epoch after round 1 completes.
    RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this]() -> ss::future<bool> {
        return ss::make_ready_future<bool>(tracker.drain_count.load() >= 1);
    });

    // Advance cluster epoch so the next round triggers a publish.
    cluster_epoch_val = 6;

    RPTEST_REQUIRE_EVENTUALLY_CORO(20s, [this]() -> ss::future<bool> {
        auto& state = ps->partitions[make_ntp("t0", 0)];
        return ss::make_ready_future<bool>(
          state.written_gc_epoch.has_value()
          && *state.written_gc_epoch == cluster_epoch(5)
          && tracker.drain_count.load() >= 2);
    });
}
