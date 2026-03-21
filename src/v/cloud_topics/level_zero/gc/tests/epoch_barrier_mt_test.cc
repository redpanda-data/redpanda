/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

/*
 * Multithreaded tests for the epoch barrier.
 *
 * These tests run the epoch barrier on multiple real Seastar shards to verify
 * that cross-shard write coordination in handle_barrier works correctly.
 */

#include "cloud_topics/level_zero/gc/epoch_barrier.h"
#include "cloud_topics/level_zero/gc/tests/epoch_barrier_test_utils.h"
#include "cluster/cluster_epoch_service.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <system_error>

using namespace cloud_topics;
using namespace cloud_topics::l0::gc;
using namespace cloud_topics::l0::gc::testing;

namespace {

// -- Fixture -----------------------------------------------------------------

/// Per-shard mock partition source pointers, indexed by shard ID.
/// Populated during barrier construction.
std::vector<mock_partition_source*> g_per_shard_ps;

struct epoch_barrier_mt_test : public seastar_test {
    ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>> epoch_svc;
    mock_inflight_write_tracker tracker;
    ss::sharded<epoch_barrier> barrier;

    ss::future<> SetUpAsync() override {
        vassert(ss::smp::count >= 2, "Need at least 2 shards");

        g_per_shard_ps.clear();
        g_per_shard_ps.resize(ss::smp::count, nullptr);

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
          ss::sharded_parameter([] {
              auto p = std::make_unique<mock_partition_source>();
              g_per_shard_ps[ss::this_shard_id()] = p.get();
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
        g_per_shard_ps.clear();
    }

    ss::future<> add_partition_on_shard(
      ss::shard_id shard,
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::term_id term) {
        co_await ss::smp::submit_to(shard, [&, pid, term] {
            g_per_shard_ps[ss::this_shard_id()]->partitions[make_ntp(
              topic, pid)] = mock_partition_source::partition_state{
              .term = term,
              .is_leader = true,
            };
        });
    }

    ss::future<> set_fail_writes_on_shard(
      ss::shard_id shard,
      const ss::sstring& topic,
      model::partition_id::type pid,
      bool fail) {
        co_await ss::smp::submit_to(shard, [&, pid, fail] {
            g_per_shard_ps[ss::this_shard_id()]
              ->partitions[make_ntp(topic, pid)]
              .fail_writes = fail;
        });
    }

    ss::future<std::optional<cluster_epoch>> written_epoch_on_shard(
      ss::shard_id shard,
      const ss::sstring& topic,
      model::partition_id::type pid) {
        co_return co_await ss::smp::submit_to(shard, [&, pid] {
            auto& ps = *g_per_shard_ps[ss::this_shard_id()];
            auto it = ps.partitions.find(make_ntp(topic, pid));
            if (it == ps.partitions.end()) {
                return std::optional<cluster_epoch>{};
            }
            return it->second.written_gc_epoch;
        });
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
};

} // namespace

// -- Cross-shard tests -------------------------------------------------------

TEST_F_CORO(epoch_barrier_mt_test, cross_shard_writes_to_all_leaders) {
    // Partitions on different shards.
    co_await add_partition_on_shard(0, "t0", 0, model::term_id(1));
    co_await add_partition_on_shard(1, "t0", 1, model::term_id(1));

    // Round 1: drain only, no writes (no prev_safe_epoch).
    auto result = co_await advance(cluster_epoch(5));
    EXPECT_TRUE(result);

    // Round 2: deferred publish writes gc_epoch=5 from round 1.
    result = co_await advance(cluster_epoch(6), cluster_epoch(5));
    EXPECT_TRUE(result);

    co_await tests::drain_task_queue();

    // Both shards should have their partition's gc epoch written.
    auto epoch_s0 = co_await written_epoch_on_shard(0, "t0", 0);
    auto epoch_s1 = co_await written_epoch_on_shard(1, "t0", 1);
    EXPECT_EQ(epoch_s0, cluster_epoch(5));
    EXPECT_EQ(epoch_s1, cluster_epoch(5));
}

TEST_F_CORO(epoch_barrier_mt_test, cross_shard_publish_failure_retried) {
    co_await add_partition_on_shard(0, "t0", 0, model::term_id(1));
    co_await add_partition_on_shard(1, "t0", 1, model::term_id(1));

    // Round 1: drain completes, confirmed_epoch=5.
    auto result = co_await advance(cluster_epoch(5));
    EXPECT_TRUE(result);

    // Fail writes on shard 1 so the deferred publish partially fails.
    co_await set_fail_writes_on_shard(1, "t0", 1, true);

    // Round 2: publishes gc_epoch=5 (fire-and-forget). Shard 0 succeeds,
    // shard 1 fails silently. Round 2 drain still completes.
    result = co_await advance(cluster_epoch(6), cluster_epoch(5));
    EXPECT_TRUE(result);

    co_await tests::drain_task_queue();

    auto epoch_s0 = co_await written_epoch_on_shard(0, "t0", 0);
    auto epoch_s1 = co_await written_epoch_on_shard(1, "t0", 1);
    EXPECT_EQ(epoch_s0, cluster_epoch(5));
    EXPECT_EQ(epoch_s1, std::nullopt) << "Shard 1 publish should have failed";

    // Fix the failure. Round 3 publishes gc_epoch=6, succeeding on both.
    co_await set_fail_writes_on_shard(1, "t0", 1, false);
    result = co_await advance(cluster_epoch(7), cluster_epoch(6));
    EXPECT_TRUE(result);

    co_await tests::drain_task_queue();

    epoch_s0 = co_await written_epoch_on_shard(0, "t0", 0);
    epoch_s1 = co_await written_epoch_on_shard(1, "t0", 1);
    EXPECT_EQ(epoch_s0, cluster_epoch(6));
    EXPECT_EQ(epoch_s1, cluster_epoch(6));
}

TEST_F_CORO(epoch_barrier_mt_test, cross_shard_publish_during_blocked_drain) {
    // Partitions on different shards.
    co_await add_partition_on_shard(0, "t0", 0, model::term_id(1));
    co_await add_partition_on_shard(1, "t0", 1, model::term_id(1));

    // Block the drain.
    tracker.drain_gate.emplace();

    // Start round 1 — drain kicks off but blocks.
    auto status = co_await barrier.local().handle_barrier(cluster_epoch(5));
    EXPECT_EQ(status, epoch_barrier::barrier_status::pending);
    EXPECT_EQ(tracker.drain_count, 1);

    // New candidate with prev_safe. This publishes gc_epoch=5 to both
    // shards (cross-shard) and starts a new (also blocked) drain.
    status = co_await barrier.local().handle_barrier(
      cluster_epoch(6), cluster_epoch(5));
    EXPECT_EQ(status, epoch_barrier::barrier_status::pending);
    EXPECT_EQ(tracker.drain_count, 2);

    co_await tests::drain_task_queue();

    // Publish happened even though drain is still in progress.
    auto epoch_s0 = co_await written_epoch_on_shard(0, "t0", 0);
    auto epoch_s1 = co_await written_epoch_on_shard(1, "t0", 1);
    EXPECT_EQ(epoch_s0, cluster_epoch(5));
    EXPECT_EQ(epoch_s1, cluster_epoch(5));

    // Round 2 still pending — combined drain hasn't resolved.
    status = co_await barrier.local().handle_barrier(cluster_epoch(6));
    EXPECT_EQ(status, epoch_barrier::barrier_status::pending);

    // Release — both drain futures resolve.
    tracker.drain_gate->set_value();
    co_await tests::drain_task_queue();

    status = co_await barrier.local().handle_barrier(cluster_epoch(6));
    EXPECT_EQ(status, epoch_barrier::barrier_status::ready);
}

TEST_F_CORO(epoch_barrier_mt_test, cross_shard_new_candidate_resets_all) {
    co_await add_partition_on_shard(0, "t0", 0, model::term_id(1));
    co_await add_partition_on_shard(1, "t0", 1, model::term_id(1));

    // Round 1: drain, confirmed_epoch=5.
    auto result = co_await advance(cluster_epoch(5));
    EXPECT_TRUE(result);

    // Round 2: new candidate triggers new drain, publishes gc_epoch=5.
    result = co_await advance(cluster_epoch(6), cluster_epoch(5));
    EXPECT_TRUE(result);
    EXPECT_EQ(tracker.drain_count, 2);

    co_await tests::drain_task_queue();

    auto epoch_s0 = co_await written_epoch_on_shard(0, "t0", 0);
    auto epoch_s1 = co_await written_epoch_on_shard(1, "t0", 1);
    EXPECT_EQ(epoch_s0, cluster_epoch(5));
    EXPECT_EQ(epoch_s1, cluster_epoch(5));
}
