/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/compaction/compaction_queue.h"
#include "cloud_topics/level_one/maintenance/leveling/leveling_queue.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/scheduler_probe.h"
#include "cloud_topics/level_one/maintenance/scheduling_policies.h"
#include "cloud_topics/level_one/maintenance/worker.h"
#include "cloud_topics/level_one/maintenance/worker_manager.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

using namespace cloud_topics;
using namespace std::chrono_literals;

class WorkerManagerTestFixture : public seastar_test {
public:
    ss::future<> start_workers(l1::worker_manager& manager) {
        co_await manager._workers.start(
          &manager,
          nullptr,
          nullptr,
          nullptr,
          ss::default_scheduling_group(),
          nullptr);
        co_await manager._workers.invoke_on_all(
          &l1::compaction_worker::resume_compaction_work_loop);
        co_await manager._workers.invoke_on_all(
          &l1::compaction_worker::resume_leveling_work_loop);
    }

    ss::future<l1::compaction_worker::worker_state>
    get_compaction_state(l1::worker_manager& manager, ss::shard_id shard) {
        return manager._workers.invoke_on(
          shard, [](l1::compaction_worker& worker) {
              return worker._worker_target_compaction_state;
          });
    }

    ss::future<l1::compaction_worker::worker_state>
    get_leveling_state(l1::worker_manager& manager, ss::shard_id shard) {
        return manager._workers.invoke_on(
          shard, [](l1::compaction_worker& worker) {
              return worker._worker_target_leveling_state;
          });
    }

    ss::future<bool>
    work_fut_has_value(l1::worker_manager& manager, ss::shard_id shard) {
        return manager._workers.invoke_on(
          shard, [](l1::compaction_worker& worker) {
              return worker._compaction_work_fut.has_value()
                     && worker._leveling_work_fut.has_value();
          });
    }

    // Registers a fake inflight leveling job on the worker, standing in for a
    // job dispatched onto the gate, without driving a real leveling pipeline.
    ss::future<> add_inflight_leveling_job(
      l1::worker_manager& manager,
      ss::shard_id shard,
      model::topic_id_partition tidp,
      kafka::offset base) {
        return manager._workers.invoke_on(
          shard, [tidp, base](l1::compaction_worker& worker) {
              worker._inflight_leveling.emplace(
                l1::compaction_worker::inflight_key{
                  .tidp = tidp, .base_offset = base},
                ss::make_lw_shared<
                  l1::compaction_worker::leveling_job_handle>());
          });
    }

    ss::future<l1::compaction_job_state> inflight_leveling_state(
      l1::worker_manager& manager,
      ss::shard_id shard,
      model::topic_id_partition tidp,
      kafka::offset base) {
        return manager._workers.invoke_on(
          shard, [tidp, base](l1::compaction_worker& worker) {
              auto it = worker._inflight_leveling.find(
                l1::compaction_worker::inflight_key{
                  .tidp = tidp, .base_offset = base});
              return it->second->state;
          });
    }

    // Simulates a leveling job winding down: drop it from the inflight set and
    // signal the drain condition variable, as `do_level_range`'s cleanup does.
    ss::future<>
    drain_inflight_leveling(l1::worker_manager& manager, ss::shard_id shard) {
        return manager._workers.invoke_on(
          shard, [](l1::compaction_worker& worker) {
              worker._inflight_leveling.clear();
              worker._leveling_drained_cv.signal();
          });
    }
};

TEST_F(WorkerManagerTestFixture, PauseAndResumeWorkers) {
    auto cmp_func =
      [](const l1::compaction_job_ptr& a, const l1::compaction_job_ptr& b) {
          return a->meta->ntp < b->meta->ntp;
      };

    l1::compaction_scheduler_probe probe;
    l1::compaction_queue pq(std::move(cmp_func));
    l1::leveling_extent_reclamation_policy lq_policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue lq(lq_policy.get_comparator());
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    start_workers(manager).get();
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });
    using worker_state = l1::compaction_worker::worker_state;
    for (ss::shard_id i = 0; i < ss::this_smp_shard_count(); ++i) {
        // Both kinds start active with both loop fibers running.
        ASSERT_EQ(get_compaction_state(manager, i).get(), worker_state::active);
        ASSERT_EQ(get_leveling_state(manager, i).get(), worker_state::active);
        ASSERT_TRUE(work_fut_has_value(manager, i).get());

        // Pausing only compaction leaves leveling running.
        manager.pause_worker(i, l1::maintenance_job_type::compaction).get();
        ASSERT_EQ(get_compaction_state(manager, i).get(), worker_state::paused);
        ASSERT_EQ(get_leveling_state(manager, i).get(), worker_state::active);

        // Pausing leveling too tears down both loop fibers.
        manager.pause_worker(i, l1::maintenance_job_type::leveling).get();
        ASSERT_EQ(get_compaction_state(manager, i).get(), worker_state::paused);
        ASSERT_EQ(get_leveling_state(manager, i).get(), worker_state::paused);
        ASSERT_FALSE(work_fut_has_value(manager, i).get());

        // A whole-worker resume brings both kinds back to active.
        manager.resume_worker(i).get();
        ASSERT_EQ(get_compaction_state(manager, i).get(), worker_state::active);
        ASSERT_EQ(get_leveling_state(manager, i).get(), worker_state::active);
        ASSERT_TRUE(work_fut_has_value(manager, i).get());

        // A whole-worker pause followed by per-kind resumes round-trips.
        manager.pause_worker(i).get();
        ASSERT_EQ(get_compaction_state(manager, i).get(), worker_state::paused);
        ASSERT_EQ(get_leveling_state(manager, i).get(), worker_state::paused);

        manager.resume_worker(i, l1::maintenance_job_type::leveling).get();
        ASSERT_EQ(get_compaction_state(manager, i).get(), worker_state::paused);
        ASSERT_EQ(get_leveling_state(manager, i).get(), worker_state::active);

        manager.resume_worker(i, l1::maintenance_job_type::compaction).get();
        ASSERT_EQ(get_compaction_state(manager, i).get(), worker_state::active);
        ASSERT_EQ(get_leveling_state(manager, i).get(), worker_state::active);
        ASSERT_TRUE(work_fut_has_value(manager, i).get());
    }
}

TEST_F(WorkerManagerTestFixture, AcquireWork) {
    auto cmp_func =
      [](const l1::compaction_job_ptr& a, const l1::compaction_job_ptr& b) {
          return a->meta->ntp < b->meta->ntp;
      };

    l1::compaction_scheduler_probe probe;
    l1::compaction_queue pq(std::move(cmp_func));
    l1::leveling_extent_reclamation_policy lq_policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue lq(lq_policy.get_comparator());
    l1::log_list_t list;
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });

    const auto test_ntp = model::ntp(
      model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));
    const auto test_tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), test_ntp.tp.partition);
    auto meta = ss::make_lw_shared<l1::log_compaction_meta>(
      test_tidp, test_ntp);
    list.push_back(*meta);
    auto job = ss::make_lw_shared<l1::compaction_job>(
      meta, l1::compaction_info_and_timestamp{});
    pq.push(job);

    auto work_opt = manager.try_acquire_compaction_work(ss::this_shard_id());
    ASSERT_TRUE(work_opt.has_value());
    ASSERT_EQ(work_opt.value()->meta->ntp, test_ntp);
    ASSERT_EQ(work_opt.value()->meta->tidp, test_tidp);
    ASSERT_TRUE(work_opt.value()->meta->compaction.inflight_shard.has_value());
    ASSERT_EQ(
      work_opt.value()->meta->compaction.inflight_shard.value(),
      ss::this_shard_id());

    manager.complete_compaction_work(work_opt.value().get());
    ASSERT_FALSE(work_opt.value()->meta->compaction.inflight_shard.has_value());
}

// `try_acquire_leveling_work` must pop past stale jobs at the head of the
// queue (jobs whose CTP was unmanaged, leaving `meta->link` unlinked) and
// return the first live job behind them, rather than giving up on the first.
TEST_F(WorkerManagerTestFixture, AcquireLevelingWorkSkipsStaleEntries) {
    auto cmp_func =
      [](const l1::compaction_job_ptr& a, const l1::compaction_job_ptr& b) {
          return a->meta->ntp < b->meta->ntp;
      };

    l1::compaction_scheduler_probe probe;
    l1::compaction_queue pq(std::move(cmp_func));
    l1::leveling_extent_reclamation_policy lq_policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue lq(lq_policy.get_comparator());
    l1::log_list_t list;
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });

    auto make_meta = [](std::string_view topic) {
        auto ntp = model::ntp(
          model::ns("kafka"),
          model::topic(ss::sstring{topic}),
          model::partition_id(0));
        auto tidp = model::topic_id_partition(
          model::topic_id(uuid_t::create()), ntp.tp.partition);
        return ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
    };
    // Higher extent_count => higher expected reclaim => sorts first.
    auto make_job =
      [](const l1::log_compaction_meta_ptr& meta, size_t extent_count) {
          return ss::make_lw_shared<l1::leveling_job>(
            meta,
            l1::levelable_range{
              .base_offset = kafka::offset{0},
              .last_offset = kafka::offset{99},
              .size_bytes = 1,
              .extent_count = extent_count},
            l1::metastore::compaction_epoch{0});
      };

    // Stale job at the head: its meta is never linked into `list`.
    auto stale = make_meta("stale");
    lq.push(make_job(stale, 100));

    // A live, linked job sitting behind the stale one.
    auto live = make_meta("live");
    list.push_back(*live);
    lq.push(make_job(live, 10));

    auto work_opt = manager.try_acquire_leveling_work(ss::this_shard_id());
    ASSERT_TRUE(work_opt.has_value());
    ASSERT_EQ(work_opt.value()->meta->ntp, live->ntp);
    ASSERT_TRUE(lq.empty());

    // A queue with only stale jobs yields nothing (and is drained).
    auto stale_only = make_meta("stale_only");
    lq.push(make_job(stale_only, 5));
    ASSERT_FALSE(
      manager.try_acquire_leveling_work(ss::this_shard_id()).has_value());
    ASSERT_TRUE(lq.empty());
}

// Leveling should not run concurrently with compaction of the same CTP:
// `try_acquire_leveling_work` must drop a job whose CTP has an inflight
// compaction (without marking its range inflight) and return the next live
// job behind it.
TEST_F(WorkerManagerTestFixture, AcquireLevelingWorkSkipsCompactingCTP) {
    auto cmp_func =
      [](const l1::compaction_job_ptr& a, const l1::compaction_job_ptr& b) {
          return a->meta->ntp < b->meta->ntp;
      };

    l1::compaction_scheduler_probe probe;
    l1::compaction_queue pq(std::move(cmp_func));
    l1::leveling_extent_reclamation_policy lq_policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue lq(lq_policy.get_comparator());
    l1::log_list_t list;
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });

    auto make_meta = [](std::string_view topic) {
        auto ntp = model::ntp(
          model::ns("kafka"),
          model::topic(ss::sstring{topic}),
          model::partition_id(0));
        auto tidp = model::topic_id_partition(
          model::topic_id(uuid_t::create()), ntp.tp.partition);
        return ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
    };
    // Higher extent_count => higher expected reclaim => sorts first.
    auto make_job =
      [](const l1::log_compaction_meta_ptr& meta, size_t extent_count) {
          return ss::make_lw_shared<l1::leveling_job>(
            meta,
            l1::levelable_range{
              .base_offset = kafka::offset{0},
              .last_offset = kafka::offset{99},
              .size_bytes = 1,
              .extent_count = extent_count},
            l1::metastore::compaction_epoch{0});
      };

    // A managed CTP with an inflight compaction at the head of the queue.
    auto compacting = make_meta("compacting");
    list.push_back(*compacting);
    compacting->compaction.inflight_shard = ss::this_shard_id();
    lq.push(make_job(compacting, 100));

    // A live CTP with no inflight compaction sitting behind it.
    auto live = make_meta("live");
    list.push_back(*live);
    lq.push(make_job(live, 10));

    auto work_opt = manager.try_acquire_leveling_work(ss::this_shard_id());
    ASSERT_TRUE(work_opt.has_value());
    ASSERT_EQ(work_opt.value()->meta->ntp, live->ntp);
    ASSERT_TRUE(lq.empty());
    // The dropped job's range was never marked inflight, so the collector is
    // free to re-queue it once the compaction completes.
    ASSERT_TRUE(compacting->leveling.inflight_ranges.empty());
}

// Dequeuing a compaction job must preempt leveling of the same CTP: its queued
// leveling jobs are dropped and its inflight leveling jobs are hard-stopped.
TEST_F(WorkerManagerTestFixture, AcquireCompactionWorkPreemptsLeveling) {
    auto cmp_func =
      [](const l1::compaction_job_ptr& a, const l1::compaction_job_ptr& b) {
          return a->meta->ntp < b->meta->ntp;
      };

    l1::compaction_scheduler_probe probe;
    l1::compaction_queue pq(std::move(cmp_func));
    l1::leveling_extent_reclamation_policy lq_policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue lq(lq_policy.get_comparator());
    l1::log_list_t list;
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    start_workers(manager).get();
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });

    const auto shard = ss::this_shard_id();
    const auto ntp = model::ntp(
      model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));
    const auto tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), ntp.tp.partition);
    const auto base = kafka::offset{0};
    auto meta = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
    list.push_back(*meta);

    pq.push(
      ss::make_lw_shared<l1::compaction_job>(
        meta, l1::compaction_info_and_timestamp{}));

    // A queued leveling job for the CTP, plus an inflight one on this shard.
    lq.push(
      ss::make_lw_shared<l1::leveling_job>(
        meta,
        l1::levelable_range{
          .base_offset = kafka::offset{100},
          .last_offset = kafka::offset{199},
          .size_bytes = 1,
          .extent_count = 2},
        l1::metastore::compaction_epoch{0}));
    meta->leveling.inflight_shards[shard] = 1;
    add_inflight_leveling_job(manager, shard, tidp, base).get();

    auto work_opt = manager.try_acquire_compaction_work(shard);
    ASSERT_TRUE(work_opt.has_value());
    ASSERT_TRUE(work_opt.value()->meta->compaction.inflight_shard.has_value());

    // The CTP's queued leveling job was dropped on compaction dequeue.
    ASSERT_TRUE(lq.empty());

    // The preemption is dispatched asynchronously onto the manager's gate;
    // drain the task queue so it runs, then confirm the inflight leveling job
    // was hard-stopped.
    tests::drain_task_queue().get();
    ASSERT_EQ(
      inflight_leveling_state(manager, shard, tidp, base).get(),
      l1::compaction_job_state::hard_stop);
}

// `complete_leveling_work` decrements the CTP's inflight-shard count and stamps
// the just-completed range with a commit timestamp, replacing the `nullopt`
// recorded at acquire so the collector applies a post-commit cooldown before
// re-queueing the range.
TEST_F(
  WorkerManagerTestFixture, CompleteLevelingWorkClearsInflightAndStampsRange) {
    auto cmp_func =
      [](const l1::compaction_job_ptr& a, const l1::compaction_job_ptr& b) {
          return a->meta->ntp < b->meta->ntp;
      };
    l1::compaction_scheduler_probe probe;
    l1::compaction_queue pq(std::move(cmp_func));
    l1::leveling_extent_reclamation_policy lq_policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue lq(lq_policy.get_comparator());
    l1::log_list_t list;
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });

    const auto ntp = model::ntp(
      model::ns("kafka"), model::topic("level"), model::partition_id(0));
    const auto tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), ntp.tp.partition);
    auto meta = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
    list.push_back(*meta);

    const auto base = kafka::offset{0};
    const auto last = kafka::offset{99};
    lq.push(
      ss::make_lw_shared<l1::leveling_job>(
        meta,
        l1::levelable_range{
          .base_offset = base,
          .last_offset = last,
          .size_bytes = 1,
          .extent_count = 1},
        l1::metastore::compaction_epoch{0}));

    const auto shard = ss::this_shard_id();

    // The commit timestamp recorded for [base, last], or nullopt if the range
    // is present but uncommitted (or absent).
    auto range_commit_ts = [&]() -> std::optional<model::timestamp> {
        auto s = meta->leveling.inflight_ranges.make_stream();
        while (s.has_next()) {
            auto r = s.next();
            if (r.base_offset == base && r.last_offset == last) {
                return r.value;
            }
        }
        return std::nullopt;
    };

    auto work = manager.try_acquire_leveling_work(shard);
    ASSERT_TRUE(work.has_value());
    // Acquire marks the range inflight: shard count 1, range present with no
    // commit timestamp yet.
    ASSERT_EQ(meta->leveling.inflight_shards[shard], 1u);
    ASSERT_TRUE(meta->leveling.inflight_ranges.overlaps(base, last));
    ASSERT_FALSE(range_commit_ts().has_value());

    manager.complete_leveling_work(work.value().get(), shard);
    // Complete clears the shard count and stamps the range with a commit time.
    ASSERT_EQ(meta->leveling.inflight_shards[shard], 0u);
    ASSERT_TRUE(meta->leveling.inflight_ranges.overlaps(base, last));
    ASSERT_TRUE(range_commit_ts().has_value());
}

// `request_stop_leveling` hard-stops the inflight leveling jobs for a CTP on
// the worker shards that have one, located via the CTP's inflight-shard counts.
TEST_F(WorkerManagerTestFixture, RequestStopLevelingTerminatesInflightJobs) {
    auto cmp_func =
      [](const l1::compaction_job_ptr& a, const l1::compaction_job_ptr& b) {
          return a->meta->ntp < b->meta->ntp;
      };
    l1::compaction_scheduler_probe probe;
    l1::compaction_queue pq(std::move(cmp_func));
    l1::leveling_extent_reclamation_policy lq_policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue lq(lq_policy.get_comparator());
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    start_workers(manager).get();
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });

    const auto shard = ss::this_shard_id();
    const auto ntp = model::ntp(
      model::ns("kafka"), model::topic("level"), model::partition_id(0));
    const auto tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), ntp.tp.partition);
    const auto base = kafka::offset{0};

    auto meta = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
    // `request_stop_leveling` finds which shards to signal from the CTP's
    // inflight-shard counts; the matching handle on the worker is what gets
    // stopped.
    meta->leveling.inflight_shards[shard] = 1;
    add_inflight_leveling_job(manager, shard, tidp, base).get();
    ASSERT_EQ(
      inflight_leveling_state(manager, shard, tidp, base).get(),
      l1::compaction_job_state::idle);

    manager.request_stop_leveling(meta);
    // The stop is dispatched asynchronously onto the manager's gate; drain the
    // task queue so it runs, then confirm the handle was hard-stopped.
    tests::drain_task_queue().get();
    ASSERT_EQ(
      inflight_leveling_state(manager, shard, tidp, base).get(),
      l1::compaction_job_state::hard_stop);
}

// `do_pause_worker` must not resolve while a leveling job is still inflight:
// leveling jobs run on the gate independently of the leveling loop, so pausing
// the loop alone does not quiesce the worker. Pause must drain
// `_inflight_leveling` first. Simulate an inflight job by registering a handle
// directly, assert pause blocks on it, then drain it and assert pause resolves.
TEST_F(WorkerManagerTestFixture, PauseWaitsForInflightLevelingJobs) {
    auto cmp_func =
      [](const l1::compaction_job_ptr& a, const l1::compaction_job_ptr& b) {
          return a->meta->ntp < b->meta->ntp;
      };

    l1::compaction_scheduler_probe probe;
    l1::compaction_queue pq(std::move(cmp_func));
    l1::leveling_extent_reclamation_policy lq_policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue lq(lq_policy.get_comparator());
    l1::worker_manager manager(
      pq, lq, nullptr, nullptr, nullptr, probe, nullptr);
    start_workers(manager).get();
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });
    using worker_state = l1::compaction_worker::worker_state;

    const auto shard = ss::this_shard_id();
    const auto tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), model::partition_id(0));
    const auto base = kafka::offset{0};

    add_inflight_leveling_job(manager, shard, tidp, base).get();

    // Pause should block on the drain while the job is still inflight. Drain
    // the task queue so pause runs as far as it can: with the drain in place it
    // parks on `_leveling_drained_cv`, so the future must remain pending.
    auto pause_fut = manager.pause_worker(shard);
    tests::drain_task_queue().get();
    ASSERT_FALSE(pause_fut.available())
      << "pause resolved while a leveling job was still inflight";

    // The inflight job should have been asked to wind down gracefully.
    ASSERT_EQ(
      inflight_leveling_state(manager, shard, tidp, base).get(),
      l1::compaction_job_state::soft_stop);

    // Once the job winds down, pause must complete.
    drain_inflight_leveling(manager, shard).get();
    std::move(pause_fut).get();
    ASSERT_EQ(get_compaction_state(manager, shard).get(), worker_state::paused);
    ASSERT_EQ(get_leveling_state(manager, shard).get(), worker_state::paused);
}

// Verifies that `dirty_ratio_scheduling_policy` orders partitions from
// highest `dirty_ratio` to lowest.
TEST(DirtyRatioSchedulingPolicyTest, OrdersHighestDirtyRatioFirst) {
    auto make_job = [](std::string_view topic_name, double ratio) {
        auto ntp = model::ntp(
          model::ns("kafka"),
          model::topic(ss::sstring{topic_name}),
          model::partition_id(0));
        auto tidp = model::topic_id_partition(
          model::topic_id(uuid_t::create()), ntp.tp.partition);
        auto m = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
        return ss::make_lw_shared<l1::compaction_job>(
          std::move(m),
          l1::compaction_info_and_timestamp{
            .info = {.dirty_ratio = ratio},
            .collected_at = model::timestamp::now(),
            .max_compactible_offset = kafka::offset::max(),
          });
    };

    auto low = make_job("low", 0.1);
    auto mid = make_job("mid", 0.5);
    auto high = make_job("high", 0.9);

    l1::dirty_ratio_scheduling_policy policy;
    l1::compaction_queue q(policy.get_comparator());
    q.push(low);
    q.push(mid);
    q.push(high);

    // Pop order should be: most dirty -> least dirty.
    ASSERT_DOUBLE_EQ(q.top()->info_and_ts.info.dirty_ratio, 0.9);
    q.pop();
    ASSERT_DOUBLE_EQ(q.top()->info_and_ts.info.dirty_ratio, 0.5);
    q.pop();
    ASSERT_DOUBLE_EQ(q.top()->info_and_ts.info.dirty_ratio, 0.1);
}

// Verifies that `compaction_lag_scheduling_policy` orders partitions from
// highest lag (oldest `earliest_dirty_ts`) to lowest (most recent).
TEST(CompactionLagSchedulingPolicyTest, OrdersHighestLagFirst) {
    auto make_job = [](std::string_view topic_name, model::timestamp ts) {
        auto ntp = model::ntp(
          model::ns("kafka"),
          model::topic(ss::sstring{topic_name}),
          model::partition_id(0));
        auto tidp = model::topic_id_partition(
          model::topic_id(uuid_t::create()), ntp.tp.partition);
        auto m = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
        return ss::make_lw_shared<l1::compaction_job>(
          std::move(m),
          l1::compaction_info_and_timestamp{
            .info = {.earliest_dirty_ts = ts},
            .collected_at = model::timestamp::now(),
            .max_compactible_offset = kafka::offset::max(),
          });
    };

    // Smaller timestamp = older = higher lag.
    auto old_log = make_job("old", model::timestamp{1000});
    auto mid_log = make_job("mid", model::timestamp{5000});
    auto new_log = make_job("new", model::timestamp{9000});

    l1::compaction_lag_scheduling_policy policy;
    l1::compaction_queue q(policy.get_comparator());
    q.push(mid_log);
    q.push(new_log);
    q.push(old_log);

    // Pop order should be: oldest (highest lag) -> newest (lowest lag).
    ASSERT_EQ(
      q.top()->info_and_ts.info.earliest_dirty_ts, model::timestamp{1000});
    q.pop();
    ASSERT_EQ(
      q.top()->info_and_ts.info.earliest_dirty_ts, model::timestamp{5000});
    q.pop();
    ASSERT_EQ(
      q.top()->info_and_ts.info.earliest_dirty_ts, model::timestamp{9000});
}

// Verifies that `leveling_extent_reclamation_policy` orders jobs by
// expected extent-count reduction (input_extents - ceil(size / target)).
TEST(LevelingExtentReclamationPolicyTest, OrdersByExpectedReclaim) {
    auto make_job = [](size_t size_bytes, size_t extent_count) {
        auto ntp = model::ntp(
          model::ns("kafka"), model::topic("t"), model::partition_id(0));
        auto tidp = model::topic_id_partition(
          model::topic_id(uuid_t::create()), ntp.tp.partition);
        auto meta = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
        return ss::make_lw_shared<l1::leveling_job>(
          std::move(meta),
          l1::levelable_range{
            .base_offset = kafka::offset{0},
            .last_offset = kafka::offset{99},
            .size_bytes = size_bytes,
            .extent_count = extent_count,
          },
          cloud_topics::l1::metastore::compaction_epoch{0});
    };

    // target=100. Expected reclaim = input - ceil(size/100):
    //   tiny_many : 10  - ceil(50/100)=1   -> reclaim 9   (biggest)
    //   mid_few   : 5   - ceil(300/100)=3  -> reclaim 2
    //   big_fewest: 3   - ceil(200/100)=2  -> reclaim 1   (smallest)
    auto tiny_many = make_job(50, 10);
    auto mid_few = make_job(300, 5);
    auto big_fewest = make_job(200, 3);

    l1::leveling_extent_reclamation_policy policy{
      config::mock_binding<size_t>(100)};
    l1::leveling_queue q(policy.get_comparator());
    q.push(mid_few);
    q.push(big_fewest);
    q.push(tiny_many);

    ASSERT_EQ(q.top()->range.extent_count, 10u);
    q.pop();
    ASSERT_EQ(q.top()->range.extent_count, 5u);
    q.pop();
    ASSERT_EQ(q.top()->range.extent_count, 3u);
}
