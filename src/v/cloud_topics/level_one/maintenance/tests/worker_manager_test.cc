/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/leveling/leveling_queue.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/scheduler_probe.h"
#include "cloud_topics/level_one/maintenance/scheduling_policies.h"
#include "cloud_topics/level_one/maintenance/worker.h"
#include "cloud_topics/level_one/maintenance/worker_manager.h"
#include "config/property.h"
#include "model/fundamental.h"
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
          nullptr,
          nullptr);
        co_await manager._workers.invoke_on_all(&l1::compaction_worker::start);
    }

    ss::future<l1::compaction_worker::worker_state>
    get_worker_state(l1::worker_manager& manager, ss::shard_id shard) {
        return manager._workers.invoke_on(
          shard,
          [](l1::compaction_worker& worker) { return worker._worker_state; });
    }

    ss::future<bool>
    work_fut_has_value(l1::worker_manager& manager, ss::shard_id shard) {
        return manager._workers.invoke_on(
          shard, [](l1::compaction_worker& worker) {
              return worker._compaction_work_fut.has_value();
          });
    }
};

TEST_F(WorkerManagerTestFixture, PauseAndResumeWorkers) {
    l1::compaction_scheduler_probe probe;
    l1::log_compaction_queue pq;
    l1::worker_manager manager(
      pq, nullptr, nullptr, nullptr, probe, nullptr, nullptr);
    start_workers(manager).get();
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });
    using worker_state = l1::compaction_worker::worker_state;
    for (ss::shard_id i = 0; i < ss::this_smp_shard_count(); ++i) {
        // Workers start in active state
        ASSERT_EQ(get_worker_state(manager, i).get(), worker_state::active);
        ASSERT_TRUE(work_fut_has_value(manager, i).get());

        // Pause workers and expect to see state reflect that.
        manager.pause_worker(i).get();
        ASSERT_EQ(get_worker_state(manager, i).get(), worker_state::paused);
        ASSERT_FALSE(work_fut_has_value(manager, i).get());

        // Resume workers and expect to see active state.
        manager.resume_worker(i).get();
        ASSERT_EQ(get_worker_state(manager, i).get(), worker_state::active);
        ASSERT_TRUE(work_fut_has_value(manager, i).get());
    }
}

TEST_F(WorkerManagerTestFixture, AcquireWork) {
    auto cmp_func = [](
                      const l1::log_compaction_meta_ptr& a,
                      const l1::log_compaction_meta_ptr& b) {
        return a->ntp < b->ntp;
    };

    l1::compaction_scheduler_probe probe;
    l1::log_compaction_queue pq(std::move(cmp_func));
    l1::log_list_t list;
    l1::worker_manager manager(
      pq, nullptr, nullptr, nullptr, probe, nullptr, nullptr);
    auto stop_manager = ss::defer([&manager] { manager.stop().get(); });

    const auto test_ntp = model::ntp(
      model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));
    const auto test_tidp = model::topic_id_partition(
      model::topic_id(uuid_t::create()), test_ntp.tp.partition);
    auto meta = ss::make_lw_shared<l1::log_compaction_meta>(
      test_tidp, test_ntp);
    list.push_back(*meta);
    using status = l1::log_compaction_state::status;
    meta->compaction.s = status::queued;
    pq.emplace(meta);

    auto work_opt = manager.try_acquire_compaction_work(ss::this_shard_id());
    ASSERT_TRUE(work_opt.has_value());
    ASSERT_EQ(work_opt.value()->ntp, test_ntp);
    ASSERT_EQ(work_opt.value()->tidp, test_tidp);
    ASSERT_TRUE(work_opt.value()->compaction.inflight_shard.has_value());
    ASSERT_EQ(work_opt.value()->compaction.s, status::inflight);
    ASSERT_EQ(
      work_opt.value()->compaction.inflight_shard.value(), ss::this_shard_id());

    manager.complete_compaction_work(work_opt.value().get());
    ASSERT_FALSE(work_opt.value()->compaction.inflight_shard.has_value());
    ASSERT_EQ(work_opt.value()->compaction.s, status::idle);
}

// Verifies that `dirty_ratio_scheduling_policy` orders partitions from
// highest `dirty_ratio` to lowest.
TEST(DirtyRatioSchedulingPolicyTest, OrdersHighestDirtyRatioFirst) {
    auto make_meta = [](std::string_view topic_name, double ratio) {
        auto ntp = model::ntp(
          model::ns("kafka"),
          model::topic(ss::sstring{topic_name}),
          model::partition_id(0));
        auto tidp = model::topic_id_partition(
          model::topic_id(uuid_t::create()), ntp.tp.partition);
        auto m = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
        m->compaction.info_and_ts = l1::compaction_info_and_timestamp{
          .info = {.dirty_ratio = ratio},
          .collected_at = model::timestamp::now(),
          .max_compactible_offset = kafka::offset::max(),
        };
        return m;
    };

    auto low = make_meta("low", 0.1);
    auto mid = make_meta("mid", 0.5);
    auto high = make_meta("high", 0.9);

    l1::dirty_ratio_scheduling_policy policy;
    l1::log_compaction_queue q(policy.get_comparator());
    q.push(low);
    q.push(mid);
    q.push(high);

    // Pop order should be: most dirty -> least dirty.
    ASSERT_DOUBLE_EQ(q.top()->compaction.info_and_ts->info.dirty_ratio, 0.9);
    q.pop();
    ASSERT_DOUBLE_EQ(q.top()->compaction.info_and_ts->info.dirty_ratio, 0.5);
    q.pop();
    ASSERT_DOUBLE_EQ(q.top()->compaction.info_and_ts->info.dirty_ratio, 0.1);
}

// Verifies that `compaction_lag_scheduling_policy` orders partitions from
// highest lag (oldest `earliest_dirty_ts`) to lowest (most recent).
TEST(CompactionLagSchedulingPolicyTest, OrdersHighestLagFirst) {
    auto make_meta = [](std::string_view topic_name, model::timestamp ts) {
        auto ntp = model::ntp(
          model::ns("kafka"),
          model::topic(ss::sstring{topic_name}),
          model::partition_id(0));
        auto tidp = model::topic_id_partition(
          model::topic_id(uuid_t::create()), ntp.tp.partition);
        auto m = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
        m->compaction.info_and_ts = l1::compaction_info_and_timestamp{
          .info = {.earliest_dirty_ts = ts},
          .collected_at = model::timestamp::now(),
          .max_compactible_offset = kafka::offset::max(),
        };
        return m;
    };

    // Smaller timestamp = older = higher lag.
    auto old_log = make_meta("old", model::timestamp{1000});
    auto mid_log = make_meta("mid", model::timestamp{5000});
    auto new_log = make_meta("new", model::timestamp{9000});

    l1::compaction_lag_scheduling_policy policy;
    l1::log_compaction_queue q(policy.get_comparator());
    q.push(mid_log);
    q.push(new_log);
    q.push(old_log);

    // Pop order should be: oldest (highest lag) -> newest (lowest lag).
    ASSERT_EQ(
      q.top()->compaction.info_and_ts->info.earliest_dirty_ts,
      model::timestamp{1000});
    q.pop();
    ASSERT_EQ(
      q.top()->compaction.info_and_ts->info.earliest_dirty_ts,
      model::timestamp{5000});
    q.pop();
    ASSERT_EQ(
      q.top()->compaction.info_and_ts->info.earliest_dirty_ts,
      model::timestamp{9000});
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
