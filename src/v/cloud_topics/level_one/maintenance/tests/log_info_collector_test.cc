/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "cloud_topics/level_one/maintenance/log_info_collector.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/scheduling_policies.h"
#include "cloud_topics/level_one/metastore/offset_interval_map.h"
#include "cluster/topic_configuration.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/tests/random_batch.h"

#include <gtest/gtest.h>

using namespace cloud_topics;

namespace {
// Rebuilds `ranges` with every entry's value set to `committed_at`, simulating
// a commit. The map has no per-range mutation, so rebuild from a stream,
// mirroring how `offset_interval_set` callers mutate.
void mark_all_committed(
  l1::offset_interval_map<std::optional<model::timestamp>>& ranges,
  model::timestamp committed_at) {
    l1::offset_interval_map<std::optional<model::timestamp>> rebuilt;
    auto s = ranges.make_stream();
    while (s.has_next()) {
        auto range = s.next();
        rebuilt.insert(range.base_offset, range.last_offset, committed_at);
    }
    ranges = std::move(rebuilt);
}

// Simulates workers dequeuing every job from `queue` and recording each range
// as inflight on its owning CTP, as the worker_manager does on dequeue. The
// collector itself no longer records inflight ranges.
void drain_and_mark_inflight(l1::leveling_queue& queue) {
    while (!queue.empty()) {
        const auto& job = queue.top();
        job->meta->leveling.inflight_ranges.insert(
          job->range.base_offset, job->range.last_offset, std::nullopt);
        queue.pop();
    }
}
} // namespace

class LogInfoCollectorTestFixture : public l1::l1_reader_fixture {
protected:
    // Builds `n` separate small (undersized) L1 objects for `tidp`, each from
    // its own batch run, so they form a run of consecutive undersized extents
    // that the leveling range builder will coalesce into a range.
    void seed_undersized_objects(const model::topic_id_partition& tidp, int n) {
        model::offset o{0};
        for (int i = 0; i < n; ++i) {
            auto batches = model::test::make_random_batches(o, 10).get();
            o = model::next_offset(batches.back().last_offset());
            std::vector<tidp_batches_t> bs;
            bs.emplace_back(tidp, std::move(batches));
            make_l1_objects(std::move(bs)).get();
        }
    }
};

// A fake topic config provider which always returns a value. The config is
// marked compacted so it passes the compaction loop's `is_compacted()` filter
// in `build_compaction_specs`/`needs_compaction`.
class fake_cfg_provider : public l1::topic_cfg_provider {
public:
    fake_cfg_provider() {
        _cfg.properties.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
    }

    std::optional<std::reference_wrapper<const cluster::topic_configuration>>
    get_topic_cfg(model::topic_namespace_view) const final {
        return _cfg;
    }

private:
    cluster::topic_configuration _cfg{};
};

// A fake offset provider which always returns kafka::offset::max().
class fake_offset_provider : public l1::max_compactible_offset_provider {
public:
    ss::future<> fill_max_compactible_offsets(
      chunked_hash_map<model::ntp, kafka::offset>&) const final {
        co_return;
    }
};

TEST_F(LogInfoCollectorTestFixture, TestInfoCollector) {
    auto cfg_provider = std::make_unique<fake_cfg_provider>();
    auto offset_provider = std::make_unique<fake_offset_provider>();
    l1::log_info_collector log_info_collector(
      &_metastore, std::move(cfg_provider), std::move(offset_provider));
    std::vector<std::pair<model::ntp, model::topic_id_partition>> ntidps;
    const auto topic_names = {"topic_a", "topic_b", "topic_c"};
    const auto num_topics = topic_names.size();
    for (const auto& topic : topic_names) {
        ntidps.push_back(make_ntidp(topic));
    }

    std::vector<tidp_batches_t> tidp_batches;
    l1::log_set_t logs;
    l1::compaction_queue cached_metadata(
      [](const l1::compaction_job_ptr& a, const l1::compaction_job_ptr& b) {
          return a->meta->ntp < b->meta->ntp;
      });
    l1::log_list_t logs_list;
    for (const auto& [ntp, tidp] : ntidps) {
        auto [it, success] = logs.emplace(
          ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp));
        logs_list.push_back(*it->get());
        auto batches
          = model::test::make_random_batches(model::offset{0}, 10).get();
        tidp_batches.emplace_back(tidp, std::move(batches));
    }

    make_l1_objects(std::move(tidp_batches)).get();
    log_info_collector.collect_compaction_info(logs, logs_list, cached_metadata)
      .get();
    ASSERT_EQ(cached_metadata.size(), num_topics);
    while (!cached_metadata.empty()) {
        auto sample = cached_metadata.top();
        cached_metadata.pop();
        ASSERT_FLOAT_EQ(sample->info_and_ts.info.dirty_ratio, 1.0);
        ASSERT_TRUE(sample->info_and_ts.info.earliest_dirty_ts.has_value());
    }
}

TEST_F(LogInfoCollectorTestFixture, TestSampleLevelingInfo) {
    auto cfg_provider = std::make_unique<fake_cfg_provider>();
    auto offset_provider = std::make_unique<fake_offset_provider>();
    l1::log_info_collector log_info_collector(
      &_metastore, std::move(cfg_provider), std::move(offset_provider));

    auto [ntp, tidp] = make_ntidp("leveling_topic");
    auto log_ptr = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);

    // Seed the metastore with two small objects for this partition. With the
    // default config (max_object_size=80MiB, threshold=0.5 =>
    // min_acceptable=40MiB), each object is undersized. The leveling range
    // builder only emits a range when it sees a run of *two or more*
    // consecutive undersized extents (singletons can't reduce extent count),
    // so we need at least two objects to produce a non-empty range.
    model::offset o{0};
    {
        auto batches = model::test::make_random_batches(o, 10).get();
        o = model::next_offset(batches.back().last_offset());
        std::vector<tidp_batches_t> bs;
        bs.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(bs)).get();
    }

    {
        auto batches = model::test::make_random_batches(o, 10).get();
        std::vector<tidp_batches_t> bs;
        bs.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(bs)).get();
    }

    l1::log_set_t logs_set;
    auto [it, inserted] = logs_set.insert(log_ptr);
    ASSERT_TRUE(inserted);
    l1::log_list_t logs_list;
    logs_list.push_back(*log_ptr);
    l1::leveling_extent_reclamation_policy policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue queue(policy.get_comparator());

    log_info_collector.collect_leveling_info(logs_set, logs_list, queue).get();

    ASSERT_GT(queue.size(), 0u);
    size_t total_size_bytes = 0;
    while (!queue.empty()) {
        total_size_bytes += queue.top()->range.size_bytes;
        queue.pop();
    }
    ASSERT_GT(total_size_bytes, 0u);
}

// While a range is still inflight (recorded in `inflight_ranges` with a nullopt
// value), a subsequent collection must not re-queue it, even though the
// metastore still reports the underlying extents as undersized.
TEST_F(LogInfoCollectorTestFixture, TestLevelingDoesNotReQueueInflightRange) {
    auto [ntp, tidp] = make_ntidp("leveling_topic");
    auto log_ptr = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
    seed_undersized_objects(tidp, 2);

    l1::log_set_t logs_set;
    logs_set.insert(log_ptr);
    l1::log_list_t logs_list;
    logs_list.push_back(*log_ptr);

    l1::log_info_collector collector(
      &_metastore,
      std::make_unique<fake_cfg_provider>(),
      std::make_unique<fake_offset_provider>());
    l1::leveling_extent_reclamation_policy policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue queue(policy.get_comparator());

    // First collection queues the range(s). Collection records nothing as
    // inflight; that happens when a worker dequeues the job.
    collector.collect_leveling_info(logs_set, logs_list, queue).get();
    ASSERT_GT(queue.size(), 0u);

    // Simulate workers dequeuing the jobs and recording them as inflight.
    drain_and_mark_inflight(queue);
    const auto inflight_count = log_ptr->leveling.inflight_ranges.size();
    ASSERT_GT(inflight_count, 0u);

    // Second collection must not re-queue the still-inflight ranges.
    collector.collect_leveling_info(logs_set, logs_list, queue).get();
    ASSERT_TRUE(queue.empty());
    ASSERT_EQ(log_ptr->leveling.inflight_ranges.size(), inflight_count);
}

// A range returned by the metastore must not be queued if it *overlaps* an
// already-inflight range, even when the bounds differ: the metastore can
// re-derive slightly different ranges over the same still-pending extents, so
// dedup is by overlap rather than exact match.
TEST_F(LogInfoCollectorTestFixture, TestLevelingSkipsOverlappingRange) {
    auto [ntp, tidp] = make_ntidp("leveling_topic");
    auto log_ptr = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
    seed_undersized_objects(tidp, 2);

    l1::log_set_t logs_set;
    logs_set.insert(log_ptr);
    l1::log_list_t logs_list;
    logs_list.push_back(*log_ptr);

    l1::log_info_collector collector(
      &_metastore,
      std::make_unique<fake_cfg_provider>(),
      std::make_unique<fake_offset_provider>());
    l1::leveling_extent_reclamation_policy policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue queue(policy.get_comparator());

    // Record an inflight range, dequeued on a prior tick, with bounds that do
    // not match but do overlap the range the metastore will return (which
    // starts at offset 0). It is inflight (nullopt), so it survives eviction.
    ASSERT_TRUE(log_ptr->leveling.inflight_ranges.insert(
      kafka::offset{0}, kafka::offset{5}, std::nullopt));

    // The returned range overlaps the recorded one, so nothing is queued and
    // the recorded range is left untouched.
    collector.collect_leveling_info(logs_set, logs_list, queue).get();
    ASSERT_TRUE(queue.empty());
    ASSERT_EQ(log_ptr->leveling.inflight_ranges.size(), 1u);
}

// Leveling and compaction of the same CTP shouldn't be scheduled concurrently:
// while a CTP has an inflight compaction, leveling collection should not queue
// jobs for it. Once the compaction completes (clearing `inflight_shard`),
// collection queues its ranges again.
TEST_F(LogInfoCollectorTestFixture, TestLevelingSkipsCompactingLog) {
    auto [ntp, tidp] = make_ntidp("leveling_topic");
    auto log_ptr = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
    seed_undersized_objects(tidp, 2);

    l1::log_set_t logs_set;
    logs_set.insert(log_ptr);
    l1::log_list_t logs_list;
    logs_list.push_back(*log_ptr);

    l1::log_info_collector collector(
      &_metastore,
      std::make_unique<fake_cfg_provider>(),
      std::make_unique<fake_offset_provider>());
    l1::leveling_extent_reclamation_policy policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue queue(policy.get_comparator());

    log_ptr->compaction.inflight_shard = ss::this_shard_id();
    collector.collect_leveling_info(logs_set, logs_list, queue).get();
    ASSERT_TRUE(queue.empty());

    log_ptr->compaction.inflight_shard.reset();
    collector.collect_leveling_info(logs_set, logs_list, queue).get();
    ASSERT_GT(queue.size(), 0u);
}

// A range whose completion timestamp is not strictly before the collection's
// snapshot must be retained: the collector cannot yet assume the metastore
// reflects the commit, so the overlapping range must not be re-queued.
TEST_F(LogInfoCollectorTestFixture, TestLevelingRetainsRecentlyCommittedRange) {
    auto [ntp, tidp] = make_ntidp("leveling_topic");
    auto log_ptr = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
    seed_undersized_objects(tidp, 2);

    l1::log_set_t logs_set;
    logs_set.insert(log_ptr);
    l1::log_list_t logs_list;
    logs_list.push_back(*log_ptr);

    l1::log_info_collector collector(
      &_metastore,
      std::make_unique<fake_cfg_provider>(),
      std::make_unique<fake_offset_provider>());
    l1::leveling_extent_reclamation_policy policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue queue(policy.get_comparator());

    collector.collect_leveling_info(logs_set, logs_list, queue).get();
    ASSERT_GT(queue.size(), 0u);
    // Simulate workers dequeuing the jobs and recording them as inflight.
    drain_and_mark_inflight(queue);

    // Mark every inflight range as committed at a timestamp no collection
    // snapshot can be strictly past, so the eviction check
    // (collection_timestamp > committed_at) is deterministically false and the
    // ranges are retained.
    mark_all_committed(
      log_ptr->leveling.inflight_ranges, model::timestamp::max());

    // The retained ranges overlap the metastore's, so nothing is re-queued.
    collector.collect_leveling_info(logs_set, logs_list, queue).get();
    ASSERT_TRUE(queue.empty());
}

// A range whose completion timestamp predates the collection snapshot is
// assumed visible to the metastore, so its entry is evicted and the range
// becomes schedulable again: it is re-queued.
TEST_F(LogInfoCollectorTestFixture, TestLevelingReschedulesEvictedRange) {
    auto [ntp, tidp] = make_ntidp("leveling_topic");
    auto log_ptr = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
    seed_undersized_objects(tidp, 2);

    l1::log_set_t logs_set;
    logs_set.insert(log_ptr);
    l1::log_list_t logs_list;
    logs_list.push_back(*log_ptr);

    l1::log_info_collector collector(
      &_metastore,
      std::make_unique<fake_cfg_provider>(),
      std::make_unique<fake_offset_provider>());
    l1::leveling_extent_reclamation_policy policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue queue(policy.get_comparator());

    collector.collect_leveling_info(logs_set, logs_list, queue).get();
    ASSERT_GT(queue.size(), 0u);
    // Simulate workers dequeuing the jobs and recording them as inflight.
    drain_and_mark_inflight(queue);

    // Mark every inflight range as committed at the epoch, well before any
    // collection snapshot, so each entry is evicted.
    mark_all_committed(log_ptr->leveling.inflight_ranges, model::timestamp{0});

    collector.collect_leveling_info(logs_set, logs_list, queue).get();
    // With the stale entries evicted, the ranges no longer overlap anything
    // inflight and are re-queued.
    ASSERT_GT(queue.size(), 0u);
    // Eviction emptied the inflight map and collection does not re-record (that
    // happens on dequeue).
    ASSERT_TRUE(log_ptr->leveling.inflight_ranges.empty());
}

// A single collection's eviction must act per-entry: a range committed before
// the snapshot is dropped while one committed at/after it is retained, both
// independently of the range the metastore returns this tick.
TEST_F(LogInfoCollectorTestFixture, TestLevelingEvictsOnlyStaleRanges) {
    auto [ntp, tidp] = make_ntidp("leveling_topic");
    auto log_ptr = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
    // The metastore will return a range over [0, N] for some N well below the
    // offsets used for the pre-recorded ranges below.
    seed_undersized_objects(tidp, 2);

    l1::log_set_t logs_set;
    logs_set.insert(log_ptr);
    l1::log_list_t logs_list;
    logs_list.push_back(*log_ptr);

    l1::log_info_collector collector(
      &_metastore,
      std::make_unique<fake_cfg_provider>(),
      std::make_unique<fake_offset_provider>());
    l1::leveling_extent_reclamation_policy policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue queue(policy.get_comparator());

    // Pre-record two disjoint inflight ranges (disjoint from the metastore's
    // range too): one committed at the epoch (stale, must be evicted) and one
    // committed at max (must be retained).
    auto& inflight = log_ptr->leveling.inflight_ranges;
    ASSERT_TRUE(inflight.insert(
      kafka::offset{1000}, kafka::offset{1005}, model::timestamp{0}));
    ASSERT_TRUE(inflight.insert(
      kafka::offset{2000}, kafka::offset{2005}, model::timestamp::max()));

    collector.collect_leveling_info(logs_set, logs_list, queue).get();

    // The metastore's range is queued (it overlaps neither pre-recorded range),
    // the stale range is evicted, and the recent range is retained. Collection
    // records nothing in inflight_ranges (that happens on dequeue), so the
    // metastore's range does not appear here.
    ASSERT_GT(queue.size(), 0u);
    ASSERT_FALSE(inflight.contains(kafka::offset{1000}));
    ASSERT_TRUE(inflight.contains(kafka::offset{2000}));
    ASSERT_FALSE(inflight.contains(kafka::offset{0}));
    ASSERT_EQ(inflight.size(), 1u);
}

// A fresh metastore sample overwrites the CTP's queue: a previously-queued
// range that the new sample does not reproduce (and is not inflight) is
// dropped, and re-collecting identical ranges does not duplicate them.
TEST_F(LogInfoCollectorTestFixture, TestLevelingFreshSampleSupersedesQueue) {
    auto [ntp, tidp] = make_ntidp("leveling_topic");
    auto log_ptr = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);
    seed_undersized_objects(tidp, 2);

    l1::log_set_t logs_set;
    logs_set.insert(log_ptr);
    l1::log_list_t logs_list;
    logs_list.push_back(*log_ptr);

    l1::log_info_collector collector(
      &_metastore,
      std::make_unique<fake_cfg_provider>(),
      std::make_unique<fake_offset_provider>());
    l1::leveling_extent_reclamation_policy policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue queue(policy.get_comparator());

    collector.collect_leveling_info(logs_set, logs_list, queue).get();
    const auto queued = queue.size();
    ASSERT_GT(queued, 0u);
    ASSERT_EQ(queue.partition_count(), 1u);

    // Inject a stale job for the CTP that the next sample will not reproduce.
    constexpr auto stale_marker = kafka::offset{10'000'000};
    queue.push(
      ss::make_lw_shared<l1::leveling_job>(
        log_ptr,
        l1::levelable_range{
          .base_offset = stale_marker,
          .last_offset = kafka::offset{10'000'005},
          .size_bytes = 1,
          .extent_count = 1,
        },
        l1::metastore::compaction_epoch{0}));
    ASSERT_EQ(queue.size(), queued + 1);

    // Re-collect the same (still-undersized, nothing inflight) ranges. The
    // CTP's queue is rebuilt from scratch: the stale job is gone and the
    // reproduced ranges are not duplicated. Without the overwrite, nothing
    // inflight means nothing would be skipped and the size would grow.
    collector.collect_leveling_info(logs_set, logs_list, queue).get();
    EXPECT_EQ(queue.size(), queued);
    EXPECT_EQ(queue.partition_count(), 1u);
    while (!queue.empty()) {
        EXPECT_NE(queue.top()->range.base_offset, stale_marker);
        queue.pop();
    }
}

// Overwriting one CTP's queue on a fresh sample must not disturb another CTP's
// queued ranges: `clear()` is per-partition.
TEST_F(
  LogInfoCollectorTestFixture, TestLevelingFreshSampleIsolatedPerPartition) {
    auto [ntp_a, tidp_a] = make_ntidp("leveling_topic_a");
    auto [ntp_b, tidp_b] = make_ntidp("leveling_topic_b");
    auto log_a = ss::make_lw_shared<l1::log_compaction_meta>(tidp_a, ntp_a);
    auto log_b = ss::make_lw_shared<l1::log_compaction_meta>(tidp_b, ntp_b);
    seed_undersized_objects(tidp_a, 2);
    seed_undersized_objects(tidp_b, 2);

    l1::log_set_t logs_set;
    logs_set.insert(log_a);
    logs_set.insert(log_b);
    l1::log_list_t logs_list;
    logs_list.push_back(*log_a);
    logs_list.push_back(*log_b);

    l1::log_info_collector collector(
      &_metastore,
      std::make_unique<fake_cfg_provider>(),
      std::make_unique<fake_offset_provider>());
    l1::leveling_extent_reclamation_policy policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue queue(policy.get_comparator());

    collector.collect_leveling_info(logs_set, logs_list, queue).get();
    ASSERT_EQ(queue.partition_count(), 2u);
    const auto total = queue.size();
    ASSERT_GT(total, 0u);

    // Re-collect a fresh sample for only partition A (drop B from the list).
    // A's queue is overwritten, but B's queued ranges must remain.
    logs_list.erase(logs_list.iterator_to(*log_b));
    collector.collect_leveling_info(logs_set, logs_list, queue).get();

    EXPECT_EQ(queue.partition_count(), 2u);
    EXPECT_EQ(queue.size(), total);
}
