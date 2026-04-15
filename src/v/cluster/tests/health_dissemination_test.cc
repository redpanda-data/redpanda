// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/health_monitor_backend.h"
#include "cluster/health_monitor_types.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "serde/async.h"

#include <seastar/core/shared_ptr.hh>

#include <gtest/gtest.h>

#include <initializer_list>

using namespace cluster::health;

namespace {

/// Find a topic entry in the diff. Returns iterator or end().
auto find_topic(
  const topic_partition_metadata_diff& d, const model::topic_namespace& tp) {
    return std::ranges::find_if(
      d, [&](const auto& e) { return e.first == tp; });
}

const model::topic_namespace tp_ns{
  model::ns{"kafka"}, model::topic{"test-topic"}};
const model::topic_namespace tp_ns2{
  model::ns{"kafka"}, model::topic{"other-topic"}};

partition_metadata make_meta(
  model::term_id term = model::term_id{1},
  std::optional<model::node_id> leader = model::node_id{0}) {
    return {.term = term, .leader_id = leader};
}

partition_data
make_data(size_t size = 1000, kafka::offset hwm = kafka::offset{100}) {
    return {.size_bytes = size, .high_watermark = hwm};
}

health_snapshot_ptr make_snapshot(
  std::initializer_list<std::pair<model::partition_id, partition_data>>
    data_parts) {
    auto snap = ss::make_lw_shared<health_snapshot>();
    snap->data.try_emplace(tp_ns, data_parts.begin(), data_parts.end());
    return snap;
}

node_health make_report(
  std::initializer_list<std::pair<model::partition_id, partition_metadata>>
    parts,
  std::initializer_list<std::pair<model::partition_id, partition_data>>
    data_parts) {
    node_health nh{
      .snapshot = make_snapshot(data_parts),
      .metadata = ss::make_lw_shared<topic_partition_metadata_map>()};
    nh.metadata->try_emplace(tp_ns, ss::chunked_hash_map_from_range(parts));
    return nh;
}

void check_reports_equal(const node_health& lhs, const node_health& rhs) {
    EXPECT_EQ(*lhs.metadata, *rhs.metadata);
    ASSERT_TRUE(lhs.snapshot && rhs.snapshot);
    EXPECT_EQ(*lhs.snapshot, *rhs.snapshot);
}

} // namespace

TEST(DiffEntry, ApplyProducesExpectedReport) {
    auto old_report = make_report(
      {{model::partition_id{0}, make_meta(model::term_id{1})}},
      {{model::partition_id{0}, make_data(100)}});
    auto new_report = make_report(
      {{model::partition_id{0}, make_meta(model::term_id{5})}},
      {{model::partition_id{0}, make_data(999)}});

    diff_entry diff(old_report, new_report);
    diff.apply_to(old_report);

    check_reports_equal(old_report, new_report);
}

TEST(DiffEntry, ApplyTombstoneRemovesPartition) {
    auto old_report = make_report(
      {{model::partition_id{0}, make_meta()},
       {model::partition_id{1}, make_meta()}},
      {{model::partition_id{0}, make_data()},
       {model::partition_id{1}, make_data()}});
    auto new_report = make_report(
      {{model::partition_id{1}, make_meta()}},
      {{model::partition_id{1}, make_data()}});

    diff_entry diff(old_report, new_report);
    diff.apply_to(old_report);

    EXPECT_FALSE(
      (*old_report.metadata)[tp_ns].contains(model::partition_id{0}));
    EXPECT_TRUE((*old_report.metadata)[tp_ns].contains(model::partition_id{1}));
}

TEST(DiffEntry, ApplyTombstoneRemovesEmptyTopic) {
    auto old_report = make_report(
      {{model::partition_id{0}, make_meta()}},
      {{model::partition_id{0}, make_data()}});
    auto new_report = make_report({}, {});

    diff_entry diff(old_report, new_report);
    diff.apply_to(old_report);

    EXPECT_FALSE(old_report.metadata->contains(tp_ns));
}

TEST(DiffEntry, ComposeAndApplyProducesExpectedReport) {
    // A → B: partition 0 changes term, partition 1 added
    // B → C: partition 0 changes term again, partition 1 removed
    auto report_a = make_report(
      {{model::partition_id{0}, make_meta(model::term_id{1})}},
      {{model::partition_id{0}, make_data(100)}});
    auto report_b = make_report(
      {{model::partition_id{0}, make_meta(model::term_id{2})},
       {model::partition_id{1}, make_meta(model::term_id{1})}},
      {{model::partition_id{0}, make_data(200)},
       {model::partition_id{1}, make_data(50)}});
    auto report_c = make_report(
      {{model::partition_id{0}, make_meta(model::term_id{3})}},
      {{model::partition_id{0}, make_data(300)}});

    diff_entry a_to_b(report_a, report_b);
    diff_entry b_to_c(report_b, report_c);

    a_to_b.compose(b_to_c);
    a_to_b.apply_to(report_a);

    // report_a should now match report_c
    check_reports_equal(report_a, report_c);
}

namespace {
// Helper: count total partition entries across all topics in a diff
size_t diff_partition_count(const diff_entry& d) {
    size_t count = 0;
    for (const auto& [_, parts] : *d.metadata_diff) {
        count += parts.size();
    }
    return count;
}

// Helper: build a multi-topic report
node_health make_multi_topic_report(
  const model::topic_namespace& t1,
  std::initializer_list<std::pair<model::partition_id, partition_metadata>>
    t1_parts,
  const model::topic_namespace& t2,
  std::initializer_list<std::pair<model::partition_id, partition_metadata>>
    t2_parts) {
    node_health nh;
    nh.snapshot = ss::make_lw_shared<health_snapshot>();
    nh.metadata = ss::make_lw_shared<topic_partition_metadata_map>();
    if (t1_parts.size()) {
        nh.metadata->try_emplace(t1, ss::chunked_hash_map_from_range(t1_parts));
    }
    if (t2_parts.size()) {
        nh.metadata->try_emplace(t2, ss::chunked_hash_map_from_range(t2_parts));
    }
    return nh;
}

} // namespace

// --- diff_entry (diff size, multi-topic, topic removal) ---

TEST(DiffEntry, DiffSizeIsMinimal) {
    // Only changed partitions appear in the diff, not all of them.
    auto old_report = make_report(
      {{model::partition_id{0}, make_meta(model::term_id{1})},
       {model::partition_id{1}, make_meta(model::term_id{1})},
       {model::partition_id{2}, make_meta(model::term_id{1})}},
      {{model::partition_id{0}, make_data()},
       {model::partition_id{1}, make_data()},
       {model::partition_id{2}, make_data()}});

    // Only partition 1 changes
    auto new_report = make_report(
      {{model::partition_id{0}, make_meta(model::term_id{1})},
       {model::partition_id{1}, make_meta(model::term_id{5})},
       {model::partition_id{2}, make_meta(model::term_id{1})}},
      {{model::partition_id{0}, make_data()},
       {model::partition_id{1}, make_data()},
       {model::partition_id{2}, make_data()}});

    diff_entry diff(old_report, new_report);

    EXPECT_EQ(diff_partition_count(diff), 1);
}

TEST(DiffEntry, ComposedDiffSizeIsUnion) {
    // A→B changes p0, B→C changes p1. Composed diff has 2 entries.
    auto report_a = make_report(
      {{model::partition_id{0}, make_meta(model::term_id{1})},
       {model::partition_id{1}, make_meta(model::term_id{1})}},
      {{model::partition_id{0}, make_data()},
       {model::partition_id{1}, make_data()}});
    auto report_b = make_report(
      {{model::partition_id{0}, make_meta(model::term_id{2})},
       {model::partition_id{1}, make_meta(model::term_id{1})}},
      {{model::partition_id{0}, make_data()},
       {model::partition_id{1}, make_data()}});
    auto report_c = make_report(
      {{model::partition_id{0}, make_meta(model::term_id{2})},
       {model::partition_id{1}, make_meta(model::term_id{3})}},
      {{model::partition_id{0}, make_data()},
       {model::partition_id{1}, make_data()}});

    diff_entry a_to_b(report_a, report_b);
    diff_entry b_to_c(report_b, report_c);

    EXPECT_EQ(diff_partition_count(a_to_b), 1);
    EXPECT_EQ(diff_partition_count(b_to_c), 1);

    a_to_b.compose(b_to_c);
    EXPECT_EQ(diff_partition_count(a_to_b), 2);

    a_to_b.apply_to(report_a);
    check_reports_equal(report_a, report_c);
}

TEST(DiffEntry, MultiTopicDiff) {
    auto old_report = make_multi_topic_report(
      tp_ns,
      {{model::partition_id{0}, make_meta(model::term_id{1})}},
      tp_ns2,
      {{model::partition_id{0}, make_meta(model::term_id{1})}});

    // Change partition in topic 1, leave topic 2 unchanged
    auto new_report = make_multi_topic_report(
      tp_ns,
      {{model::partition_id{0}, make_meta(model::term_id{5})}},
      tp_ns2,
      {{model::partition_id{0}, make_meta(model::term_id{1})}});

    diff_entry diff(old_report, new_report);

    // Only 1 partition changed (in topic 1), topic 2 absent from diff
    EXPECT_EQ(diff_partition_count(diff), 1);
    EXPECT_NE(
      find_topic(*diff.metadata_diff, tp_ns), diff.metadata_diff->end());
    EXPECT_EQ(
      find_topic(*diff.metadata_diff, tp_ns2), diff.metadata_diff->end());

    diff.apply_to(old_report);
    check_reports_equal(old_report, new_report);
}

TEST(DiffEntry, TopicRemoval) {
    auto old_report = make_multi_topic_report(
      tp_ns,
      {{model::partition_id{0}, make_meta()},
       {model::partition_id{1}, make_meta()}},
      tp_ns2,
      {{model::partition_id{0}, make_meta()}});

    // Topic 2 removed entirely
    auto new_report = make_multi_topic_report(
      tp_ns,
      {{model::partition_id{0}, make_meta()},
       {model::partition_id{1}, make_meta()}},
      tp_ns2,
      {});

    diff_entry diff(old_report, new_report);

    // Topic 2's partition should be tombstoned
    auto t2_it = find_topic(*diff.metadata_diff, tp_ns2);
    ASSERT_NE(t2_it, diff.metadata_diff->end());
    EXPECT_EQ(t2_it->second.size(), 1); // 1 tombstone
    // Topic 1 unchanged — absent from diff
    EXPECT_EQ(
      find_topic(*diff.metadata_diff, tp_ns), diff.metadata_diff->end());

    diff.apply_to(old_report);
    check_reports_equal(old_report, new_report);
}

TEST(DiffEntry, TopicAddedAndRemoved) {
    // A: only topic 1
    auto report_a = make_multi_topic_report(
      tp_ns,
      {{model::partition_id{0}, make_meta(model::term_id{1})}},
      tp_ns2,
      {});

    // B: topic 2 added
    auto report_b = make_multi_topic_report(
      tp_ns,
      {{model::partition_id{0}, make_meta(model::term_id{1})}},
      tp_ns2,
      {{model::partition_id{0}, make_meta(model::term_id{1})}});

    // C: topic 2 removed again
    auto report_c = make_multi_topic_report(
      tp_ns,
      {{model::partition_id{0}, make_meta(model::term_id{1})}},
      tp_ns2,
      {});

    diff_entry a_to_b(report_a, report_b);
    diff_entry b_to_c(report_b, report_c);

    // A→B: 1 addition, B→C: 1 tombstone
    EXPECT_EQ(diff_partition_count(a_to_b), 1);
    EXPECT_EQ(diff_partition_count(b_to_c), 1);

    a_to_b.compose(b_to_c);

    // Composed: topic 2 partition added then removed → tombstone remains
    // (could be optimized away, but we don't)
    EXPECT_EQ(diff_partition_count(a_to_b), 1);

    a_to_b.apply_to(report_a);
    check_reports_equal(report_a, report_c);
}

// --- diff_store ---

TEST(DiffStore, AddAndGetDiff) {
    diff_store store;
    diff_entry d;
    d.start = node_health_version{1};
    d.end = node_health_version{2};
    store.add(std::move(d));

    EXPECT_EQ(*store.latest_version(), node_health_version{2});

    auto* result = store.get_diff(node_health_version{1});
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->start, node_health_version{1});
    EXPECT_EQ(result->end, node_health_version{2});
}

TEST(DiffStore, GetDiffNotFound) {
    diff_store store;
    diff_entry d;
    d.start = node_health_version{1};
    d.end = node_health_version{2};
    store.add(std::move(d));

    EXPECT_EQ(store.get_diff(node_health_version{0}), nullptr);
    EXPECT_EQ(store.get_diff(node_health_version{5}), nullptr);
}

TEST(DiffStore, ForwardComposition) {
    diff_store store;
    for (int i = 1; i <= 4; ++i) {
        diff_entry d;
        d.start = node_health_version{i};
        d.end = node_health_version{i + 1};
        partition_metadata_diff_list parts;
        parts.emplace_back(
          model::partition_id{i}, make_meta(model::term_id{i}));
        d.metadata_diff->emplace_back(tp_ns, std::move(parts));
        store.add(std::move(d));
    }

    // Request 1..5 — should compose 1..2 + 2..3 + 3..4 + 4..5
    auto* result = store.get_diff(node_health_version{1});
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->start, node_health_version{1});
    EXPECT_EQ(result->end, node_health_version{5});

    // All 4 partitions present in composed diff
    auto it = find_topic(*result->metadata_diff, tp_ns);
    ASSERT_NE(it, result->metadata_diff->end());
    for (int i = 1; i <= 4; ++i) {
        EXPECT_NE(
          std::ranges::find_if(
            it->second,
            [&](const auto& p) { return p.first == model::partition_id{i}; }),
          it->second.end());
    }
}

TEST(DiffStore, CompositionAfterPriorGet) {
    // From the design plan example trace. Initial state: [1..3, 2..3, 3..4].
    // Peer C at V2: get_diff(2) composes 2..3 + 3..4 → 2..4.
    //   Store becomes [1..3, 2..4, 3..4].
    // Peer D at V1: get_diff(1) must skip 2..4, find 3..4,
    //   compose 1..3 + 3..4 → 1..4. Store becomes [1..4, 2..4, 3..4].
    diff_store store;

    diff_entry d1;
    d1.start = node_health_version{1};
    d1.end = node_health_version{3};
    store.add(std::move(d1));

    diff_entry d2;
    d2.start = node_health_version{2};
    d2.end = node_health_version{3};
    store.add(std::move(d2));

    diff_entry d3;
    d3.start = node_health_version{3};
    d3.end = node_health_version{4};
    store.add(std::move(d3));

    // Peer C at V2: 2..3 + 3..4 → 2..4
    auto* r1 = store.get_diff(node_health_version{2});
    ASSERT_NE(r1, nullptr);
    EXPECT_EQ(r1->start, node_health_version{2});
    EXPECT_EQ(r1->end, node_health_version{4});

    // Peer D at V1: 1..3 + 3..4 → 1..4 (skips 2..4)
    auto* r2 = store.get_diff(node_health_version{1});
    ASSERT_NE(r2, nullptr);
    EXPECT_EQ(r2->start, node_health_version{1});
    EXPECT_EQ(r2->end, node_health_version{4});
}

TEST(DiffStore, Eviction) {
    diff_store store;
    // Add more than max_diffs (8)
    for (int i = 1; i <= 10; ++i) {
        diff_entry d;
        d.start = node_health_version{i};
        d.end = node_health_version{i + 1};
        store.add(std::move(d));
    }

    // Oldest diffs (1..2, 2..3) should be evicted
    EXPECT_EQ(store.get_diff(node_health_version{1}), nullptr);
    EXPECT_EQ(store.get_diff(node_health_version{2}), nullptr);

    // Recent diffs should still be available
    auto* result = store.get_diff(node_health_version{3});
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->end, node_health_version{11});
}

// --- versioned_health_store ---

TEST(VersionedHealthStore, Bootstrap) {
    versioned_health_store store;
    EXPECT_EQ(store.current(), nullptr);
    EXPECT_FALSE(store.version().has_value());

    store.update_self(
      make_report(
        {{model::partition_id{0}, make_meta()}},
        {{model::partition_id{0}, make_data()}}),
      model::node_boot_id{});

    EXPECT_NE(store.current(), nullptr);
    EXPECT_EQ(store.version(), node_health_version{1});
}

TEST(VersionedHealthStore, VersionIncrementOnSent) {
    versioned_health_store store;
    store.update_self(
      make_report(
        {{model::partition_id{0}, make_meta()}},
        {{model::partition_id{0}, make_data()}}),
      model::node_boot_id{});

    // Send it — marks as sent
    auto result = store.get_for_sending(std::nullopt);
    ASSERT_TRUE(std::holds_alternative<const versioned_report*>(result));

    // Next update should increment version
    store.update_self(
      make_report(
        {{model::partition_id{0}, make_meta(model::term_id{2})}},
        {{model::partition_id{0}, make_data(2000)}}),
      model::node_boot_id{});

    EXPECT_EQ(store.version(), node_health_version{2});
}

TEST(VersionedHealthStore, VersionReuseWhenUnsent) {
    versioned_health_store store;
    store.update_self(
      make_report(
        {{model::partition_id{0}, make_meta()}},
        {{model::partition_id{0}, make_data()}}),
      model::node_boot_id{});

    // Don't send — update again
    store.update_self(
      make_report(
        {{model::partition_id{0}, make_meta(model::term_id{2})}},
        {{model::partition_id{0}, make_data(2000)}}),
      model::node_boot_id{});

    // Version should stay at 1 (reused, not incremented)
    EXPECT_EQ(store.version(), node_health_version{1});
}

TEST(VersionedHealthStore, GetForSendingDiff) {
    versioned_health_store store;
    store.update_self(
      make_report(
        {{model::partition_id{0}, make_meta()}},
        {{model::partition_id{0}, make_data()}}),
      model::node_boot_id{});

    // Send version 1
    store.get_for_sending(std::nullopt);

    // Update to version 2
    store.update_self(
      make_report(
        {{model::partition_id{0}, make_meta(model::term_id{5})}},
        {{model::partition_id{0}, make_data(5000)}}),
      model::node_boot_id{});

    // Send version 2
    store.get_for_sending(std::nullopt);

    // Update to version 3
    store.update_self(
      make_report(
        {{model::partition_id{0}, make_meta(model::term_id{10})}},
        {{model::partition_id{0}, make_data(9000)}}),
      model::node_boot_id{});

    // Peer at version 2 should get a diff
    auto result = store.get_for_sending(node_health_version{2});
    ASSERT_TRUE(std::holds_alternative<const diff_entry*>(result));

    auto* diff = std::get<const diff_entry*>(result);
    EXPECT_EQ(diff->start, node_health_version{2});
    EXPECT_EQ(diff->end, node_health_version{3});
}

TEST(VersionedHealthStore, GetForSendingUpToDate) {
    versioned_health_store store;
    store.update_self(
      make_report(
        {{model::partition_id{0}, make_meta()}},
        {{model::partition_id{0}, make_data()}}),
      model::node_boot_id{});

    // Peer already at current version
    auto result = store.get_for_sending(node_health_version{1});
    EXPECT_TRUE(std::holds_alternative<std::monostate>(result));
}

TEST(VersionedHealthStore, GetForSendingEmpty) {
    versioned_health_store store;
    auto result = store.get_for_sending(std::nullopt);
    EXPECT_TRUE(std::holds_alternative<std::monostate>(result));
}

TEST(VersionedHealthStore, UpdateFromReport) {
    versioned_health_store store;
    auto ok = store.update_from_report(
      versioned_report{
        make_report(
          {{model::partition_id{0}, make_meta()}},
          {{model::partition_id{0}, make_data()}}),
        node_health_version{5}});

    EXPECT_TRUE(ok);
    EXPECT_EQ(store.version(), node_health_version{5});

    // Stale report rejected
    auto stale = store.update_from_report(
      versioned_report{make_report({}, {}), node_health_version{3}});
    EXPECT_FALSE(stale);
    EXPECT_EQ(store.version(), node_health_version{5});
}

TEST(VersionedHealthStore, UpdateFromReportClearsDiffs) {
    versioned_health_store store;
    store.update_self(
      make_report(
        {{model::partition_id{0}, make_meta()}},
        {{model::partition_id{0}, make_data()}}),
      model::node_boot_id{});

    // Send v1, then update to v2 — creates a diff 1..2
    store.get_for_sending(std::nullopt);
    store.update_self(
      make_report(
        {{model::partition_id{0}, make_meta(model::term_id{2})}},
        {{model::partition_id{0}, make_data(2000)}}),
      model::node_boot_id{});

    // Peer at v1 can get diff
    auto result = store.get_for_sending(node_health_version{1});
    ASSERT_TRUE(std::holds_alternative<const diff_entry*>(result));

    // Full report replaces everything — diffs cleared
    store.update_from_report(
      versioned_report{
        make_report(
          {{model::partition_id{0}, make_meta(model::term_id{10})}},
          {{model::partition_id{0}, make_data(9000)}}),
        node_health_version{10}});

    // Old diff no longer available — peer gets full report instead
    auto after = store.get_for_sending(node_health_version{1});
    EXPECT_TRUE(std::holds_alternative<const versioned_report*>(after));
}

TEST(VersionedHealthStore, UpdateFromDiff) {
    versioned_health_store store;
    store.update_from_report(
      versioned_report{
        make_report(
          {{model::partition_id{0}, make_meta(model::term_id{1})}},
          {{model::partition_id{0}, make_data(100)}}),
        node_health_version{5}});

    diff_entry diff;
    diff.start = node_health_version{5};
    diff.end = node_health_version{6};
    partition_metadata_diff_list parts;
    parts.emplace_back(model::partition_id{0}, make_meta(model::term_id{10}));
    diff.metadata_diff->emplace_back(tp_ns, std::move(parts));
    diff.snapshot = make_snapshot({{model::partition_id{0}, make_data(999)}});

    auto ok = store.update_from_diff(std::move(diff));
    EXPECT_TRUE(ok);
    EXPECT_EQ(store.version(), node_health_version{6});
    EXPECT_EQ(
      store.current()->metadata->at(tp_ns).at(model::partition_id{0}).term,
      model::term_id{10});
}

TEST(VersionedHealthStore, UpdateFromDiffMismatch) {
    versioned_health_store store;
    store.update_from_report(
      versioned_report{make_report({}, {}), node_health_version{5}});

    diff_entry diff;
    diff.start = node_health_version{3}; // mismatch
    diff.end = node_health_version{6};

    EXPECT_FALSE(store.update_from_diff(std::move(diff)));
    EXPECT_EQ(store.version(), node_health_version{5}); // unchanged
}

// --- health_pull_reply serde round-trip ---

namespace {

cluster::health_pull_reply make_test_reply() {
    cluster::health_pull_reply reply;
    reply.error = cluster::errc::success;

    auto make_snap = [] {
        health_snapshot s;
        s.data[tp_ns].emplace(
          model::partition_id{0},
          partition_data{
            .size_bytes = 1000, .high_watermark = kafka::offset{42}});
        return s;
    };

    auto make_meta_map = [] {
        topic_partition_metadata_map m;
        m[tp_ns].emplace(
          model::partition_id{0},
          partition_metadata{.term = model::term_id{3}});
        return m;
    };

    // Build internal types, convert to serde via constructors
    diff_entry d;
    d.start = node_health_version{1};
    d.end = node_health_version{2};
    d.snapshot = ss::make_lw_shared<const health_snapshot>(make_snap());
    {
        partition_metadata_diff_list parts;
        parts.emplace_back(
          model::partition_id{0}, make_meta(model::term_id{5}));
        d.metadata_diff->emplace_back(tp_ns, std::move(parts));
    }
    reply.items.emplace_back(model::node_id{1}, diff_entry_serde{d});

    versioned_report vr(
      node_health{
        .snapshot = ss::make_lw_shared<const health_snapshot>(make_snap()),
        .metadata = ss::make_lw_shared<topic_partition_metadata_map>(
          make_meta_map())},
      node_health_version{5});
    reply.items.emplace_back(model::node_id{2}, versioned_report_serde{vr});

    return reply;
}

} // namespace

TEST(HealthPullReply, SerdeRoundTrip) {
    auto expected = make_test_reply();

    iobuf buf;
    serde::write_async(buf, make_test_reply()).get();

    auto parser = iobuf_parser{std::move(buf)};
    auto result = serde::read_async<cluster::health_pull_reply>(parser).get();

    EXPECT_EQ(result, expected);

    // Check src_timestamp survives round-trip within tolerance
    auto check_timestamp = [](
                             const health_snapshot& a,
                             const health_snapshot& b) {
        auto dt = std::abs(
          (a.src_timestamp.value - b.src_timestamp.value).count());
        EXPECT_LT(std::chrono::nanoseconds{dt}, std::chrono::milliseconds{100});
    };
    for (size_t i = 0; i < result.items.size(); ++i) {
        ss::visit(
          result.items[i].second,
          [&](const diff_entry_serde& d) {
              check_timestamp(
                *d.snapshot,
                *std::get<diff_entry_serde>(expected.items[i].second).snapshot);
          },
          [&](const versioned_report_serde& r) {
              check_timestamp(
                *r.snapshot,
                *std::get<versioned_report_serde>(expected.items[i].second)
                   .snapshot);
          });
    }
}

TEST(HealthPullReply, InternalToSerdeRoundTrip) {
    // Build internal types
    auto report_a = make_report(
      {{model::partition_id{0}, make_meta(model::term_id{1})}},
      {{model::partition_id{0}, make_data(100)}});
    auto report_b = make_report(
      {{model::partition_id{0}, make_meta(model::term_id{5})}},
      {{model::partition_id{0}, make_data(999)}});

    diff_entry original_diff(report_a, report_b);
    original_diff.start = node_health_version{1};
    original_diff.end = node_health_version{2};

    versioned_report original_report(
      make_report(
        {{model::partition_id{0}, make_meta(model::term_id{3})}},
        {{model::partition_id{0}, make_data(500)}}),
      node_health_version{7});

    // Pack into reply via serde constructors (sender side)
    cluster::health_pull_reply reply;
    reply.items.emplace_back(
      model::node_id{1}, diff_entry_serde{original_diff});
    reply.items.emplace_back(
      model::node_id{2}, versioned_report_serde{original_report});

    // Serde round-trip
    iobuf buf;
    serde::write_async(buf, std::move(reply)).get();
    auto parser = iobuf_parser{std::move(buf)};
    auto result = serde::read_async<cluster::health_pull_reply>(parser).get();

    ASSERT_EQ(result.items.size(), 2);

    // Extract back to internal types (receiver side)
    auto& [id0, upd0] = result.items[0];
    ASSERT_TRUE(std::holds_alternative<diff_entry_serde>(upd0));
    diff_entry restored_diff(std::move(std::get<diff_entry_serde>(upd0)));

    EXPECT_EQ(restored_diff.start, original_diff.start);
    EXPECT_EQ(restored_diff.end, original_diff.end);
    EXPECT_EQ(*restored_diff.snapshot, *original_diff.snapshot);
    EXPECT_EQ(
      as_map(*restored_diff.metadata_diff),
      as_map(*original_diff.metadata_diff));

    auto& [id1, upd1] = result.items[1];
    ASSERT_TRUE(std::holds_alternative<versioned_report_serde>(upd1));
    versioned_report restored_report(
      std::move(std::get<versioned_report_serde>(upd1)));

    EXPECT_EQ(restored_report.version, original_report.version);
    EXPECT_EQ(
      *restored_report.health.snapshot, *original_report.health.snapshot);
    EXPECT_EQ(
      *restored_report.health.metadata, *original_report.health.metadata);

    // Check src_timestamp within tolerance
    auto check_ts = [](const health_snapshot& a, const health_snapshot& b) {
        auto dt = std::abs(
          (a.src_timestamp.value - b.src_timestamp.value).count());
        EXPECT_LT(std::chrono::nanoseconds{dt}, std::chrono::milliseconds{100});
    };
    check_ts(*restored_diff.snapshot, *original_diff.snapshot);
    check_ts(
      *restored_report.health.snapshot, *original_report.health.snapshot);
}
