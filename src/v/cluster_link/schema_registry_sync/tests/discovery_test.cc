/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/schema_registry_sync/discovery.h"
#include "cluster_link/schema_registry_sync/scope.h"
#include "cluster_link/schema_registry_sync/tests/sr_sync_test_fixtures.h"
#include "container/chunked_hash_map.h"
#include "pandaproxy/schema_registry/types.h"
#include "test_utils/test.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>

#include <gmock/gmock.h>

#include <algorithm>

// Unit tests for the discovery legs against a scripted source: the failure
// classification (per-item error vs. source_unavailable vs. the probe's
// endpoint denial), the active/soft-deleted partition, and the probe's
// cursor-vs-floor walk semantics -- the corner cases that are awkward to
// stage through a running task.
//
// The http_fallback_tail_sync orchestration has no unit tests here on purpose:
// it is the glue between discovery and task-owned state (the retained
// inventory, the live stats, the reconcile), and those seams only exist with
// the task around them. It is covered end to end by the http_tail_* tests in
// mirroring_task_test.cc.
namespace cluster_link::tests {

namespace {

ss::logger test_log("discovery_test");

struct discovery_harness {
    fake_source_state source;
    fake_source_reader reader{&source};
    prefix_logger logger{test_log, "[test]"};
    srs::discovery disc{&logger, "test-link"};
    ss::abort_source as;
};

srs::discovery::in_scope_fn all_in_scope() {
    return [](const ppsr::context_subject&) { return true; };
}

chunked_hash_set<ppsr::context>
contexts_of(std::initializer_list<ppsr::context> cs) {
    chunked_hash_set<ppsr::context> out;
    for (const auto& c : cs) {
        out.insert(c);
    }
    return out;
}

chunked_hash_set<ppsr::context_subject>
subjects_of(std::initializer_list<ppsr::context_subject> ss) {
    chunked_hash_set<ppsr::context_subject> out;
    for (const auto& s : ss) {
        out.insert(s);
    }
    return out;
}

srs::context_mapper identity_mapper() {
    return srs::context_mapper::make(model::schema_registry_sync_config{});
}

bool holds(
  const chunked_vector<ppsr::subject_version>& found,
  const ppsr::subject_version& node) {
    return std::ranges::find(found, node) != found.end();
}

srs::source_error unavailable_error() {
    return srs::source_error{
      .kind = srs::source_error_kind::source_unavailable,
      .message = "source down"};
}

srs::source_error failed_error() {
    return srs::source_error{
      .kind = srs::source_error_kind::operation_failed,
      .message = "listing failed"};
}

} // namespace

TEST(discovery, list_new_subjects_diffs_against_known_and_scope) {
    discovery_harness h;
    auto orders = ppsr::context_subject::unqualified("orders");
    auto payments = ppsr::context_subject::unqualified("payments");
    auto internal = ppsr::context_subject::unqualified("internal");
    h.source.add(orders, 1);
    h.source.add(payments, 1);
    h.source.add(internal, 1);

    auto res = h.disc
                 .list_new_subjects(
                   h.reader,
                   contexts_of({ppsr::default_context}),
                   subjects_of({orders}),
                   [&internal](const ppsr::context_subject& cs) {
                       return cs != internal;
                   },
                   h.as)
                 .get();

    // Known and out-of-scope subjects are filtered; only the new name lands.
    ASSERT_EQ(res.subjects.size(), 1);
    EXPECT_EQ(res.subjects[0], payments);
    EXPECT_TRUE(res.errors.empty());
    EXPECT_FALSE(res.unavailable.has_value());
}

TEST(discovery, list_new_subjects_counts_a_context_failure_and_continues) {
    discovery_harness h;
    auto other = ppsr::context{".other"};
    auto other_x = ppsr::context_subject{other, ppsr::subject{"x"}};
    h.source.add(other_x, 1);
    h.source.list_subjects_errors.emplace(
      ppsr::default_context, failed_error());

    auto res = h.disc
                 .list_new_subjects(
                   h.reader,
                   contexts_of({ppsr::default_context, other}),
                   {},
                   all_in_scope(),
                   h.as)
                 .get();

    // The failed context is one counted error; the other context still lists.
    EXPECT_EQ(res.errors.size(), 1);
    ASSERT_EQ(res.subjects.size(), 1);
    EXPECT_EQ(res.subjects[0], other_x);
    EXPECT_FALSE(res.unavailable.has_value());
}

TEST(discovery, list_new_subjects_stops_on_unavailable) {
    discovery_harness h;
    h.source.add(ppsr::context_subject::unqualified("orders"), 1);
    h.source.list_subjects_error = unavailable_error();

    auto res = h.disc
                 .list_new_subjects(
                   h.reader,
                   contexts_of({ppsr::default_context}),
                   {},
                   all_in_scope(),
                   h.as)
                 .get();

    ASSERT_TRUE(res.unavailable.has_value());
    EXPECT_EQ(
      res.unavailable->kind, srs::source_error_kind::source_unavailable);
    EXPECT_TRUE(res.subjects.empty());
    EXPECT_TRUE(res.errors.empty());
}

TEST(discovery, list_versions_partitions_active_and_deleted) {
    discovery_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    h.source.add(a, 1);
    h.source.add(a, 2, ppsr::is_deleted::yes);

    chunked_vector<ppsr::context_subject> subjects;
    subjects.push_back(a);
    auto res = h.disc.list_versions(h.reader, subjects, 4, h.as).get();

    EXPECT_TRUE(res.discovered.active.contains(key(a, 1)));
    EXPECT_TRUE(res.discovered.deleted.contains(key(a, 2)));
    EXPECT_EQ(res.discovered.active.size(), 1);
    EXPECT_EQ(res.discovered.deleted.size(), 1);
    EXPECT_TRUE(res.errors.empty());
    EXPECT_FALSE(res.unavailable.has_value());
}

TEST(discovery, list_versions_reads_a_fully_soft_deleted_subject_as_deleted) {
    discovery_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    h.source.add(a, 1, ppsr::is_deleted::yes);
    h.source.add(a, 2, ppsr::is_deleted::yes);

    chunked_vector<ppsr::context_subject> subjects;
    subjects.push_back(a);
    auto res = h.disc.list_versions(h.reader, subjects, 4, h.as).get();

    // The active-only listing of a fully soft-deleted subject 404s at the
    // source; that reads as "no active versions", not as a counted error.
    EXPECT_TRUE(res.discovered.active.empty());
    EXPECT_TRUE(res.discovered.deleted.contains(key(a, 1)));
    EXPECT_TRUE(res.discovered.deleted.contains(key(a, 2)));
    EXPECT_TRUE(res.errors.empty());
    EXPECT_FALSE(res.unavailable.has_value());
}

TEST(discovery, list_versions_counts_a_subject_failure_and_continues) {
    discovery_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    h.source.add(a, 1);
    h.source.add(b, 1);
    h.source.list_versions_errors.emplace(a, failed_error());

    chunked_vector<ppsr::context_subject> subjects;
    subjects.push_back(a);
    subjects.push_back(b);
    auto res = h.disc.list_versions(h.reader, subjects, 1, h.as).get();

    EXPECT_EQ(res.errors.size(), 1);
    EXPECT_TRUE(res.discovered.active.contains(key(b, 1)));
    EXPECT_FALSE(res.unavailable.has_value());
}

TEST(discovery, list_versions_unavailable_short_circuits_peers) {
    discovery_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    h.source.add(a, 1);
    h.source.add(b, 1);
    h.source.list_versions_errors.emplace(a, unavailable_error());

    // parallelism = 1 makes the order deterministic: a's fiber records the
    // unavailable, so b's fiber must skip without touching the source.
    chunked_vector<ppsr::context_subject> subjects;
    subjects.push_back(a);
    subjects.push_back(b);
    auto res = h.disc.list_versions(h.reader, subjects, 1, h.as).get();

    ASSERT_TRUE(res.unavailable.has_value());
    EXPECT_TRUE(res.discovered.active.empty());
    EXPECT_TRUE(res.errors.empty());
    EXPECT_EQ(h.source.list_versions_calls, 1);
}

TEST(discovery, probe_finds_new_versions_above_the_floor) {
    discovery_harness h;
    auto orders = ppsr::context_subject::unqualified("orders");
    h.source.add_with_id(orders, 1, 1);
    h.source.add_with_id(orders, 2, 2);
    h.source.add_with_id(orders, 3, 3);
    chunked_hash_map<ppsr::context, ppsr::schema_id> floor;
    floor.emplace(ppsr::default_context, ppsr::schema_id{1});

    auto mapper = identity_mapper();
    auto res = h.disc
                 .probe_new_ids(
                   h.reader,
                   contexts_of({ppsr::default_context}),
                   mapper,
                   floor,
                   all_in_scope(),
                   h.as)
                 .get();

    EXPECT_EQ(res.found.size(), 2);
    EXPECT_TRUE(holds(res.found, key(orders, 2)));
    EXPECT_TRUE(holds(res.found, key(orders, 3)));
    EXPECT_TRUE(res.errors.empty());
    EXPECT_FALSE(res.unavailable.has_value());
    // The floor is one past the highest held id, so id 1 is never re-probed;
    // the walk ends on the first miss.
    EXPECT_EQ(h.source.probes(1), 0);
    EXPECT_EQ(h.source.probes(4), 1);
    EXPECT_EQ(h.source.probes(5), 0);
}

TEST(discovery, probe_starts_at_the_first_id_with_no_floor) {
    discovery_harness h;
    auto orders = ppsr::context_subject::unqualified("orders");
    h.source.add_with_id(orders, 1, 1);

    auto mapper = identity_mapper();
    auto res = h.disc
                 .probe_new_ids(
                   h.reader,
                   contexts_of({ppsr::default_context}),
                   mapper,
                   {},
                   all_in_scope(),
                   h.as)
                 .get();

    // No floor entry clamps the start to the lowest id a registry allocates
    // (1), not to the type's default (0).
    EXPECT_EQ(h.source.probes(1), 1);
    ASSERT_EQ(res.found.size(), 1);
    EXPECT_TRUE(holds(res.found, key(orders, 1)));
}

TEST(discovery, probe_cursor_advances_on_ids_that_yield_nothing) {
    discovery_harness h;
    auto orders = ppsr::context_subject::unqualified("orders");
    auto payments = ppsr::context_subject::unqualified("payments");
    h.source.add_with_id(orders, 1, 1);
    h.source.add_with_id(payments, 1, 2);
    chunked_hash_map<ppsr::context, ppsr::schema_id> floor;
    floor.emplace(ppsr::default_context, ppsr::schema_id{1});
    auto in_scope = [&payments](const ppsr::context_subject& cs) {
        return cs != payments;
    };

    auto mapper = identity_mapper();
    auto first = h.disc
                   .probe_new_ids(
                     h.reader,
                     contexts_of({ppsr::default_context}),
                     mapper,
                     floor,
                     in_scope,
                     h.as)
                   .get();
    EXPECT_TRUE(first.found.empty());
    EXPECT_EQ(h.source.probes(2), 1);

    auto second = h.disc
                    .probe_new_ids(
                      h.reader,
                      contexts_of({ppsr::default_context}),
                      mapper,
                      floor,
                      in_scope,
                      h.as)
                    .get();

    // Id 2 exists but imported nothing, so the floor (still 1) never moves
    // past it; only the cursor, which advances on the id existing, keeps the
    // second call from re-walking it.
    EXPECT_TRUE(second.found.empty());
    EXPECT_EQ(h.source.probes(2), 1);
    EXPECT_EQ(h.source.probes(3), 2);
}

TEST(discovery, probe_continues_past_a_fully_soft_deleted_id) {
    discovery_harness h;
    auto orders = ppsr::context_subject::unqualified("orders");
    h.source.add_with_id(orders, 1, 1, ppsr::is_deleted::yes);
    h.source.add_with_id(orders, 2, 2);

    auto mapper = identity_mapper();
    auto res = h.disc
                 .probe_new_ids(
                   h.reader,
                   contexts_of({ppsr::default_context}),
                   mapper,
                   {},
                   all_in_scope(),
                   h.as)
                 .get();

    // Id 1 resolves with an empty pair list (every version behind it is
    // soft-deleted); that is a hit that continues the walk, not a miss that
    // ends it.
    ASSERT_EQ(res.found.size(), 1);
    EXPECT_TRUE(holds(res.found, key(orders, 2)));
    EXPECT_EQ(h.source.probes(3), 1);
}

TEST(discovery, probe_skips_global_and_unmapped_contexts) {
    discovery_harness h;
    h.source.add_with_id(ppsr::context_subject::unqualified("orders"), 1, 1);
    // An exact mapping with an empty table maps no source context.
    model::schema_registry_sync_config config;
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    api.destination
      = model::schema_registry_sync_config::exact_context_mapping{};
    config.sync_mode = std::move(api);
    auto mapper = srs::context_mapper::make(config);

    auto res = h.disc
                 .probe_new_ids(
                   h.reader,
                   contexts_of({ppsr::global_context, ppsr::default_context}),
                   mapper,
                   {},
                   all_in_scope(),
                   h.as)
                 .get();

    // Neither context is probed: the global is not an id namespace, and an
    // unmapped context is skipped silently rather than fed to the reconciler
    // (check_preconditions faults the link before this can matter).
    EXPECT_TRUE(res.found.empty());
    EXPECT_TRUE(res.errors.empty());
    EXPECT_FALSE(res.unavailable.has_value());
    EXPECT_EQ(h.source.probes(1), 0);
}

TEST(discovery, probe_denial_degrades_and_keeps_earlier_finds) {
    discovery_harness h;
    auto orders = ppsr::context_subject::unqualified("orders");
    h.source.add_with_id(orders, 1, 1);
    h.source.schema_id_errors.emplace(
      ppsr::schema_id{2},
      srs::source_error{
        .kind = srs::source_error_kind::endpoint_unsupported,
        .message = "endpoint denied"});

    auto mapper = identity_mapper();
    auto res = h.disc
                 .probe_new_ids(
                   h.reader,
                   contexts_of({ppsr::default_context}),
                   mapper,
                   {},
                   all_in_scope(),
                   h.as)
                 .get();

    // The denial ends the leg for this call only: nothing is counted, nothing
    // parks, and what the walk found before the denial is kept.
    ASSERT_EQ(res.found.size(), 1);
    EXPECT_TRUE(holds(res.found, key(orders, 1)));
    EXPECT_TRUE(res.errors.empty());
    EXPECT_FALSE(res.unavailable.has_value());
}

TEST(discovery, probe_reports_unavailable) {
    discovery_harness h;
    h.source.schema_id_errors.emplace(ppsr::schema_id{1}, unavailable_error());

    auto mapper = identity_mapper();
    auto res = h.disc
                 .probe_new_ids(
                   h.reader,
                   contexts_of({ppsr::default_context}),
                   mapper,
                   {},
                   all_in_scope(),
                   h.as)
                 .get();

    ASSERT_TRUE(res.unavailable.has_value());
    EXPECT_EQ(
      res.unavailable->kind, srs::source_error_kind::source_unavailable);
    EXPECT_TRUE(res.found.empty());
    EXPECT_TRUE(res.errors.empty());
}

TEST(discovery, probe_counts_a_real_failure_and_ends_that_walk) {
    discovery_harness h;
    auto orders = ppsr::context_subject::unqualified("orders");
    h.source.add_with_id(orders, 1, 1);
    h.source.schema_id_errors.emplace(ppsr::schema_id{2}, failed_error());

    auto mapper = identity_mapper();
    auto res = h.disc
                 .probe_new_ids(
                   h.reader,
                   contexts_of({ppsr::default_context}),
                   mapper,
                   {},
                   all_in_scope(),
                   h.as)
                 .get();

    // A reachable-but-failed probe is one counted error and the end of this
    // context's walk -- ids past it stay unprobed until the next call.
    ASSERT_EQ(res.found.size(), 1);
    EXPECT_TRUE(holds(res.found, key(orders, 1)));
    EXPECT_EQ(res.errors.size(), 1);
    EXPECT_FALSE(res.unavailable.has_value());
    EXPECT_EQ(h.source.probes(3), 0);
}

} // namespace cluster_link::tests
