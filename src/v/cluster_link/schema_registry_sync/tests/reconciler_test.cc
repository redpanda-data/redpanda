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

#include "cluster_link/schema_registry_sync/reconciler.h"
#include "cluster_link/schema_registry_sync/source_reader.h"
#include "cluster_link/schema_registry_sync/tests/sr_sync_test_fixtures.h"
#include "container/chunked_hash_map.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/tests/fake_registry.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/when_all.hh>

#include <gmock/gmock.h>

#include <limits>
#include <string>
#include <tuple>
#include <vector>

namespace cluster_link::tests {

namespace {
constexpr static auto no_memory_limit = std::numeric_limits<ssize_t>::max();
}

TEST(reconciler, replicates_chain_in_reference_order) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    h.source.add(a, 1);
    h.source.add_with_refs(b, 1, refs_to({ref_to(a, 1)}));

    // Upserts in referrer-first order: the engine must reorder.
    srs::work_set work;
    work.upserts.push_back(key(b, 1));
    work.upserts.push_back(key(a, 1));

    auto stats = h.run(std::move(work)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 2);
    EXPECT_EQ(stats->errors, 0);

    const auto& all = h.destination.get_all();
    auto ia = index_of(all, "a");
    auto ib = index_of(all, "b");
    ASSERT_GE(ia, 0);
    ASSERT_GE(ib, 0);
    EXPECT_LT(ia, ib);
}

// Under the REMOVE policy a schema carrying unsupported features is imported
// (only the supported projection is stored) and the removals are counted.
TEST(reconciler, remove_policy_imports_and_counts_unsupported) {
    reconcile_harness h;
    h.feature_policy
      = model::schema_registry_sync_config::unsupported_feature_policy::remove;
    auto a = ppsr::context_subject::unqualified("a");
    h.source.add(a, 1);
    chunked_vector<ppsr::unsupported_feature> unsupported{
      {.json_pointer = "/ruleSet"}, {.json_pointer = "/metadata/tags"}};
    h.source.set_unsupported(a, 1, std::move(unsupported));

    srs::work_set work;
    work.upserts.push_back(key(a, 1));

    auto stats = h.run(std::move(work)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 1);
    EXPECT_EQ(stats->unsupported_features_removed, 2);
    EXPECT_EQ(stats->errors, 0);
    EXPECT_GE(index_of(h.destination.get_all(), "a"), 0);
}

// Under REMOVE the removed-features counter must reflect only projections that
// actually landed: a schema carrying unsupported features whose import is
// rejected counts a single error and leaves unsupported_features_removed at 0.
TEST(reconciler, remove_policy_does_not_count_on_import_failure) {
    fake_source_state source;
    schema::fake_registry inner;
    failing_import_registry destination{&inner};

    auto a = ppsr::context_subject::unqualified("a");
    source.add(a, 1);
    source.set_unsupported(a, 1, {{.json_pointer = "/ruleSet"}});
    destination.fail_import(
      key(a, 1), ppsr::error_code::schema_invalid, "unparseable body");

    srs::work_set work;
    work.upserts.push_back(key(a, 1));

    fake_source_reader reader{&source};
    srs::context_mapper mapper;
    srs::reconciler::limits lim{
      .memory_bytes = no_memory_limit, .parallelism = 1};
    auto r = srs::reconciler{
      &reader,
      &destination,
      [](const ppsr::context_subject&) { return true; },
      mapper,
      lim,
      model::schema_registry_sync_config::unsupported_feature_policy::remove};

    ss::abort_source as;
    srs::reconcile_stats stats;
    auto res = r.reconcile(std::move(work), {}, stats, as).get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(stats.errors, 1);
    EXPECT_EQ(stats.versions_changed, 0);
    EXPECT_EQ(stats.unsupported_features_removed, 0);
    EXPECT_EQ(index_of(inner.get_all(), "a"), -1);
}

// Under the FAIL policy an unsupported feature is a per-item failure, not a
// whole-sync abort: the offending node is counted as an error and skipped
// (never imported), while the rest of the work still replicates.
TEST(reconciler, fail_policy_counts_and_skips_unsupported) {
    reconcile_harness h;
    h.feature_policy
      = model::schema_registry_sync_config::unsupported_feature_policy::fail;
    auto a = ppsr::context_subject::unqualified("a"); // unsupported -> skipped
    auto b = ppsr::context_subject::unqualified("b"); // clean -> imported
    h.source.add(a, 1);
    h.source.add(b, 1);
    h.source.set_unsupported(a, 1, {{.json_pointer = "/ruleSet"}});

    srs::work_set work;
    work.upserts.push_back(key(a, 1));
    work.upserts.push_back(key(b, 1));

    // The run completes (no throw); the unsupported node is counted and
    // skipped.
    auto stats = h.run(std::move(work)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->errors, 1);
    EXPECT_EQ(stats->versions_changed, 1);
    EXPECT_EQ(stats->unsupported_features_removed, 0);
    EXPECT_EQ(index_of(h.destination.get_all(), "a"), -1);
    EXPECT_GE(index_of(h.destination.get_all(), "b"), 0);
}

// Under FAIL a rejected schema is skipped before its references are queued, so
// a reference pulled in solely for the rejected schema is not dragged into the
// destination. Here b references a and only b is selected for replication; b
// turns out to carry unsupported features (discovered on fetch) and is
// rejected, so a -- reachable only as b's reference -- is never imported.
TEST(reconciler, fail_policy_skips_references_of_rejected_schema) {
    reconcile_harness h;
    h.feature_policy
      = model::schema_registry_sync_config::unsupported_feature_policy::fail;
    auto a = ppsr::context_subject::unqualified("a"); // ref of b, pulled for b
    auto b = ppsr::context_subject::unqualified("b"); // unsupported -> rejected
    h.source.add(a, 1);
    h.source.add_with_refs(b, 1, refs_to({ref_to(a, 1)}));
    h.source.set_unsupported(b, 1, {{.json_pointer = "/ruleSet"}});

    // Select only b for replication; a would be discovered solely as b's ref.
    srs::work_set work;
    work.upserts.push_back(key(b, 1));

    auto stats = h.run(std::move(work)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->errors, 1);
    EXPECT_EQ(stats->versions_changed, 0);
    EXPECT_EQ(stats->unsupported_features_removed, 0);
    EXPECT_EQ(index_of(h.destination.get_all(), "b"), -1);
    EXPECT_EQ(index_of(h.destination.get_all(), "a"), -1);
    // a is never even fetched: rejecting b before queuing its references means
    // a is never discovered/enqueued, not merely dropped before import.
    EXPECT_EQ(h.source.reads(a, 1), 0);
}

// Tolerance test: a schema that lists the same reference twice is handled and
// the referrer still imports referent-first. Holds with or without the dedup.
TEST(reconciler, duplicate_reference_still_imports) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    h.source.add(a, 1);
    h.source.add_with_refs(b, 1, refs_to({ref_to(a, 1), ref_to(a, 1)}));

    srs::work_set work;
    work.upserts.push_back(key(b, 1));
    work.upserts.push_back(key(a, 1));

    auto stats = h.run(std::move(work)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 2);
    EXPECT_EQ(stats->errors, 0);
    EXPECT_LT(
      index_of(h.destination.get_all(), "a"),
      index_of(h.destination.get_all(), "b"));
}

TEST(reconciler, diamond_fetches_referent_once) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    auto c = ppsr::context_subject::unqualified("c");
    auto d = ppsr::context_subject::unqualified("d");
    h.source.add(a, 1);
    h.source.add_with_refs(b, 1, refs_to({ref_to(a, 1)}));
    h.source.add_with_refs(c, 1, refs_to({ref_to(a, 1)}));
    h.source.add_with_refs(d, 1, refs_to({ref_to(b, 1), ref_to(c, 1)}));

    srs::work_set work;
    work.upserts.push_back(key(d, 1));
    work.upserts.push_back(key(c, 1));
    work.upserts.push_back(key(b, 1));
    work.upserts.push_back(key(a, 1));

    auto stats = h.run(std::move(work)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 4);
    EXPECT_EQ(stats->errors, 0);
    // The shared referent a is fetched exactly once despite two referrers.
    EXPECT_EQ(h.source.reads(a, 1), 1);

    const auto& all = h.destination.get_all();
    EXPECT_LT(index_of(all, "a"), index_of(all, "b"));
    EXPECT_LT(index_of(all, "a"), index_of(all, "c"));
    EXPECT_LT(index_of(all, "b"), index_of(all, "d"));
    EXPECT_LT(index_of(all, "c"), index_of(all, "d"));
}

TEST(reconciler, out_of_scope_reference_errors_referrer) {
    reconcile_harness h;
    auto a = ppsr::context_subject{ppsr::context{".other"}, ppsr::subject{"a"}};
    auto b = ppsr::context_subject::unqualified("b");
    h.source.add(a, 1);
    h.source.add_with_refs(b, 1, refs_to({ref_to(a, 1)}));

    srs::work_set work;
    work.upserts.push_back(key(b, 1));

    ss::abort_source as;
    // Only the default context is in scope; the ".other" referent is not.
    auto r = h.make([](const ppsr::context_subject& s) {
        return s.ctx == ppsr::default_context;
    });
    srs::reconcile_stats stats;
    auto res = r.reconcile(std::move(work), {}, stats, as).get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(stats.versions_changed, 0);
    EXPECT_EQ(stats.errors, 1);
    EXPECT_EQ(index_of(h.destination.get_all(), "b"), -1);
}

TEST(reconciler, dangling_reference_errors_referrer) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    // b references a:v9, which does not exist on the source.
    h.source.add_with_refs(b, 1, refs_to({ref_to(a, 9)}));

    srs::work_set work;
    work.upserts.push_back(key(b, 1));

    auto stats = h.run(std::move(work)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 0);
    // Both the dangling referent (fetch fails) and the referrer error.
    EXPECT_GE(stats->errors, 1);
    EXPECT_EQ(index_of(h.destination.get_all(), "b"), -1);
}

TEST(reconciler, single_fetch_when_refs_already_replicated) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    h.source.add(a, 1);
    h.source.add_with_refs(b, 1, refs_to({ref_to(a, 1)}));

    // a is already replicated on the destination, so b imports on first fetch.
    chunked_hash_set<ppsr::subject_version> seed;
    seed.insert(key(a, 1));

    srs::work_set work;
    work.upserts.push_back(key(b, 1));

    auto stats = h.run(std::move(work), std::move(seed)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 1);
    EXPECT_EQ(stats->errors, 0);
    EXPECT_EQ(h.source.reads(b, 1), 1);
}

TEST(reconciler, double_fetch_when_discovered_before_deps) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    h.source.add(a, 1);
    h.source.add_with_refs(b, 1, refs_to({ref_to(a, 1)}));

    // Nothing pre-replicated. The engine drains discovery LIFO, so pushing a
    // then b discovers b first: it finds a missing, releases its body, and is
    // re-fetched once a completes.
    srs::work_set work;
    work.upserts.push_back(key(a, 1));
    work.upserts.push_back(key(b, 1));

    auto stats = h.run(std::move(work)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 2);
    EXPECT_EQ(h.source.reads(b, 1), 2);
    EXPECT_EQ(h.source.reads(a, 1), 1);
}

TEST(reconciler, idempotent_resync) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    h.source.add(a, 1);
    h.source.add_with_refs(b, 1, refs_to({ref_to(a, 1)}));

    // Both already on the destination. With an empty work_set there is nothing
    // to import and nothing to fetch.
    chunked_hash_set<ppsr::subject_version> seed;
    seed.insert(key(a, 1));
    seed.insert(key(b, 1));

    auto stats = h.run(srs::work_set{}, std::move(seed)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 0);
    EXPECT_EQ(stats->errors, 0);
    EXPECT_EQ(h.source.reads(a, 1), 0);
    EXPECT_EQ(h.source.reads(b, 1), 0);
}

// Invariant guard: the `seed_replicated` set satisfies references; it does NOT
// mark upsert targets as complete. A node that is active on the source but
// soft-deleted on the destination appears in both `upserts` (S.active \
// D.active) and the `all`-based seed (soft-deleted nodes are in `all`). The
// engine must still fetch and import it (reactivation). An "optimization" that
// skipped upserts already in `_replicated` would silently drop this
// reactivation and report success despite source/destination divergence.
TEST(reconciler, seed_replicated_upsert_is_still_imported) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    h.source.add(a, 1);

    // a:v1 is in the work-set AND in the reference-satisfaction seed (as a
    // destination soft-deleted node would be).
    chunked_hash_set<ppsr::subject_version> seed;
    seed.insert(key(a, 1));

    srs::work_set work;
    work.upserts.push_back(key(a, 1));

    auto stats = h.run(std::move(work), std::move(seed)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 1);
    EXPECT_EQ(stats->errors, 0);
    // Still fetched (not skipped because it was in the seed).
    EXPECT_GE(h.source.reads(a, 1), 1);
}

// Soft-delete propagation onto an active destination version via the
// listing-derived fallback: with a source that omits the `deleted` flag from
// the body, re-importing the version with the caller's declared deleted-state
// transitions an existing active version to soft-deleted. The node is in the
// seed (a destination version is in `all`), so this also guards that a seeded
// upsert is still imported. Underpins the task propagating a source soft-delete
// onto a live destination version.
TEST(reconciler, propagates_soft_delete_over_active_destination) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    // Destination already holds a:v1 active; the source has soft-deleted it but
    // does not echo the flag, so only the listing-derived set conveys it.
    h.destination.import_schema(make_schema(a, 1, R"({"v":1})")).get();
    h.source.reports_deleted_flag = false;
    h.source.add(a, 1, ppsr::is_deleted::yes);

    chunked_hash_set<ppsr::subject_version> seed;
    seed.insert(key(a, 1));
    srs::work_set work;
    work.upserts.push_back(key(a, 1));
    work.soft_deleted.insert(key(a, 1));

    auto stats = h.run(std::move(work), std::move(seed)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 1);
    EXPECT_EQ(stats->errors, 0);

    const auto& all = h.destination.get_all();
    auto ia = index_of(all, "a");
    ASSERT_GE(ia, 0);
    EXPECT_EQ(all[ia].deleted, ppsr::is_deleted::yes);
}

// When the source omits the `deleted` flag from the per-version body, the
// caller's listing-derived soft-delete set is the fallback:
// read_subject_version reports deleted == nullopt, so declaring the node in
// work_set.soft_deleted still lands it soft-deleted on the destination.
// Guards the fallback path for a source that does not echo the flag.
TEST(reconciler, listing_soft_delete_used_when_source_omits_flag) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    // Source does not echo a deleted flag, so the body cannot convey the
    // soft-delete; only the listing-derived set can.
    h.source.reports_deleted_flag = false;
    h.source.add(a, 1, ppsr::is_deleted::no);

    srs::work_set work;
    work.upserts.push_back(key(a, 1));
    work.soft_deleted.insert(key(a, 1));

    auto stats = h.run(std::move(work)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 1);
    EXPECT_EQ(stats->errors, 0);

    const auto& all = h.destination.get_all();
    auto ia = index_of(all, "a");
    ASSERT_GE(ia, 0);
    EXPECT_EQ(all[ia].deleted, ppsr::is_deleted::yes);
}

// The source's reported deleted flag wins over the listing-derived set when the
// two disagree: a version the source reports soft-deleted lands deleted even
// though it is absent from work_set.soft_deleted. The body is authoritative
// when present.
TEST(reconciler, reported_deleted_wins_over_listing_absent) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    // Source reports the version soft-deleted; the listing set does NOT list
    // it.
    h.source.add(a, 1, ppsr::is_deleted::yes);

    srs::work_set work;
    work.upserts.push_back(key(a, 1));
    // work.soft_deleted intentionally left empty.

    EXPECT_THAT(
      h.run(std::move(work)).get(),
      testing::Optional(
        testing::AllOf(
          testing::Field(
            "versions_changed", &srs::reconcile_stats::versions_changed, 1),
          testing::Field("errors", &srs::reconcile_stats::errors, 0))));

    const auto& all = h.destination.get_all();
    auto ia = index_of(all, "a");
    ASSERT_GE(ia, 0);
    EXPECT_EQ(all[ia].deleted, ppsr::is_deleted::yes);
}

// The mirror case, and the point of preferring the body: a fresh body reporting
// the version active wins over a stale listing set that still lists it as
// soft-deleted (the listing was read earlier and may have raced). The version
// lands active despite being in work_set.soft_deleted.
TEST(reconciler, reported_active_wins_over_stale_listing_soft_delete) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    // Source reports the version active; the stale listing set still lists it.
    h.source.add(a, 1, ppsr::is_deleted::no);

    srs::work_set work;
    work.upserts.push_back(key(a, 1));
    work.soft_deleted.insert(key(a, 1));

    EXPECT_THAT(
      h.run(std::move(work)).get(),
      testing::Optional(
        testing::AllOf(
          testing::Field(
            "versions_changed", &srs::reconcile_stats::versions_changed, 1),
          testing::Field("errors", &srs::reconcile_stats::errors, 0))));

    const auto& all = h.destination.get_all();
    auto ia = index_of(all, "a");
    ASSERT_GE(ia, 0);
    EXPECT_EQ(all[ia].deleted, ppsr::is_deleted::no);
}

TEST(reconciler, cyclic_source_does_not_hang) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    // a refs b, b refs a: a malformed (cyclic) source.
    h.source.add_with_refs(a, 1, refs_to({ref_to(b, 1)}));
    h.source.add_with_refs(b, 1, refs_to({ref_to(a, 1)}));

    srs::work_set work;
    work.upserts.push_back(key(a, 1));
    work.upserts.push_back(key(b, 1));

    auto stats = h.run(std::move(work)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 0);
    EXPECT_EQ(stats->errors, 2);
    EXPECT_TRUE(h.destination.get_all().empty());
}

TEST(reconciler, import_conflict_counts_as_error) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    h.source.add(a, 1);
    h.source.add(b, 1);

    // Pre-seed the destination with b:v1 at an id that collides with no source
    // schema, so importing the source's b:v1 throws
    // subject_version_schema_id_already_exists (not an unrelated id clash).
    {
        auto conflicting = make_schema(b, 1, R"({"conflict":true})");
        conflicting.id = ppsr::schema_id{99};
        h.destination.import_schema(std::move(conflicting)).get();
    }

    srs::work_set work;
    work.upserts.push_back(key(a, 1));
    work.upserts.push_back(key(b, 1));

    auto stats = h.run(std::move(work)).get();
    ASSERT_TRUE(stats.has_value());
    // a imports; b conflicts and is counted as an error, no fault.
    EXPECT_EQ(stats->versions_changed, 1);
    EXPECT_EQ(stats->errors, 1);
    EXPECT_GE(index_of(h.destination.get_all(), "a"), 0);
}

// Any import failure other than shutdown is a per-item error. The destination
// can reject a schema the source happily served -- version/impl skew can make a
// body unparseable, or a reference can fail to resolve at import. The
// reconciler must count it, fail the node, cascade to dependents that can no
// longer resolve, and keep going: one bad schema must never abort replication
// of the rest. The in-memory fake never parses or validates references, so the
// rejection is injected per node.
TEST(reconciler, import_errors_are_per_item_and_cascade) {
    fake_source_state source;
    schema::fake_registry inner;
    failing_import_registry destination{&inner};

    auto a = ppsr::context_subject::unqualified("a"); // import rejected
    auto b = ppsr::context_subject::unqualified("b"); // refs a -> cascades
    auto x = ppsr::context_subject::unqualified("x"); // import rejected
    auto y = ppsr::context_subject::unqualified("y"); // refs x -> cascades
    auto z = ppsr::context_subject::unqualified("z"); // independent -> imports

    source.add(a, 1);
    source.add_with_refs(b, 1, refs_to({ref_to(a, 1)}));
    source.add(x, 1);
    source.add_with_refs(y, 1, refs_to({ref_to(x, 1)}));
    source.add(z, 1);

    // The destination rejects a (unparseable body) and x (reference unresolved
    // at import). Both are intrinsic to one schema, not systemic failures.
    destination.fail_import(
      key(a, 1), ppsr::error_code::schema_invalid, "unparseable body");
    destination.fail_import(
      key(x, 1),
      ppsr::error_code::schema_missing_reference,
      "reference not found");

    srs::work_set work;
    for (const auto& s : {a, b, x, y, z}) {
        work.upserts.push_back(key(s, 1));
    }

    fake_source_reader reader{&source};
    srs::reconciler::limits lim{
      .memory_bytes = no_memory_limit, .parallelism = 1};
    srs::context_mapper mapper;
    auto r = srs::reconciler{
      &reader,
      &destination,
      [](const ppsr::context_subject&) { return true; },
      mapper,
      lim,
      model::schema_registry_sync_config::unsupported_feature_policy::fail};

    ss::abort_source as;
    srs::reconcile_stats stats;
    auto res = r.reconcile(std::move(work), {}, stats, as).get();

    // The run completes (no fault) despite two rejected schemas.
    ASSERT_TRUE(res.has_value());
    // a and x are rejected at import; b and y cascade-fail (their referent
    // never landed). Only the independent z imports.
    EXPECT_EQ(stats.errors, 4);
    EXPECT_EQ(stats.versions_changed, 1);
    EXPECT_GE(index_of(inner.get_all(), "z"), 0);
    EXPECT_EQ(index_of(inner.get_all(), "a"), -1);
    EXPECT_EQ(index_of(inner.get_all(), "b"), -1);
    EXPECT_EQ(index_of(inner.get_all(), "x"), -1);
    EXPECT_EQ(index_of(inner.get_all(), "y"), -1);
}

// Deadlock regression: a deep chain e<-d<-c<-b<-a, a byte budget of a single
// body, and four workers. The byte-semaphore can never deadlock because a
// worker holds units only around the one node it imports (which waits on
// nothing) and releases them when it defers a node with missing refs. The call
// must RETURN (not hang) and import all five referent-first.
TEST(reconciler, tiny_memory_budget_deep_chain_completes) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    auto c = ppsr::context_subject::unqualified("c");
    auto d = ppsr::context_subject::unqualified("d");
    auto e = ppsr::context_subject::unqualified("e");
    h.source.add(a, 1);
    auto chain =
      [&](const ppsr::context_subject& sub, const ppsr::context_subject& dep) {
          h.source.add_with_refs(sub, 1, refs_to({ref_to(dep, 1)}));
      };
    chain(b, a);
    chain(c, b);
    chain(d, c);
    chain(e, d);

    // One body fits at a time (acquire clamps to the budget), so only the
    // deadlock-freedom invariant lets four workers make progress.
    h.lim = srs::reconciler::limits{.memory_bytes = 1, .parallelism = 4};

    srs::work_set work;
    work.upserts.push_back(key(e, 1));
    work.upserts.push_back(key(d, 1));
    work.upserts.push_back(key(c, 1));
    work.upserts.push_back(key(b, 1));
    work.upserts.push_back(key(a, 1));

    auto stats = h.run(std::move(work)).get();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->versions_changed, 5);
    EXPECT_EQ(stats->errors, 0);

    const auto& all = h.destination.get_all();
    EXPECT_LT(index_of(all, "a"), index_of(all, "b"));
    EXPECT_LT(index_of(all, "b"), index_of(all, "c"));
    EXPECT_LT(index_of(all, "c"), index_of(all, "d"));
    EXPECT_LT(index_of(all, "d"), index_of(all, "e"));
}

// Abort mid-sync: the source read completes (so `discover` acquires byte-budget
// units), then the destination import blocks until the test aborts. `reconcile`
// must return promptly, the gate must close (no hang), and the units held
// across the blocked import must be released (no leak) after the call returns.
// Blocking in import rather than in the read is deliberate: it is the only way
// the abort interrupts a worker that is holding units, so the assertion
// exercises the release-on-abort path instead of being trivially satisfied.
TEST(reconciler, abort_mid_sync_drains_cleanly) {
    fake_source_state source;
    schema::fake_registry inner;
    source.add(ppsr::context_subject::unqualified("a"), 1);

    fake_source_reader reader{&source};
    blocking_import_registry destination{&inner};

    srs::reconciler::limits lim{
      .memory_bytes = no_memory_limit, .parallelism = 4};
    srs::context_mapper mapper;
    auto r = srs::reconciler{
      &reader,
      &destination,
      [](const ppsr::context_subject&) { return true; },
      mapper,
      lim,
      model::schema_registry_sync_config::unsupported_feature_policy::fail};

    srs::work_set work;
    work.upserts.push_back(key(ppsr::context_subject::unqualified("a"), 1));

    ss::abort_source as;
    srs::reconcile_stats stats;
    auto fut = r.reconcile(std::move(work), {}, stats, as);

    // A worker has read the body and acquired byte-budget units; it is now
    // parked inside the import. Abort the sync while those units are held.
    destination.entered().get();
    as.request_abort();
    // The in-flight import must be released manually: schema::registry's
    // import_schema takes no abort_source, so requesting the reconcile's abort
    // does not cancel an import already running inside the destination. The
    // worker stays parked until the wrapper's own promise resolves, so the test
    // resolves it as an abort to drive the worker through the unwind path. (In
    // production the reconciler's gate.close() likewise waits for in-flight
    // imports to finish rather than cancelling them.)
    destination.abort();

    // Returns promptly via the abort path (not a hang); the abort propagates as
    // an exception, surfaced by reconcile's `as.check()`.
    EXPECT_THROW(fut.get(), ss::abort_requested_exception);
    // Leak-freedom on the abort path is asserted by the dassert in reconcile().
}

// Live-progress contract: the reconciler increments the caller's stats as each
// node completes, not only at the end. Two independent nodes, a single worker:
// the worker imports the first, then parks inside the second's import. At that
// point the live `versions_changed` must already reflect the completed first
// node (== 1), proving progress is observable mid-sync.
TEST(reconciler, reports_progress_live) {
    fake_source_state source;
    schema::fake_registry inner;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    source.add(a, 1);
    source.add(b, 1);

    fake_source_reader reader{&source};
    // Let the first import through; park inside the second.
    blocking_import_registry destination{&inner, /*block_after=*/1};

    // Single worker so imports are strictly serialised: one completes before
    // the next begins.
    srs::reconciler::limits lim{
      .memory_bytes = no_memory_limit, .parallelism = 1};
    srs::context_mapper mapper;
    auto r = srs::reconciler{
      &reader,
      &destination,
      [](const ppsr::context_subject&) { return true; },
      mapper,
      lim,
      model::schema_registry_sync_config::unsupported_feature_policy::fail};

    srs::work_set work;
    work.upserts.push_back(key(a, 1));
    work.upserts.push_back(key(b, 1));

    ss::abort_source as;
    srs::reconcile_stats stats;
    auto fut = r.reconcile(std::move(work), {}, stats, as);

    // The worker has imported the first node and is now parked inside the
    // second. The live counter already reflects the completed first node.
    destination.entered().get();
    EXPECT_EQ(stats.versions_changed, 1);

    // Let the parked import complete; both nodes import.
    destination.unblock();
    auto res = fut.get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(stats.versions_changed, 2);
    EXPECT_EQ(stats.errors, 0);
}

// A read returning `source_unavailable` mid-run is a fault: it stops the worker
// pool and surfaces as the unavailable result (not a per-item error). Here a
// referent b:1 is reachable but its read is forced to fail with
// source_unavailable; reconcile must resolve to that error rather than counting
// it among `stats.errors`. (The integration suite covers the list-path fault;
// this covers the read path at the reconciler level.)
TEST(reconciler, read_source_unavailable_faults_run) {
    reconcile_harness h;
    auto a = ppsr::context_subject::unqualified("a");
    auto b = ppsr::context_subject::unqualified("b");
    h.source.add(a, 1);
    h.source.add(b, 1);
    h.source.fail_read(
      b,
      1,
      srs::source_error{
        .kind = srs::source_error_kind::source_unavailable,
        .message = "source down mid-run"});

    srs::work_set work;
    work.upserts.push_back(key(a, 1));
    work.upserts.push_back(key(b, 1));

    ss::abort_source as;
    auto r = h.make();
    srs::reconcile_stats stats;
    auto res = r.reconcile(std::move(work), {}, stats, as).get();
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error().kind, srs::source_error_kind::source_unavailable);
}

// Concurrency stress: a moderately large reference DAG (a base layer of leaves,
// two layers each referencing the layer below, plus diamonds where multiple
// referrers share referents), run with parallelism=4 and a modest byte budget
// so the reserve-then-consume admission path is exercised. The run must
// complete (no hang) with every node imported, no per-item errors, the
// byte-semaphore back at its full budget (no leak), and topological order held
// for every reference edge. Determinism comes from --runs_per_test rather than
// randomised interleavings: with four workers and a tight budget, worker
// scheduling already varies run to run.
TEST(reconciler, concurrent_stress) {
    reconcile_harness h;

    constexpr int leaves = 24;
    constexpr int mids = 12;
    constexpr int tops = 6;

    auto sub = [](std::string_view prefix, int i) {
        return ppsr::context_subject::unqualified(
          fmt::format("{}-{}", prefix, i));
    };

    // Record every reference edge so the topological-order assertion can check
    // each one against the destination's import order.
    std::vector<std::pair<ppsr::context_subject, ppsr::context_subject>> edges;
    auto link =
      [&](const ppsr::context_subject& from, const ppsr::context_subject& to) {
          edges.emplace_back(to, from);
      };

    // Base layer: leaves with no references.
    for (int i = 0; i < leaves; ++i) {
        h.source.add(sub("leaf", i), 1);
    }

    // Mid layer: each references two distinct leaves; adjacent mids share a
    // leaf, forming diamonds.
    for (int i = 0; i < mids; ++i) {
        auto l1 = sub("leaf", (2 * i) % leaves);
        auto l2 = sub("leaf", (2 * i + 1) % leaves);
        h.source.add_with_refs(
          sub("mid", i), 1, refs_to({ref_to(l1, 1), ref_to(l2, 1)}));
        link(sub("mid", i), l1);
        link(sub("mid", i), l2);
    }

    // Top layer: each references two mids (shared across tops -> more
    // diamonds).
    for (int i = 0; i < tops; ++i) {
        auto m1 = sub("mid", (2 * i) % mids);
        auto m2 = sub("mid", (2 * i + 1) % mids);
        h.source.add_with_refs(
          sub("top", i), 1, refs_to({ref_to(m1, 1), ref_to(m2, 1)}));
        link(sub("top", i), m1);
        link(sub("top", i), m2);
    }

    constexpr int total = leaves + mids + tops;

    // Upserts in reverse-dependency order (tops first) so the engine has to
    // reorder; four workers; a budget that holds only a few bodies at once.
    srs::work_set work;
    for (int i = 0; i < tops; ++i) {
        work.upserts.push_back(key(sub("top", i), 1));
    }
    for (int i = 0; i < mids; ++i) {
        work.upserts.push_back(key(sub("mid", i), 1));
    }
    for (int i = 0; i < leaves; ++i) {
        work.upserts.push_back(key(sub("leaf", i), 1));
    }

    h.lim = srs::reconciler::limits{
      .memory_bytes = no_memory_limit, .parallelism = 4};

    ss::abort_source as;
    auto r = h.make();
    srs::reconcile_stats stats;
    auto res = r.reconcile(std::move(work), {}, stats, as).get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(stats.versions_changed, total);
    EXPECT_EQ(stats.errors, 0);

    // Every node imported exactly once.
    const auto& all = h.destination.get_all();
    EXPECT_EQ(all.size(), static_cast<size_t>(total));

    // Topological order holds: for every (referent, referrer) edge the referent
    // was imported before the referrer.
    for (const auto& [referent, referrer] : edges) {
        auto i_ref = index_of(all, referent.sub());
        auto i_rer = index_of(all, referrer.sub());
        ASSERT_GE(i_ref, 0);
        ASSERT_GE(i_rer, 0);
        EXPECT_LT(i_ref, i_rer)
          << fmt::format("{} must precede {}", referent.sub(), referrer.sub());
    }
}

TEST(reconciler, remaps_context_at_import) {
    fake_source_state source;
    source.contexts.push_back(ppsr::context{".prod"});
    auto base = ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{"base"}};
    auto leaf = ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{"leaf"}};
    auto orders = ppsr::context_subject{
      ppsr::context{".prod"}, ppsr::subject{"orders"}};
    source.add(base, 1);
    source.add(leaf, 1);
    // orders references base with a context-qualified reference, and leaf with
    // an unqualified one (which resolves against orders' own .prod context).
    source.add_with_refs(
      orders,
      1,
      refs_to(
        {ref_to(base, 1),
         ref_to(ppsr::context_subject::unqualified("leaf"), 1)}));

    fake_source_reader reader{&source};
    schema::fake_registry destination;

    // Map source .prod onto destination .dest.
    model::schema_registry_sync_config config;
    model::schema_registry_sync_config::shadow_schema_registry_api api;
    model::schema_registry_sync_config::exact_context_mapping mapping;
    mapping.mappings.emplace(".prod", ".dest");
    api.destination = std::move(mapping);
    config.sync_mode = std::move(api);
    auto mapper = srs::context_mapper::make(config);

    srs::reconciler::limits lim{
      .memory_bytes = no_memory_limit, .parallelism = 1};
    auto r = srs::reconciler{
      &reader,
      &destination,
      [](const ppsr::context_subject&) { return true; },
      mapper,
      lim,
      model::schema_registry_sync_config::unsupported_feature_policy::fail};

    srs::work_set work;
    work.upserts.push_back(key(base, 1));
    work.upserts.push_back(key(leaf, 1));
    work.upserts.push_back(key(orders, 1));
    ss::abort_source as;
    srs::reconcile_stats stats;
    auto res = r.reconcile(std::move(work), {}, stats, as).get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(stats.versions_changed, 3);

    // Every schema lands in the destination .dest context (subject remapped)...
    const auto& all = destination.get_all();
    ASSERT_EQ(all.size(), 3u);
    for (const auto& s : all) {
        EXPECT_EQ(s.schema.sub().ctx, ppsr::context{".dest"});
    }
    const ppsr::stored_schema* orders_stored = nullptr;
    for (const auto& s : all) {
        if (s.schema.sub().sub == ppsr::subject{"orders"}) {
            orders_stored = &s;
        }
    }
    ASSERT_NE(orders_stored, nullptr);
    const auto& refs = orders_stored->schema.def().refs();
    ASSERT_EQ(refs.size(), 2u);
    auto ref_named =
      [&](std::string_view name) -> const ppsr::schema_reference* {
        for (const auto& r : refs) {
            if (r.name == name) {
                return &r;
            }
        }
        return nullptr;
    };
    // ...the qualified reference's own context is remapped to .dest...
    const auto* base_ref = ref_named("base");
    ASSERT_NE(base_ref, nullptr);
    EXPECT_EQ(base_ref->sub.qualified, ppsr::is_qualified::yes);
    EXPECT_EQ(base_ref->sub.sub.ctx, ppsr::context{".dest"});
    // ...and the unqualified reference is left as-is (it resolves against the
    // remapped parent on the destination, so it needs no rewrite).
    const auto* leaf_ref = ref_named("leaf");
    ASSERT_NE(leaf_ref, nullptr);
    EXPECT_EQ(leaf_ref->sub.qualified, ppsr::is_qualified::no);
    EXPECT_EQ(leaf_ref->sub.sub.ctx, ppsr::default_context);
}

} // namespace cluster_link::tests
