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
#include "cluster_link/schema_registry_sync/tests/sr_sync_test_fixtures.h"
#include "container/chunked_hash_map.h"
#include "pandaproxy/schema_registry/types.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>

#include <gmock/gmock.h>

// Unit tests for the discovery legs against a scripted source: the failure
// classification (per-item error vs. source_unavailable) and the
// active/soft-deleted partition -- the corner cases that are awkward to
// stage through a running task.
//
// The http_fallback_tail_sync orchestration has no unit tests here on purpose:
// it is the glue between discovery and task-owned state (the retained
// inventory, the live stats, the reconcile), and those seams only exist with
// the task around them. It is covered end to end by the http_tail_* tests in
// mirroring_task_test.cc.
namespace cluster_link::tests {

namespace {

struct discovery_harness {
    fake_source_state source;
    fake_source_reader reader{&source};
    srs::discovery disc;
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

} // namespace cluster_link::tests
