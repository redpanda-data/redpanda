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

#pragma once

#include "cluster_link/schema_registry_sync/probe.h"
#include "cluster_link/schema_registry_sync/reconciler.h"
#include "cluster_link/schema_registry_sync/source_reader.h"
#include "cluster_link/task.h"
#include "container/chunked_hash_map.h"
#include "schema/registry.h"
#include "ssx/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/util/noncopyable_function.hh>

namespace cluster_link::schema_registry_sync {

/// A snapshot of the destination Schema Registry's in-scope (subject, version)
/// nodes, retained for diffing against the source during reconciliation.
struct inventory {
    /// Non-deleted (subject, version) nodes.
    chunked_hash_set<ppsr::subject_version> active;
    /// Non-deleted and soft-deleted nodes; a superset of `active`.
    chunked_hash_set<ppsr::subject_version> all;
};

/// Scans the destination registry for every in-scope (subject, version) node.
/// A single include_deleted scan reports each version's soft-delete state, so
/// `active` is the non-deleted subset of `all` from one snapshot.
///
/// `in_scope` is a source-namespace predicate; the destination is scanned in
/// the destination namespace, so the scan reverse-maps each destination context
/// to its source context via `mapper` before applying `in_scope` and returns
/// nodes in the source namespace, keeping the whole diff single-namespaced.
/// Under the identity mapper this is a passthrough. `in_scope` must be pure; it
/// runs on each registry shard.
ss::future<inventory> scan_destination_inventory(
  schema::registry& destination,
  ss::noncopyable_function<bool(const ppsr::context_subject&)> in_scope,
  const context_mapper& mapper,
  ss::abort_source& as);

/// Shadows a source Schema Registry into the local (destination) Schema
/// Registry. Runs on the shard leading `_schemas/0`, a cluster-wide singleton.
/// Each run reconciles the source onto the destination, importing the source
/// schema versions missing from the destination in reference (topological)
/// order.
///
/// Source failures travel as `source_error` values: an unavailable source
/// parks the link, a per-item failure is counted and skipped. Destination and
/// internal faults throw and become `faulted` via the base task runner.
class mirroring_task : public task {
public:
    static constexpr auto task_name = "Schema Registry Shadowing";

    mirroring_task(
      link* link,
      const model::metadata& link_metadata,
      schema::registry* destination,
      source_reader_factory* source_factory);
    mirroring_task(const mirroring_task&) = delete;
    mirroring_task(mirroring_task&&) = delete;
    mirroring_task& operator=(const mirroring_task&) = delete;
    mirroring_task& operator=(mirroring_task&&) = delete;
    ~mirroring_task() override = default;

    void update_config(const model::metadata& link_metadata) override;

    ss::future<cl_result<void>> start() override;

    ss::future<cl_result<void>> stop() noexcept override;

    model::enabled_t is_enabled() const final;

    model::task_status_report get_status_report() const override;

protected:
    ss::future<state_transition> run_impl(ss::abort_source&) override;

    bool should_start_impl(ss::shard_id, ::model::node_id) const final;

    bool should_stop_impl(ss::shard_id, ::model::node_id) const final;

private:
    bool leads_schema_registry_partition() const;

    /// Rebuilds the source reader from the current API-mode config, releasing
    /// the previous reader's transport first. Called on a config change so a
    /// new source URL, auth, or TLS setting takes effect on the next run.
    ss::future<> reset_reader();

    /// Clears the in-memory sync state (status counters, destination inventory,
    /// last-full-sync timestamp) on losing leadership, so the next leader -- a
    /// new instance or this same one regaining leadership -- re-derives
    /// everything from the durable destination store instead of a prior
    /// tenure's view. `_config`/`_config_changed` are preserved: config is
    /// authoritative and a change queued while stopped must still take effect.
    void reset_sync_state();

    /// Whether a periodic full scan is due (first run, or the full-sync
    /// interval has elapsed). A config change additionally forces one via
    /// `_config_changed`, consumed in `run_impl`.
    bool should_long_sync() const;

    /// Rescans the destination inventory across all in-scope contexts, retains
    /// it on the task, and refreshes the destination counters. Throws on
    /// internal/destination faults.
    ss::future<> refresh_destination_inventory(
      const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
        in_scope,
      ss::abort_source&);

    /// Full source scan and create-only reconcile: discovers the active source
    /// nodes (across `contexts`), imports those missing from the destination's
    /// active set in reference order, and folds the result into the in-progress
    /// sync summary and the task status. Returns the resulting task state
    /// (active, or link_unavailable if the source becomes unreachable).
    ss::future<state_transition> full_source_sync(
      ss::abort_source&,
      const chunked_hash_set<ppsr::context>& contexts,
      model::schema_registry_sync_config::unsupported_feature_policy
        feature_policy,
      const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
        in_scope);

    /// Lists one subject's versions, classifying each into `source_active` or
    /// `source_deleted`. The source listing returns bare version numbers, so
    /// two calls recover the per-version deleted state: include_deleted::no
    /// gives the active versions, and the rest of the include_deleted::yes
    /// listing are soft-deleted. A reachable-but-failed listing is a counted
    /// per-item error and adds the subject to `failed_subjects` so its versions
    /// (undiscovered, hence source-absent-looking) are spared the hard-delete;
    /// a source_unavailable is captured in `unavailable` to back off the sync.
    ss::future<> list_one_subject(
      const ppsr::context_subject& subject,
      ss::abort_source& as,
      chunked_hash_set<ppsr::subject_version>& source_active,
      chunked_hash_set<ppsr::subject_version>& source_deleted,
      chunked_hash_set<ppsr::context_subject>& failed_subjects,
      std::optional<source_error>& unavailable);

    /// One source version listing with shared error handling: returns the
    /// versions on success, or nullopt after capturing a source_unavailable in
    /// `unavailable` or counting a reachable-but-failed listing as a per-item
    /// error. Lets list_one_subject short-circuit so a failing subject counts
    /// at most one error across its two listings.
    ss::future<std::optional<chunked_vector<ppsr::schema_version>>>
    list_versions_once(
      const ppsr::context_subject& subject,
      ppsr::include_deleted include_deleted,
      ss::abort_source& as,
      std::optional<source_error>& unavailable);

    /// A destination (subject, version) to hard-delete because the source no
    /// longer has it.
    struct purge_target {
        ppsr::subject_version node;
        bool was_active;
    };

    /// Hard-deletes the source-absent destination versions in `targets`. A
    /// version still referenced by another not-yet-purged version cannot be
    /// deleted, so reference-blocked deletes are retried across rounds until a
    /// round makes no progress. Returns the number of versions purged (folded
    /// into subject-version changes).
    ss::future<uint64_t> purge_destination_only_versions(
      chunked_vector<purge_target> targets, ss::abort_source& as);

    /// Attempts one hard-delete of `target`. If it fails because the version is
    /// still referenced by another version, it is re-queued into `next_round`.
    /// On success, `purged` is incremented.
    ss::future<> purge_one(
      purge_target target,
      ss::abort_source& as,
      uint64_t& purged,
      chunked_vector<purge_target>& next_round);

    /// Soft-deletes `dest_sub`/`version` first if `was_active` (the store
    /// refuses to tombstone an active version), then permanently deletes it.
    ss::future<> hard_delete_target(
      const ppsr::context_subject& dest_sub,
      ppsr::schema_version version,
      bool was_active);

    /// Tombstones every destination context this link owns and holds in scope
    /// as a whole that no longer exists at the source (`contexts`). The
    /// context's subjects must already have been purged; a not-yet-empty
    /// context (e.g. a reference-blocked version survived the purge) is left
    /// for the next full sync. Matches Confluent's DELETE /contexts: only the
    /// context marker is removed, leaving any context-level mode/config
    /// overrides untouched.
    ss::future<> delete_source_absent_contexts(
      const chunked_hash_set<ppsr::context>& contexts,
      const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
        in_scope,
      ss::abort_source& as);

    /// Replicates one target's (subject or context-only) source mode and
    /// compatibility config onto the destination: writes the source's own
    /// override when it has one, deletes the destination override otherwise.
    ss::future<> sync_mode_and_config(
      const ppsr::context_subject& target,
      model::schema_registry_sync_config::unsupported_feature_policy
        feature_policy,
      ss::abort_source& as,
      std::optional<source_error>& unavailable);

    /// Under FAIL, records unsupported config fields as a per-item error and
    /// returns true so the caller skips the config write; a no-op (false)
    /// otherwise. Config-path analogue of the reconciler's helper.
    bool fail_if_contains_unsupported(
      const ppsr::context_subject& target,
      model::schema_registry_sync_config::unsupported_feature_policy
        feature_policy,
      const chunked_vector<ppsr::unsupported_feature>& unsupported);

    /// Under REMOVE, logs the unsupported config fields and counts them in
    /// `unsupported_features_removed`; a no-op otherwise. Called once per
    /// completed config sync -- a static source config re-counts every full
    /// sync, mirroring FAIL's per-sync errors -- but not after a failed write,
    /// which is counted as an error instead.
    void count_if_contains_unsupported_removed(
      const ppsr::context_subject& target,
      model::schema_registry_sync_config::unsupported_feature_policy
        feature_policy,
      const chunked_vector<ppsr::unsupported_feature>& unsupported);

    // Requires a sync in progress (`current_sync` engaged).
    void record_error(std::string_view what);

    [[nodiscard]] state_transition make_unavailable(const ss::sstring& reason);
    [[nodiscard]] state_transition make_active();
    [[nodiscard]] state_transition make_faulted(const ss::sstring& reason);

    model::schema_registry_sync_status get_live_sync_status() const;

    model::schema_registry_sync_config _config;
    // Source->destination context remapping for the current run, rebuilt from
    // _config at the start of each full sync. Applied only at the destination
    // boundary; identity when no remapping is configured.
    context_mapper _mapper;
    schema::registry* _destination;
    source_reader_factory* _source_factory;
    std::unique_ptr<source_reader> _reader;
    // Serializes stopping and replacing _reader. stop() (reader-first, while
    // run_impl is still live) and reset_reader() (run by run_impl on a config
    // change) both stop the reader and then free it via reassignment; without
    // serialization one can free the reader while the other's stop() is
    // suspended mid-shutdown, a use-after-free. Held only around the
    // stop+reassign, never across the run-fiber join, so it cannot deadlock
    // with task::stop().
    ssx::mutex _reader_lifecycle{"cluster_link/sr_source/reader_lifecycle"};
    inventory _destination_inventory;
    model::schema_registry_sync_status _status;
    // Live counters for the in-flight reconcile; reflected by get_status_report
    // for mid-sync progress, then folded into _status at end of run.
    reconcile_stats _reconcile_stats;
    probe _probe;
    std::optional<ss::lowres_clock::time_point> _last_full_sync;
    // Set by update_config, consumed by run_impl to force a full scan. A flag
    // (rather than mutating _status/_last_full_sync in update_config) avoids
    // racing an in-flight run_impl across its co_await suspension points.
    bool _config_changed{false};
};

class mirroring_task_factory : public task_factory {
public:
    mirroring_task_factory(
      schema::registry* destination, source_reader_factory* source_factory)
      : _destination(destination)
      , _source_factory(source_factory) {}

    std::string_view created_task_name() const noexcept override;

    std::unique_ptr<task> create_task(link* link) override;

private:
    schema::registry* _destination;
    source_reader_factory* _source_factory;
};

} // namespace cluster_link::schema_registry_sync
