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

#include "cluster_link/schema_registry_sync/discovery.h"
#include "cluster_link/schema_registry_sync/probe.h"
#include "cluster_link/schema_registry_sync/reconciler.h"
#include "cluster_link/schema_registry_sync/source_reader.h"
#include "cluster_link/schema_registry_sync/tail_reader.h"
#include "cluster_link/task.h"
#include "container/chunked_hash_map.h"
#include "schema/registry.h"
#include "ssx/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <expected>

namespace cluster_link::schema_registry_sync {

/// A snapshot of the destination Schema Registry's in-scope (subject, version)
/// nodes, retained for diffing against the source during reconciliation.
struct inventory {
    /// Non-deleted (subject, version) nodes.
    chunked_hash_set<ppsr::subject_version> active;
    /// Non-deleted and soft-deleted nodes; a superset of `active`.
    chunked_hash_set<ppsr::subject_version> all;
    /// Highest schema id held per source context, over the nodes above; one
    /// past it is the id probe's floor. Falls when nodes are purged, unlike
    /// the destination's id allocator, which any writer raises and nothing
    /// lowers.
    chunked_hash_map<ppsr::context, ppsr::schema_id> max_id;
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
      source_reader_factory* source_factory,
      tail_reader_factory* tail_factory);
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

    /// Builds the HTTP tail's read side; called wherever its probe cursor
    /// must die (task construction, a config change, a tenure reset).
    std::unique_ptr<discovery> make_discovery();

    /// Rebuilds the source and tail readers from the current API-mode config,
    /// releasing the previous readers' transports first. Called on a config
    /// change so a new source URL, auth, or TLS setting takes effect on the
    /// next run.
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

    /// Maps a source failure onto the run's outcome: an unreachable source
    /// parks the link, while a reachable-but-failed call is a counted per-item
    /// error that leaves the rest of the run standing.
    state_transition on_source_error(const source_error& error);

    /// As on_source_error, but for a tail tick, which holds a batch it may not
    /// have applied: a source that went unavailable gets the batch put back so
    /// a later tick can retry it.
    ss::future<state_transition>
    on_tail_source_error(const source_error& error);

    /// Lists the source contexts, intersects them with the effective scan
    /// scope and vets the resulting set with the same preconditions the full
    /// sync and HTTP fallback tail both require.
    ss::future<std::expected<chunked_hash_set<ppsr::context>, state_transition>>
    load_sync_contexts(
      ss::abort_source& as,
      bool unfiltered,
      const chunked_hash_set<ppsr::context>& scan_contexts);

    /// Incremental sync: applies the changes the tail reader recorded since the
    /// last poll, through the same per-subject path a full sync uses, so a
    /// replayed change is a no-op.
    ///
    /// Returns without touching the destination -- the common case -- when
    /// nothing changed, and never advances the full-sync timer: it sees only
    /// the subjects its batch named, so it is no substitute for the full scan.
    ss::future<state_transition> feed_tail_sync(
      ss::abort_source&,
      model::schema_registry_sync_config::unsupported_feature_policy
        feature_policy,
      const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
        in_scope);

    struct batch_sync_stats {
        reconcile_stats reconciled;
        uint64_t purged{0};
    };

    /// Replicates the subjects a tail batch named, through the same discover ->
    /// import -> purge path a full sync runs over the whole selected source.
    ss::future<source_result<batch_sync_stats>> sync_batch_subjects(
      const tail_batch& batch,
      const chunked_vector<ppsr::context_subject>& subjects,
      model::schema_registry_sync_config::unsupported_feature_policy
        feature_policy,
      const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
        in_scope,
      reconciler::limits limits,
      ss::abort_source& as);

    /// The tail tick for a source whose `_schemas` feed is not armed:
    /// discovers what the destination lacks over the HTTP API alone
    /// (discovery's subject-listing diff and schema-id probe) and imports
    /// it. Deliberately partial -- deletions, mode and compatibility
    /// changes, and registrations reusing an existing schema's id wait for
    /// the full sync -- and never touches `_last_full_sync`, so a tail tick
    /// cannot postpone or masquerade as a full scan.
    ss::future<state_transition> http_fallback_tail_sync(
      ss::abort_source&,
      const chunked_hash_set<ppsr::context>& contexts,
      model::schema_registry_sync_config::unsupported_feature_policy
        feature_policy,
      const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
        in_scope);

    /// Subjects the destination already holds, derived from the retained
    /// inventory -- rebuilt first when a run left it moved-out. `all`, not
    /// `active`: a fully soft-deleted subject is still one the destination
    /// knows, and reading it as new would re-list it on every tick forever.
    ss::future<chunked_hash_set<ppsr::context_subject>> known_subjects(
      const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
        in_scope,
      ss::abort_source&);

    /// The source (subject, version) nodes a discovery pass found, split by
    /// their soft-delete state at the source.
    using discovered_versions = discovery::discovered_versions;

    /// Diffs the discovered source nodes against the retained destination
    /// inventory into the versions to import, propagating source soft-deletes.
    /// Versions the source no longer has at all are hard-deleted instead, see
    /// `collect_purge_targets`.
    work_set build_work_set(const discovered_versions& discovered) const;

    /// Folds a finished reconcile's counters into the in-progress sync summary
    /// and the task totals, then clears them so the report-time reflection of
    /// the live counters cannot double-count. Returns the folded snapshot.
    /// The HTTP-fallback tail's counterpart of the fold inside
    /// `run_reconcile`, which this path cannot use -- see
    /// http_fallback_tail_sync's reconcile.
    reconcile_stats fold_reconcile_stats();

    /// Imports `work` referent-first, then folds the reconcile's counters into
    /// the in-progress sync summary and the task totals and returns them.
    /// Leaves them unfolded on the error path, where the caller abandons the
    /// sync.
    ss::future<source_result<reconcile_stats>> run_reconcile(
      work_set work,
      model::schema_registry_sync_config::unsupported_feature_policy
        feature_policy,
      const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
        in_scope,
      reconciler::limits limits,
      ss::abort_source& as);

    /// Replicates each target's source mode and compatibility config onto the
    /// destination with bounded concurrency. Returns the first
    /// source_unavailable seen, if any, for the caller to back off on.
    ss::future<std::optional<source_error>> sync_mode_configs(
      const chunked_vector<ppsr::context_subject>& targets,
      model::schema_registry_sync_config::unsupported_feature_policy
        feature_policy,
      reconciler::limits limits,
      ss::abort_source& as);

    /// A destination (subject, version) to hard-delete because the source no
    /// longer has it.
    struct purge_target {
        ppsr::subject_version node;
        bool was_active;
    };

    /// Selects the destination versions to hard-delete. `in_purge_scope` bounds
    /// the sweep to the subjects the caller discovered from the source this
    /// run: a caller that refreshed only some subjects must accept only those,
    /// or every subject it did not look at would read as source-absent.
    chunked_vector<purge_target> collect_purge_targets(
      const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
        in_purge_scope,
      const discovered_versions& discovered,
      const chunked_hash_set<ppsr::context>& failed_contexts,
      const chunked_hash_set<ppsr::context_subject>& failed_subjects) const;

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

    /// The tail's context-deletion phase: re-lists the source's contexts for
    /// delete_source_absent_contexts, because deletion is decided against the
    /// source's whole set and a batch's own contexts would delete every context
    /// it did not name. Returns the listing's error for the caller to back off
    /// on.
    ss::future<std::optional<source_error>> sync_absent_contexts(
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
    tail_reader_factory* _tail_factory;
    std::unique_ptr<tail_reader> _tail;
    // Read side of the HTTP fallback for tail ticks whose feed is not
    // armed. Recreated on a config change and on losing leadership: its
    // probe cursor must not outlive either, and recreating beats notifying.
    std::unique_ptr<discovery> _discovery;
    // Serializes stopping and replacing _reader/_tail. stop() (readers-first,
    // while run_impl is still live) and reset_reader() (run by run_impl on a
    // config change) both stop the readers and then free them via reassignment;
    // without serialization one can free a reader while the other's stop() is
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
      schema::registry* destination,
      source_reader_factory* source_factory,
      tail_reader_factory* tail_factory)
      : _destination(destination)
      , _source_factory(source_factory)
      , _tail_factory(tail_factory) {}

    std::string_view created_task_name() const noexcept override;

    std::unique_ptr<task> create_task(link* link) override;

private:
    schema::registry* _destination;
    source_reader_factory* _source_factory;
    tail_reader_factory* _tail_factory;
};

} // namespace cluster_link::schema_registry_sync
