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

#include "cluster_link/model/types.h"
#include "cluster_link/schema_registry_sync/scope.h"
#include "cluster_link/schema_registry_sync/source_reader.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/registry.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/noncopyable_function.hh>

#include <cstdint>

namespace cluster_link::schema_registry_sync {

namespace ppsr = pandaproxy::schema_registry;

/// Outcome counters folded into the task's per-sync summary.
struct reconcile_stats {
    /// Source versions imported into the destination this run.
    uint64_t versions_changed{0};
    /// Per-item failures (unresolvable references, import conflicts, unparsable
    /// schemas, and -- under the FAIL policy -- source schemas carrying
    /// unsupported fields). These do not abort the run; the reconciler counts
    /// them and continues so the rest of the subjects still replicate.
    uint64_t errors{0};
    /// Unsupported fields removed from imported schemas this run under the
    /// REMOVE policy.
    uint64_t unsupported_features_removed{0};
};

/// The set of changes a reconcile applies: each upsert is a (context, subject,
/// version) node imported from the source with its source deleted state. The
/// caller selects which nodes to upsert (create, reactivate, or propagate a
/// soft-delete); the reconciler imports them in reference order.
///
/// `soft_deleted` names the upserts that are soft-deleted on the source, so the
/// reconciler imports them soft-deleted regardless of the per-version source
/// response, which cannot be relied on for deleted-state: a standard source
/// (e.g. Confluent) omits the `deleted` flag from the per-version body by
/// default. The caller instead derives this from its own active-vs-deleted
/// listing partition, which is authoritative across SR vendors.
struct work_set {
    chunked_vector<ppsr::subject_version> upserts;
    chunked_hash_set<ppsr::subject_version> soft_deleted;
};

/// \brief Imports missing source schema versions into the destination in
/// reference (topological) order.
///
/// The destination Schema Registry validates that a schema's references already
/// exist before accepting an import, so a referrer can only be imported after
/// every schema it references. The reconciler discovers each node's references,
/// builds the dependency graph incrementally, and drains it referent-first.
///
/// A node whose references are all already satisfied is imported on its first
/// fetch (1-fetch path). A node discovered before its dependencies releases its
/// body and is re-fetched once they complete (2-fetch path); this keeps a body
/// in memory only while it is being imported, so a byte-budgeted driver cannot
/// deadlock on a held body.
///
/// Not re-entrant: a single reconcile runs at a time. Per-run state is reset at
/// the start of each `reconcile` call.
class reconciler {
public:
    /// Resource bounds for a reconcile run.
    struct limits {
        /// Byte budget for schema bodies held in memory concurrently. Each
        /// fetch reserves a small fixed amount before reading, then tops up to
        /// the real body size with a non-blocking consume() that may drive the
        /// budget negative; the next fetch then blocks until in-flight bodies
        /// drain. A single body larger than the whole budget is admitted alone
        /// (its reservation is clamped to the budget) so it still makes
        /// progress. Floored to 1, so a degenerate 0 admits one body at a time.
        ///
        /// This bound is backpressure, not a hard per-fetch ceiling: the
        /// reservation gates fetch entry and the post-read consume() throttles
        /// subsequent fetches once a large body lands. A hard per-fetch byte
        /// cap would need Content-Length from the real client up front and is
        /// deferred.
        size_t memory_bytes;
        /// Number of worker fibers draining the discover/import queues.
        size_t parallelism;
    };

    /// \param mapper source->destination context remapping, applied only at the
    ///        import boundary (the discovery graph, `in_scope`, and reference
    ///        resolution all run in the source-context namespace). The mapper
    ///        must outlive the reconciler.
    reconciler(
      source_reader* source,
      schema::registry* destination,
      ss::noncopyable_function<bool(const ppsr::context_subject&)> in_scope,
      const context_mapper& mapper,
      limits lim,
      model::schema_registry_sync_config::unsupported_feature_policy
        feature_policy);

    /// Imports `work_set.upserts` referent-first.
    ///
    /// `seed_replicated` is the destination inventory's `all` set: nodes
    /// already present on the destination (including soft-deleted ones) satisfy
    /// a referrer's references without being re-imported.
    ///
    /// `stats` is reset to zero at entry, then incremented live as each node
    /// completes: `versions_changed` on every successful import, `errors` on
    /// every per-item failure, at the moment it happens. The caller can observe
    /// partial progress mid-run (it is single-shard, so the increments are
    /// synchronous and race-free), and reads per-run counts when reconcile
    /// returns.
    ///
    /// If the source becomes unreachable the future resolves with a
    /// `source_error` of kind `source_unavailable`; the caller surfaces this as
    /// link-unavailable. Per-item failures are counted in
    /// `reconcile_stats::errors`, not propagated.
    ss::future<source_result<void>> reconcile(
      work_set work,
      chunked_hash_set<ppsr::subject_version> seed_replicated,
      reconcile_stats& stats,
      ss::abort_source& as);

private:
    /// Lifecycle of a node in the reconcile graph. Every enqueue is gated on
    /// the node's state so a node is never fetched, imported, or counted twice.
    enum class node_state : uint8_t {
        // Not yet enqueued for discovery.
        unseen,
        // Queueud (or being fetched) for the first time to discover its
        // references. Separate from unseen so that a referent is only queued
        // once (even in the case of multiple referrers or cyclic references).
        discovering,
        // Waiting on dependencies to complete before it can be imported.
        pending,
        // Dependencies satisfied; being refertched and written to the
        // destination.
        importing,
        // Successfully imported
        done,
        // Failed; cannot be imported
        errored,
    };

    struct node_data {
        node_state state{node_state::unseen};
        uint32_t in_deg{0};
        chunked_vector<ppsr::subject_version> dependents;
    };

    /// Reads a discover-queue node, resolves its references, and either imports
    /// it now (deps satisfied) or registers it as pending behind its missing
    /// referents. Returns a source_error only on `source_unavailable`.
    ss::future<source_result<void>>
    discover(const ppsr::subject_version& n, ss::abort_source& as);

    /// Reads and imports an import-queue node whose dependencies have all
    /// completed.
    ss::future<source_result<void>>
    do_import(const ppsr::subject_version& n, ss::abort_source& as);

    /// Imports a fetched read, classifying conflicts as per-item failures.
    /// Returns true on a successful import. Carries the whole
    /// source_schema_read (not just the schema) so the unsupported-feature
    /// policy can act on `read.unsupported` at the authoritative per-node
    /// import point (FAIL rejection and REMOVE accounting both happen here).
    ss::future<bool>
    import_body(const ppsr::subject_version& n, ppsr::source_schema_read read);

    /// Under the FAIL policy, treats a schema carrying `unsupported` fields as
    /// a per-item failure: logs the offending fields, marks the node (and its
    /// dependents) failed via fail(n), and returns true so the caller skips the
    /// import. The rest of the sync proceeds -- fail-fast is reserved for
    /// global errors like source unavailability. Returns false (a no-op) under
    /// REMOVE or when the list is empty.
    ///
    /// Called by `import_body` (authoritative gate) and by `discover` before
    /// queuing a node's references.
    bool fail_if_contains_unsupported(
      const ppsr::subject_version& n,
      const chunked_vector<ppsr::unsupported_feature>& unsupported);

    /// Under the REMOVE policy, accounts for the `unsupported` fields the
    /// parser already dropped from a schema: logs them and adds them to
    /// `unsupported_features_removed`. A no-op under FAIL or when the list is
    /// empty. Call only after the import succeeds, so a failed import does not
    /// report features as removed.
    void count_if_contains_unsupported_removed(
      const ppsr::subject_version& n,
      const chunked_vector<ppsr::unsupported_feature>& unsupported);

    /// Marks a node replicated and releases dependents whose in-degree hits 0.
    void wake(const ppsr::subject_version& n);

    /// Marks a node (and transitively its dependents) failed.
    void fail(const ppsr::subject_version& n, bool recurse = true);

    node_data& data(const ppsr::subject_version& n);

    /// True while either queue has a node ready to be picked up by a worker.
    bool has_work() const { return !_discover_q.empty() || !_import_q.empty(); }

    /// A single worker fiber: waits for work, runs one node's discover/import,
    /// and drives global-termination detection. Loops until done or aborted.
    /// A source fault is recorded in `_fault` and stops the pool.
    ss::future<> worker(ss::abort_source& as);

    source_reader* _source;
    schema::registry* _destination;
    ss::noncopyable_function<bool(const ppsr::context_subject&)> _in_scope;
    const context_mapper* _mapper;
    limits _limits;
    model::schema_registry_sync_config::unsupported_feature_policy
      _feature_policy;

    chunked_hash_set<ppsr::subject_version> _replicated;
    /// Upserts the caller classified as soft-deleted on the source; imported
    /// soft-deleted, overriding the per-version source body's deleted flag.
    chunked_hash_set<ppsr::subject_version> _soft_deleted;
    chunked_hash_map<ppsr::subject_version, node_data> _nodes;
    chunked_vector<ppsr::subject_version> _discover_q;
    chunked_vector<ppsr::subject_version> _import_q;
    /// Caller-provided counters, incremented live as nodes complete. Valid only
    /// for the duration of a `reconcile` call.
    reconcile_stats* _stats{nullptr};

    /// Queued-or-in-flight node count. A node counts from the moment it is
    /// enqueued until the worker that popped it finishes its discover/import
    /// (including the synchronous `wake`/`fail` that may re-enqueue
    /// dependents). The run is complete when this reaches zero with both queues
    /// empty.
    size_t _outstanding{0};
    /// Set once the run is complete or must stop (fault/abort); all workers
    /// observe it through the condition variable and exit.
    bool _done{false};
    /// First `source_unavailable` error seen by any worker; stops the pool.
    std::optional<source_error> _fault;
    /// First non-shutdown exception (e.g. an unexpected import error) seen by
    /// any worker; rethrown after the pool drains to fault the whole sync.
    std::exception_ptr _exn;

    ssx::semaphore _mem;
    ss::condition_variable _cv;
};

} // namespace cluster_link::schema_registry_sync
