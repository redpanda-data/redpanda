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

#include "cluster_link/schema_registry_sync/scope.h"
#include "cluster_link/schema_registry_sync/source_reader.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>

namespace cluster_link::schema_registry_sync {

/// Read-side counterpart of the reconciler: enumerates the source and
/// classifies what it finds. Never writes to the destination and never
/// transitions the task; failures come back as values in each result --
/// counting them, parking the link and any import are the caller's business.
/// The source reader is a per-call parameter rather than a borrow: it is the
/// one dependency the owner swaps mid-tenure (a config change rebuilds it),
/// so no stored pointer can outlive it.
///
/// Owns the probe's scan memory (cursor and denial-log limiter), so it must
/// not outlive a task tenure or a link configuration; the owning task
/// recreates it wherever either turns over.
class discovery {
public:
    using in_scope_fn
      = ss::noncopyable_function<bool(const ppsr::context_subject&)>;

    discovery(prefix_logger* logger, ss::sstring link_name);

    /// The source (subject, version) nodes a discovery pass found, split by
    /// their soft-delete state at the source.
    struct discovered_versions {
        chunked_hash_set<ppsr::subject_version> active;
        chunked_hash_set<ppsr::subject_version> deleted;
    };

    struct listing {
        chunked_vector<ppsr::context_subject> subjects;
        chunked_vector<ss::sstring> errors;
        std::optional<source_error> unavailable;
    };

    /// The HTTP tail's listing leg: in-scope subjects the destination does
    /// not know yet (absent from `known`). A failed per-context listing is an
    /// error in the result and the walk moves on -- this leg never deletes,
    /// so there is no purge to spare anything from.
    ss::future<listing> list_new_subjects(
      source_reader& reader,
      const chunked_hash_set<ppsr::context>& contexts,
      const chunked_hash_set<ppsr::context_subject>& known,
      const in_scope_fn& in_scope,
      ss::abort_source&);

    struct versions {
        discovered_versions discovered;
        chunked_hash_set<ppsr::context_subject> failed_subjects;
        chunked_vector<ss::sstring> errors;
        std::optional<source_error> unavailable;
    };

    /// Classifies each subject's versions into active vs. soft-deleted: two
    /// listings per subject (include_deleted::yes for the full set, ::no for
    /// the active subset), fanned out with the given parallelism. A reachable
    /// but failed listing leaves that subject in `failed_subjects`, so callers
    /// whose discovery drives purge can spare its destination versions from
    /// reading as source-absent.
    ss::future<versions> list_versions(
      source_reader& reader,
      const chunked_vector<ppsr::context_subject>& subjects,
      size_t parallelism,
      ss::abort_source&);

    struct probe_result {
        chunked_vector<ppsr::subject_version> found;
        /// In-scope pairs a resolved id backs that are soft-deleted at the
        /// source: pairs the deleted=true ask returned but the live-only
        /// ask did not -- as a smaller list or as a miss (a source honoring
        /// the parameter has no live view of a fully soft-deleted id). A
        /// source ignoring the parameter never reveals soft-deleted pairs
        /// to the probe at all, so there this stays empty and those pairs
        /// wait for the full sync.
        chunked_vector<ppsr::subject_version> found_deleted;
        chunked_vector<ss::sstring> errors;
        std::optional<source_error> unavailable;
    };

    /// Finds new versions of already-known subjects, which no subject
    /// listing can see: probes source schema ids upward from `floor` (the
    /// caller's inventory-derived position) or this instance's cursor,
    /// whichever is higher, until the first absent id. Every in-scope
    /// (subject, version) pair a resolved id backs lands in the result,
    /// split by its soft-delete state at the source -- except pairs whose
    /// live-only classification ask failed outright: those are dropped with
    /// a counted error and wait for the full sync.
    ///
    /// Intentionally always classifies probe hits by the second, live-only
    /// listing instead of deferring deleted-state to the reconciler or relying
    /// on earlier read bodies to carry `deleted`: the probe endpoint does not
    /// carry per-pair deleted state, and that body shape is only observed
    /// heuristically, so one later response may still omit the flag. The
    /// listing-derived split is the probe's safe fallback that keeps a newly
    /// discovered soft-deleted version from landing active on the destination.
    ss::future<probe_result> probe_new_ids(
      source_reader& reader,
      const chunked_hash_set<ppsr::context>& contexts,
      const context_mapper& mapper,
      const chunked_hash_map<ppsr::context, ppsr::schema_id>& floor,
      const in_scope_fn& in_scope,
      ss::abort_source&);

private:
    ss::future<std::optional<chunked_vector<ppsr::schema_version>>>
    list_versions_once(
      source_reader& reader,
      const ppsr::context_subject&,
      ppsr::include_deleted,
      versions& result,
      ss::abort_source&);

    ss::future<> list_one_subject(
      source_reader& reader,
      const ppsr::context_subject&,
      versions& result,
      ss::abort_source&);

    prefix_logger* _logger;
    ss::sstring _link_name;
    // Per-source-context id-probe position: one past the highest id confirmed
    // to exist at the source, so ticks do not re-walk ids that exist but
    // yielded no import (out of scope, or fully soft-deleted at the source)
    // and so never reach the caller's floor. Dies with the instance; misses
    // are deliberately not recorded, so a hole that fills in later is still
    // found.
    chunked_hash_map<ppsr::context, ppsr::schema_id> _probe_cursor;
    static constexpr auto probe_denial_log_window = std::chrono::minutes(5);
    // Limits the probe-denial warning to one per window per link: the probe
    // asks every tick, so an unlimited warn would repeat on every one, and a
    // shard-global limiter would let one link's denial silence another's.
    ss::logger::rate_limit _probe_denial_rate{probe_denial_log_window};
};

} // namespace cluster_link::schema_registry_sync
