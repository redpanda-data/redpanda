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

#include "cluster_link/schema_registry_sync/source_reader.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"

#include <seastar/core/abort_source.hh>
#include <seastar/util/noncopyable_function.hh>

namespace cluster_link::schema_registry_sync {

/// Read-side counterpart of the reconciler: enumerates the source and
/// classifies what it finds. Never writes to the destination and never
/// transitions the task; failures come back as values in each result --
/// counting them, parking the link and any import are the caller's business.
/// The source reader is a per-call parameter rather than a borrow: it is the
/// one dependency the owner swaps mid-tenure (a config change rebuilds it),
/// so no stored pointer can outlive it.
class discovery {
public:
    using in_scope_fn
      = ss::noncopyable_function<bool(const ppsr::context_subject&)>;

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
};

} // namespace cluster_link::schema_registry_sync
