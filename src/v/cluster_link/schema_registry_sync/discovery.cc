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

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>

namespace cluster_link::schema_registry_sync {

ss::future<discovery::listing> discovery::list_new_subjects(
  source_reader& reader,
  const chunked_hash_set<ppsr::context>& contexts,
  const chunked_hash_set<ppsr::context_subject>& known,
  const in_scope_fn& in_scope,
  ss::abort_source& as) {
    listing result;
    for (const auto& sctx : contexts) {
        auto subjects_res = co_await reader.list_subjects(sctx, as);
        if (!subjects_res.has_value()) {
            if (
              subjects_res.error().kind
              == source_error_kind::source_unavailable) {
                result.unavailable = std::move(subjects_res.error());
                co_return result;
            }
            // Reachable but failed: count it and move on. There is no purge
            // to narrow, since this leg never deletes.
            result.errors.push_back(std::move(subjects_res.error().message));
            continue;
        }
        for (auto& subject : subjects_res.value()) {
            if (in_scope(subject) && !known.contains(subject)) {
                result.subjects.push_back(std::move(subject));
            }
        }
    }
    co_return result;
}

ss::future<std::optional<chunked_vector<ppsr::schema_version>>>
discovery::list_versions_once(
  source_reader& reader,
  const ppsr::context_subject& subject,
  ppsr::include_deleted include_deleted,
  versions& result,
  ss::abort_source& as) {
    as.check();
    auto res = co_await reader.list_subject_versions(
      subject, include_deleted, as);
    if (res.has_value()) {
        co_return std::move(res.value());
    }
    if (res.error().kind == source_error_kind::source_unavailable) {
        if (!result.unavailable.has_value()) {
            result.unavailable = std::move(res.error());
        }
        co_return std::nullopt;
    }
    // Not-found means no versions of this kind, not a fault -- how a fully
    // soft-deleted subject's active-only listing reads.
    if (res.error().kind == source_error_kind::subject_not_found) {
        co_return chunked_vector<ppsr::schema_version>{};
    }
    // Reachable but failed (rare delete race): count and skip. The result is
    // touched only between co_awaits, so sharing it across the concurrent
    // fibers is safe on one reactor.
    result.errors.push_back(std::move(res.error().message));
    co_return std::nullopt;
}

ss::future<> discovery::list_one_subject(
  source_reader& reader,
  const ppsr::context_subject& subject,
  versions& result,
  ss::abort_source& as) {
    // A peer fiber already hit source_unavailable; skip the remaining work.
    if (result.unavailable.has_value()) {
        co_return;
    }
    // List all versions first: unlike the active-only listing, it succeeds
    // for a fully soft-deleted subject. The two listings partition versions
    // into active vs. soft-deleted.
    auto all = co_await list_versions_once(
      reader, subject, ppsr::include_deleted::yes, result, as);
    if (!all.has_value()) {
        result.failed_subjects.insert(subject);
        co_return;
    }
    auto active = co_await list_versions_once(
      reader, subject, ppsr::include_deleted::no, result, as);
    if (!active.has_value()) {
        result.failed_subjects.insert(subject);
        co_return;
    }
    chunked_hash_set<ppsr::schema_version> active_set;
    for (auto version : *active) {
        active_set.insert(version);
        result.discovered.active.insert(
          ppsr::subject_version{subject, version});
    }
    for (auto version : *all) {
        if (!active_set.contains(version)) {
            result.discovered.deleted.insert(
              ppsr::subject_version{subject, version});
        }
    }
}

ss::future<discovery::versions> discovery::list_versions(
  source_reader& reader,
  const chunked_vector<ppsr::context_subject>& subjects,
  size_t parallelism,
  ss::abort_source& as) {
    versions result;
    co_await ss::max_concurrent_for_each(
      subjects,
      std::max<size_t>(1, parallelism),
      [&](const ppsr::context_subject& subject) {
          return list_one_subject(reader, subject, result, as);
      });
    co_return result;
}

} // namespace cluster_link::schema_registry_sync
