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

#include <ranges>

namespace cluster_link::schema_registry_sync {

discovery::discovery(prefix_logger* logger, ss::sstring link_name)
  : _logger(logger)
  , _link_name(std::move(link_name)) {}

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

ss::future<discovery::probe_result> discovery::probe_new_ids(
  source_reader& reader,
  const chunked_hash_set<ppsr::context>& contexts,
  const context_mapper& mapper,
  const chunked_hash_map<ppsr::context, ppsr::schema_id>& floor,
  const in_scope_fn& in_scope,
  ss::abort_source& as) {
    probe_result result;
    // Only mapped, non-global contexts are probeable: the registry-wide
    // global is not an id namespace (the sync writes it to the destination
    // global directly), and an unmapped context is unreachable while
    // check_preconditions faults a link whose in-scope context has no
    // mapping -- without the guard the reconciler would fail every node the
    // probe found there and count it as an error.
    auto probeable = contexts
                     | std::views::filter([&mapper](const ppsr::context& ctx) {
                           return ctx != ppsr::global_context
                                  && mapper.forward(ctx).has_value();
                       });
    for (const auto& src_ctx : probeable) {
        // Start one past the highest id in `floor` or at the cursor,
        // whichever is higher, clamped to the lowest id a registry allocates
        // -- a default-constructed schema_id is the type's minimum, not zero.
        // Absent keys are looked up, not indexed; values are read by value
        // and written back by key, since the walk below suspends and a
        // reference into either map must not outlive a suspension point.
        const auto held = [&src_ctx](const auto& m) {
            auto it = m.find(src_ctx);
            return it == m.end() ? ppsr::schema_id{0} : it->second;
        };
        auto id = std::max(
          {ppsr::schema_id{1},
           held(floor) + ppsr::schema_id{1},
           held(_probe_cursor)});

        while (true) {
            // include_deleted::yes for existence: a source honoring the
            // parameter would otherwise answer a fully soft-deleted id with a
            // miss and end the walk early.
            auto res = co_await reader.list_schema_id_subject_versions(
              id, src_ctx, ppsr::include_deleted::yes, as);
            if (!res.has_value()) {
                const auto kind = res.error().kind;
                if (
                  kind == source_error_kind::schema_id_not_found
                  || kind == source_error_kind::forbidden) {
                    // Usually means we've reached the current end of allocated
                    // schema ids. A temporary hole from a hard-deleted id is
                    // repaired by the next full sync. forbidden reads the
                    // same: an ACL-enabled source deliberately answers a
                    // missing id with 403, so the existence ask cannot tell
                    // them apart.
                    break;
                }
                if (kind == source_error_kind::endpoint_unsupported) {
                    // The source does not serve this endpoint. Skip this probe
                    // leg for now without faulting the link, and retry next
                    // tick. Warn with per-link rate limiting.
                    vloglr(
                      (*_logger),
                      ss::log_level::warn,
                      _probe_denial_rate,
                      "Schema Registry tail sync (link {}): source does not "
                      "serve the schema-id probe ({}); until it does, new "
                      "versions of known subjects replicate on the full sync",
                      _link_name,
                      res.error().message);
                    co_return result;
                }
                if (kind == source_error_kind::source_unavailable) {
                    result.unavailable = std::move(res.error());
                    co_return result;
                }
                result.errors.push_back(std::move(res.error().message));
                break;
            }
            // A hit means the id exists, even if the pair list is empty.
            //
            // Ask again live-only; pairs missing from that result are
            // soft-deleted. Deliberately not optimized away based on earlier
            // body reads: the reconciler needs this listing-derived split as a
            // safe fallback when one later body omits `deleted`.
            chunked_hash_set<ppsr::subject_version> live;
            // Skipped when no pair is in scope: the answer would be
            // discarded whole.
            const bool any_in_scope = std::ranges::any_of(
              res.value(), [&](const auto& sv) { return in_scope(sv.sub); });
            if (any_in_scope) {
                auto active = co_await reader.list_schema_id_subject_versions(
                  id, src_ctx, ppsr::include_deleted::no, as);
                if (!active.has_value()) {
                    const auto kind = active.error().kind;
                    if (kind == source_error_kind::source_unavailable) {
                        result.unavailable = std::move(active.error());
                        co_return result;
                    }
                    if (kind != source_error_kind::schema_id_not_found) {
                        // The first ask proved the id exists, so a follow-up
                        // failure (403 included -- `forbidden` is not a
                        // miss) must not hold the cursor here and wedge the
                        // walk on this id every tick. The cost is real:
                        // this id's pairs are dropped and wait for the next
                        // full sync, which enumerates by subject and lifts
                        // the floor past them. Counted so the tick is not
                        // silent about it.
                        result.errors.push_back(
                          std::move(active.error().message));
                        ++id;
                        _probe_cursor[src_ctx] = id;
                        continue;
                    }
                    // A miss here is how a source honoring the deleted
                    // parameter answers an id whose every version is
                    // soft-deleted: it has no live view. Fall through with
                    // `live` empty, classifying every pair as soft-deleted.
                    // (A hard-delete racing in between reads the same; its
                    // stale pairs then fail their body reads as counted
                    // per-item errors.)
                } else {
                    for (auto& sv : active.value()) {
                        live.insert(std::move(sv));
                    }
                }
            }
            auto scoped = res.value() | std::views::filter([&](const auto& sv) {
                              return in_scope(sv.sub);
                          });
            for (auto& sv : scoped) {
                if (live.contains(sv)) {
                    result.found.push_back(std::move(sv));
                } else {
                    result.found_deleted.push_back(std::move(sv));
                }
            }
            ++id;
            _probe_cursor[src_ctx] = id;
        }
    }
    co_return result;
}

} // namespace cluster_link::schema_registry_sync
