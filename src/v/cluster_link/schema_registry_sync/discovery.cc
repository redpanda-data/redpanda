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
            auto res = co_await reader.list_schema_id_subject_versions(
              id, src_ctx, as);
            if (!res.has_value()) {
                const auto kind = res.error().kind;
                if (kind == source_error_kind::schema_id_not_found) {
                    // The routine end of the walk: ids are consecutive, so
                    // an absent id nearly always means the counter has not
                    // moved. A hole (registered and hard-deleted between
                    // ticks) stops the walk short instead, for at most one
                    // full-sync interval -- the full sync imports by subject
                    // and lifts the floor over it.
                    break;
                }
                if (kind == source_error_kind::endpoint_unavailable) {
                    // The source does not serve this endpoint (e.g. not
                    // implemented). Skip the leg for this tick only -- no
                    // error counted, no parking a link whose other reads
                    // work -- and ask again next tick.
                    //
                    // Rate limited rather than logged once: the condition
                    // is a lasting state, and a single line would scroll out
                    // of retention. The limiter is a member, so one link's
                    // denial cannot silence another's warning.
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
            // A resolving id continues the walk -- even with an empty result,
            // which means every version behind it is soft-deleted, not that
            // the id is unallocated.
            for (auto& sv : res.value()) {
                if (in_scope(sv.sub)) {
                    result.found.push_back(std::move(sv));
                }
            }
            ++id;
            _probe_cursor[src_ctx] = id;
        }
    }
    co_return result;
}

} // namespace cluster_link::schema_registry_sync
