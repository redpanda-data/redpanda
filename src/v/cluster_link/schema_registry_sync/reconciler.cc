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

#include "base/units.h"
#include "cluster_link/logger.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <utility>

using namespace std::chrono_literals;

namespace cluster_link::schema_registry_sync {

namespace {

// Units reserved against the memory budget before a body is fetched, gating
// fetch entry on memory before the body size is known. After the read the
// reservation is topped up to the real body size.
constexpr size_t reconcile_reserve_bytes = 100_KiB;

// Resolves a source schema's references to the (subject, version) nodes they
// point at, so referenced schemas are imported before the schemas that depend
// on them. Unqualified references resolve against the referring schema's own
// context.
chunked_vector<ppsr::subject_version>
resolve_refs(const ppsr::subject_schema& schema) {
    const auto& parent_ctx = schema.sub().ctx;
    chunked_vector<ppsr::subject_version> out;
    for (const auto& ref : schema.def().refs()) {
        out.push_back(
          ppsr::subject_version{ref.sub.resolve(parent_ctx), ref.version});
    }
    return out;
}

// Byte-size proxy for a schema body: the canonical definition's length. This
// is what the byte-semaphore budgets, and what tests control via the fake.
size_t body_size(const ppsr::subject_schema& s) {
    return s.def().raw()().size_bytes();
}

// Rewrites a schema's contexts source->destination for import: the subject's
// context and each context-qualified reference's context. An unqualified
// reference is left alone -- it resolves against the referrer's remapped parent
// on the destination. Body, type, and metadata are unchanged. nullopt if a
// context has no destination mapping.
std::optional<ppsr::stored_schema>
remap_for_import(const context_mapper& mapper, ppsr::stored_schema s) {
    if (mapper.is_identity()) {
        return s;
    }
    auto [sub, def] = std::move(s.schema).destructure();
    auto mapped_ctx = mapper.forward(sub.ctx);
    if (!mapped_ctx.has_value()) {
        return std::nullopt;
    }
    sub.ctx = *mapped_ctx;
    auto [raw, type, refs, meta] = std::move(def).destructure();
    for (auto& ref : refs) {
        if (ref.sub.qualified == ppsr::is_qualified::yes) {
            auto mapped_ref_ctx = mapper.forward(ref.sub.sub.ctx);
            if (!mapped_ref_ctx.has_value()) {
                return std::nullopt;
            }
            ref.sub.sub.ctx = *mapped_ref_ctx;
        }
    }
    return ppsr::stored_schema{
      .schema = ppsr::
        subject_schema{std::move(sub), ppsr::schema_definition{std::move(raw), type, std::move(refs), std::move(meta)}},
      .version = s.version,
      .id = s.id,
      .deleted = s.deleted};
}

void adjust_units(
  ssx::semaphore& sem, ssx::semaphore_units& units, size_t new_size) {
    auto reserved = units.count();
    if (reserved < new_size) {
        units.adopt(ss::consume_units(sem, new_size - reserved));
    } else {
        units.return_units(reserved - new_size);
    }
}

} // namespace

reconciler::reconciler(
  source_reader* source,
  schema::registry* destination,
  ss::noncopyable_function<bool(const ppsr::context_subject&)> in_scope,
  const context_mapper& mapper,
  limits lim,
  model::schema_registry_sync_config::unsupported_feature_policy feature_policy)
  : _source(source)
  , _destination(destination)
  , _in_scope(std::move(in_scope))
  , _mapper(&mapper)
  , _limits(lim)
  , _feature_policy(feature_policy)
  , _mem(std::max<size_t>(1, lim.memory_bytes), "schema_registry_sync/memory") {
    // Floor the budget so the clamp `min(body_size, memory_bytes)` and the
    // semaphore agree; a degenerate 0 admits one over-budget body at a time.
    _limits.memory_bytes = std::max<size_t>(1, _limits.memory_bytes);
}

reconciler::node_data& reconciler::data(const ppsr::subject_version& n) {
    return _nodes.try_emplace(n).first->second;
}

ss::future<source_result<void>> reconciler::reconcile(
  work_set work,
  chunked_hash_set<ppsr::subject_version> seed_replicated,
  reconcile_stats& stats,
  ss::abort_source& as) {
    _replicated = std::move(seed_replicated);
    _soft_deleted = std::move(work.soft_deleted);
    _nodes.clear();
    _discover_q.clear();
    _import_q.clear();
    _stats = &stats;
    _stats->versions_changed = 0;
    _stats->errors = 0;
    _stats->unsupported_features_removed = 0;
    _outstanding = 0;
    _done = false;
    _fault.reset();
    _exn = nullptr;

    for (auto& key : work.upserts) {
        auto& d = data(key);
        if (d.state == node_state::unseen) {
            d.state = node_state::discovering;
            _discover_q.push_back(key);
            ++_outstanding;
        }
    }

    // Nothing to do: no fibers needed.
    if (_outstanding == 0) {
        _done = true;
        co_return source_result<void>{};
    }

    auto n_workers = std::max<size_t>(1, _limits.parallelism);
    ss::gate gate;
    for (size_t i = 0; i < n_workers; ++i) {
        ssx::spawn_with_gate(gate, [this, &as] { return worker(as); });
    }
    co_await gate.close();

    // Every fetch releases its reserved-and-consumed units before its worker
    // returns, so once the pool has drained the budget is whole again on every
    // exit path. A mismatch means some path leaked units.
    dassert(
      _mem.available_units()
        == static_cast<ssize_t>(std::max<size_t>(1, _limits.memory_bytes)),
      "reconcile leaked memory units: {} available of {}",
      _mem.available_units(),
      std::max<size_t>(1, _limits.memory_bytes));

    if (_exn) {
        std::rethrow_exception(_exn);
    }
    if (_fault.has_value()) {
        co_return std::unexpected(std::move(*_fault));
    }
    // An external abort with no source fault: surface as a clean cancellation
    // so the caller does not record a spurious failure.
    as.check();

    // Defensive: a valid source's reference graph is acyclic, so both queues
    // drain only once every node is done or errored. A malformed cyclic source
    // leaves nodes stuck pending (their in-degree never reaches 0); fail them
    // here for observability.
    for (const auto& [key, d] : _nodes) {
        if (
          d.state == node_state::pending
          || d.state == node_state::discovering) {
            fail(key, false);
        } else {
            vassert(
              d.state == node_state::done || d.state == node_state::errored,
              "reconcile left node {} in non-terminal state {}",
              key.sub,
              static_cast<int>(d.state));
        }
    }

    co_return source_result<void>{};
}

ss::future<> reconciler::worker(ss::abort_source& as) {
    while (true) {
        try {
            co_await _cv.wait([this, &as] {
                return has_work() || _done || as.abort_requested();
            });
            if (_done || as.abort_requested()) {
                co_return;
            }
            // The cv may release several waiters for a single enqueue; only the
            // one that finds work proceeds, the rest loop back to wait.
            if (!has_work()) {
                continue;
            }

            // Pop one node. It moves from queued to in-flight but stays counted
            // in `_outstanding` until this iteration's `discover`/`do_import`
            // (incl. wake/fail) returns, so a mid-flight worker can never
            // trigger premature-done.
            bool discovering = !_discover_q.empty();
            ppsr::subject_version n = discovering ? _discover_q.back()
                                                  : _import_q.back();
            if (discovering) {
                _discover_q.pop_back();
            } else {
                _import_q.pop_back();
            }

            auto res = discovering ? co_await discover(n, as)
                                   : co_await do_import(n, as);
            if (!res.has_value()) {
                if (!_fault.has_value()) {
                    _fault = std::move(res.error());
                }
                _done = true;
                _cv.broadcast();
                co_return;
            }

            // This node is no longer in-flight. `discover`/`wake` may have
            // enqueued new nodes (each bumping `_outstanding` and signalling);
            // the run is complete only when nothing is queued or in-flight.
            if (--_outstanding == 0) {
                _done = true;
                _cv.broadcast();
                co_return;
            }
        } catch (...) {
            auto eptr = std::current_exception();
            // Abort / teardown unblocks the waits; exit quietly and let the
            // caller observe the abort. Any other exception faults the sync.
            if (!ssx::is_shutdown_exception(eptr)) {
                if (!_exn) {
                    _exn = eptr;
                }
            }
            _done = true;
            _cv.broadcast();
            co_return;
        }
    }
}

ss::future<source_result<void>>
reconciler::discover(const ppsr::subject_version& n, ss::abort_source& as) {
    // Best effort memory management: reserve some overestimate of the typical
    // schema body size before the read, then top up to the real size after.
    // This provides a backpressure point so that a large schema body does not
    // let the reconciler run out of memory. Note that we can allocate more than
    // the limit if the body a single schema or the schemas fetched in parallel
    // exceed the limit.
    auto units = co_await ss::get_units(
      _mem, std::min(reconcile_reserve_bytes, _limits.memory_bytes), as);

    auto fetched = co_await _source->read_subject_version(n.sub, n.version, as);
    if (!fetched.has_value()) {
        if (fetched.error().kind == source_error_kind::source_unavailable) {
            co_return std::unexpected(std::move(fetched.error()));
        }
        fail(n);
        co_return source_result<void>{};
    }

    adjust_units(_mem, units, body_size(fetched.value().schema));

    auto refs = resolve_refs(fetched.value().schema);
    chunked_hash_set<ppsr::subject_version> missing;
    for (auto& ref : refs) {
        if (!_in_scope(ref.sub)) {
            vlog(
              cllog.warn,
              "Schema reference {}/{} of {}/{} is out of scope; cannot "
              "replicate referrer",
              ref.sub,
              ref.version,
              n.sub,
              n.version);
            fail(n);
            co_return source_result<void>{};
        }
        if (!_replicated.contains(ref)) {
            missing.insert(ref);
        }
    }

    if (missing.empty()) {
        co_await import_body(n, std::move(fetched.value()));
        co_return source_result<void>{};
    }

    // Reject under FAIL before queuing references, so a schema we are going to
    // reject does not drag its dependency subtree into the destination.
    if (fail_if_contains_unsupported(n, fetched.value().unsupported)) {
        co_return source_result<void>{};
    }

    // The body is released here (fetched goes out of scope): the node will be
    // re-fetched once its references complete, leading to it being fetched
    // twice in this case; and at most twice on average overall.
    auto& d = data(n);
    d.state = node_state::pending;
    d.in_deg = static_cast<uint32_t>(missing.size());
    for (auto& ref : missing) {
        auto& rd = data(ref);
        if (rd.state == node_state::errored) {
            // A referent already failed; the referrer can never import.
            fail(n);
            co_return source_result<void>{};
        }
        rd.dependents.push_back(n);
        vassert(
          std::ranges::contains(
            std::to_array(
              {node_state::unseen,
               node_state::discovering,
               node_state::pending,
               node_state::importing}),
            rd.state),
          "referent {} in unexpected state {}",
          ref.sub,
          static_cast<int>(rd.state));
        if (rd.state == node_state::unseen) {
            // Only an unseen referent needs to be queued.
            rd.state = node_state::discovering;
            _discover_q.push_back(ref);
            ++_outstanding;
            _cv.signal();
        }
    }
    co_return source_result<void>{};
}

ss::future<source_result<void>>
reconciler::do_import(const ppsr::subject_version& n, ss::abort_source& as) {
    // Same reserve-then-consume model as discover. An import node has no unmet
    // deps: it waits on nothing while holding these units, so the byte
    // semaphore cannot hold-and-wait (deadlock-free).
    auto units = co_await ss::get_units(
      _mem, std::min(reconcile_reserve_bytes, _limits.memory_bytes), as);

    auto fetched = co_await _source->read_subject_version(n.sub, n.version, as);
    if (!fetched.has_value()) {
        if (fetched.error().kind == source_error_kind::source_unavailable) {
            co_return std::unexpected(std::move(fetched.error()));
        }
        fail(n);
        co_return source_result<void>{};
    }

    adjust_units(_mem, units, body_size(fetched.value().schema));

    co_await import_body(n, std::move(fetched.value()));
    co_return source_result<void>{};
}

bool reconciler::fail_if_contains_unsupported(
  const ppsr::subject_version& n,
  const chunked_vector<ppsr::unsupported_feature>& unsupported) {
    if (
      _feature_policy
        != model::schema_registry_sync_config::unsupported_feature_policy::fail
      || unsupported.empty()) {
        return false;
    }
    // The FAIL policy refuses to import a schema the destination cannot store
    // faithfully, but this is a per-item failure, not a whole-sync abort: count
    // it, log the offending fields, and let the rest of the subjects replicate.
    vlog(
      cllog.warn,
      "{}/{} carries {} unsupported schema feature(s) the destination cannot "
      "store; failing it under the FAIL policy: {}",
      n.sub,
      n.version,
      unsupported.size(),
      fmt::join(unsupported, ", "));
    fail(n);
    return true;
}

void reconciler::count_if_contains_unsupported_removed(
  const ppsr::subject_version& n,
  const chunked_vector<ppsr::unsupported_feature>& unsupported) {
    if (
      _feature_policy
        != model::schema_registry_sync_config::unsupported_feature_policy::
          remove
      || unsupported.empty()) {
        return;
    }
    vlog(
      cllog.info,
      "Removed {} unsupported schema feature(s) from {}/{}: {}",
      unsupported.size(),
      n.sub,
      n.version,
      fmt::join(unsupported, ", "));
    _stats->unsupported_features_removed += unsupported.size();
}

ss::future<bool> reconciler::import_body(
  const ppsr::subject_version& n, ppsr::source_schema_read read) {
    // Authoritative policy gate, next to the REMOVE handling below: every
    // import path funnels through here (discover also rejects early, before
    // queuing refs).
    if (fail_if_contains_unsupported(n, read.unsupported)) {
        co_return false;
    }
    data(n).state = node_state::importing;
    // into_stored() consumes `read`, so lift out the unsupported-feature list
    // first: it is accounted for only after a successful import below.
    const auto unsupported = std::move(read.unsupported);
    // Materialize the stored schema, preferring the source's own reported
    // deleted flag: it is fresher (read on a later call than the listing) and
    // avoids the listing set-difference, which can race. Fall back to the
    // caller's active-vs-deleted listing partition only when the source omitted
    // the flag, as a source that does not report it cannot otherwise convey a
    // soft-delete.
    auto stored = std::move(read).into_stored(
      _soft_deleted.contains(n) ? ppsr::is_deleted::yes : ppsr::is_deleted::no);
    // The graph key `n` stays in the source namespace; only the schema written
    // to the destination is remapped.
    auto remapped = remap_for_import(*_mapper, std::move(stored));
    if (!remapped.has_value()) {
        vlog(
          cllog.warn,
          "Cannot replicate {}/{}: a context has no destination mapping",
          n.sub,
          n.version);
        fail(n);
        co_return false;
    }
    auto fut = co_await ss::coroutine::as_future(
      _destination->import_schema(std::move(*remapped)));
    if (fut.failed()) {
        auto eptr = fut.get_exception();
        if (ssx::is_shutdown_exception(eptr)) {
            // Propagate the shutdown/abort to the worker pool so it can exit
            // cleanly.
            std::rethrow_exception(eptr);
        }
        try {
            std::rethrow_exception(eptr);
        } catch (const std::exception& e) {
            vlog(
              cllog.warn,
              "Failed to import schema {}/{}: {}",
              n.sub,
              n.version,
              e.what());
        }
        fail(n);
        co_return false;
    }
    data(n).state = node_state::done;
    ++_stats->versions_changed;
    // Account for REMOVE-stripped features only after the projection actually
    // lands on the destination, so a failed import does not report features as
    // removed.
    count_if_contains_unsupported_removed(n, unsupported);
    wake(n);
    co_return true;
}

void reconciler::wake(const ppsr::subject_version& n) {
    _replicated.insert(n);
    auto dependents = std::move(data(n).dependents);
    for (const auto& w : dependents) {
        auto& wd = data(w);
        if (wd.state != node_state::pending) {
            continue;
        }
        wd.in_deg = std::max(wd.in_deg, 1u) - 1;
        if (wd.in_deg == 0) {
            wd.state = node_state::importing;
            _import_q.push_back(w);
            ++_outstanding;
            _cv.signal();
        }
    }
}

void reconciler::fail(const ppsr::subject_version& n, bool recurse) {
    auto& d = data(n);
    if (d.state == node_state::errored) {
        return;
    }
    d.state = node_state::errored;
    ++_stats->errors;
    vlog(cllog.warn, "Failed to replicate schema {}/{}", n.sub, n.version);
    if (!recurse) {
        return;
    }
    // failed the node's dependents transitively, since they can never satisfy
    // their references
    auto dependents = std::move(d.dependents);
    for (const auto& w : dependents) {
        fail(w);
    }
}

} // namespace cluster_link::schema_registry_sync
