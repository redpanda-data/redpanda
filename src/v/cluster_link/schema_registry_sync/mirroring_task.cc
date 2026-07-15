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

#include "cluster_link/schema_registry_sync/mirroring_task.h"

#include "cluster_link/link.h"
#include "cluster_link/schema_registry_sync/reconciler.h"
#include "cluster_link/schema_registry_sync/scope.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/namespace.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/types.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <fmt/ranges.h>

#include <utility>

namespace cluster_link::schema_registry_sync {

namespace {

ss::lowres_clock::duration
tail_interval(const model::schema_registry_sync_config& cfg) {
    if (const auto* api = cfg.api_mode(); api != nullptr) {
        return api->get_tail_interval();
    }
    return model::schema_registry_sync_config::shadow_schema_registry_api::
      default_tail_interval;
}

ss::lowres_clock::duration
full_sync_interval(const model::schema_registry_sync_config& cfg) {
    if (const auto* api = cfg.api_mode(); api != nullptr) {
        return api->get_full_sync_interval();
    }
    return model::schema_registry_sync_config::shadow_schema_registry_api::
      default_full_sync_interval;
}

// A hard-delete blocked by a live reference; the only failure the purge loop
// retries (after the referencing version is deleted first).
bool is_reference_blocked(const std::exception_ptr& ep) {
    try {
        std::rethrow_exception(ep);
    } catch (const ppsr::exception& e) {
        return e.code() == ppsr::error_code::subject_version_has_references;
    } catch (...) {
    }
    return false;
}

} // namespace

ss::future<inventory> scan_destination_inventory(
  schema::registry& destination,
  ss::noncopyable_function<bool(const ppsr::context_subject&)> in_scope,
  const context_mapper& mapper,
  ss::abort_source& as) {
    as.check();
    // A destination node is in scope iff its context reverse-maps to a source
    // context `in_scope` accepts (identity mapper: reduces to `in_scope`).
    // Captured by reference like the underlying filter; both outlive the scan.
    auto dest_in_scope = [&in_scope,
                          &mapper](const ppsr::context_subject& dest_cs) {
        auto src_ctx = mapper.reverse(dest_cs.ctx);
        return src_ctx.has_value()
               && in_scope(ppsr::context_subject{*src_ctx, dest_cs.sub});
    };
    // list_subject_versions reads the store as-is; sync first so the scan
    // isn't stale (e.g. on a freshly-elected _schemas/0 leader).
    co_await destination.sync();
    auto versions = co_await destination.list_subject_versions(
      std::move(dest_in_scope), ppsr::include_deleted::yes);
    inventory inv;
    inv.all.reserve(versions.size());
    for (const auto& sv : versions) {
        // Back to the source namespace, so the diff/seed/purge phases speak
        // one.
        auto src_ctx = mapper.reverse(sv.sub.ctx);
        if (!src_ctx.has_value()) {
            continue;
        }
        auto node = ppsr::subject_version{
          ppsr::context_subject{*src_ctx, sv.sub.sub}, sv.version};
        if (sv.deleted == ppsr::is_deleted::no) {
            inv.active.insert(node);
        }
        inv.all.insert(std::move(node));
    }
    co_return inv;
}

mirroring_task::mirroring_task(
  link* link,
  const model::metadata& link_metadata,
  schema::registry* destination,
  source_reader_factory* source_factory)
  : task(
      link,
      tail_interval(link_metadata.configuration.schema_registry_sync_cfg),
      mirroring_task::task_name)
  , _config(link_metadata.configuration.schema_registry_sync_cfg.copy())
  , _destination(destination)
  , _source_factory(source_factory)
  , _reader(_source_factory->create(_config.api_mode())) {}

void mirroring_task::update_config(const model::metadata& link_metadata) {
    _config = link_metadata.configuration.schema_registry_sync_cfg.copy();
    set_run_interval(tail_interval(_config));
    // The scope (filters/contexts) may have changed; flag a forced full scan so
    // the next run re-derives the inventory. Only a flag is set here: mutating
    // _status/_last_full_sync would race an in-flight run_impl that resumes and
    // overwrites it.
    _config_changed = true;
}

void mirroring_task::reset_sync_state() {
    _status = model::schema_registry_sync_status{};
    _destination_inventory = inventory{};
    _reconcile_stats = reconcile_stats{};
    _mapper = context_mapper{};
    _last_full_sync.reset();
}

ss::future<cl_result<void>> mirroring_task::start() {
    auto res = co_await task::start();
    // The series stay registered for the task's whole non-stopped life,
    // including while paused; stop() removes them. See probe.h for the
    // lifecycle rationale. setup is idempotent, so resuming from paused
    // (which also lands here) is fine.
    if (res.has_value()) {
        _probe.setup(get_link()->get_config()->name, [this] {
            return get_live_sync_status().totals_since_task_start;
        });
    }
    co_return res;
}

ss::future<cl_result<void>> mirroring_task::stop() noexcept {
    // Stop the reader BEFORE joining the run fiber: run_impl can be parked on
    // reader-internal waits that only the reader's own shutdown aborts (the
    // rate limiter's token queue and Retry-After pause -- up to 60s -- and the
    // connection pool's slot queue are deaf to the runner's abort source).
    // Joining first would sequence that abort behind the join that needs it.
    // The reader is built to be stopped under fire: in-flight and queued
    // requests fail promptly with abort-classified errors and run_impl
    // unwinds. as_future guards the noexcept contract.
    //
    // Under _reader_lifecycle: run_impl is still live here (reader-first), so a
    // concurrent reset_reader() could otherwise free the reader while this
    // stop() is suspended mid-shutdown. The lock is dropped before the join
    // below, so it cannot deadlock against a reset_reader() the join waits on.
    {
        auto reader_units = co_await _reader_lifecycle.get_units();
        if (_reader) {
            auto stopped = co_await ss::coroutine::as_future(_reader->stop());
            if (stopped.failed()) {
                auto ex = stopped.get_exception();
                vlog(
                  logger().warn,
                  "Error stopping Schema Registry source reader: {}",
                  ex);
            }
        }
    }
    auto res = co_await task::stop();
    _probe.clear();
    // task::stop() closed the runner's gate, so no run_impl is in flight and
    // it is safe to reset the state directly (unlike update_config, which
    // races a running fiber and defers via _config_changed). Reset so a later
    // leader starts fresh: this instance may regain _schemas/0 leadership
    // (A->B->A) and would otherwise report a prior tenure's stale
    // counters/inventory and skip its first full sync on a still-recent
    // _last_full_sync.
    reset_sync_state();
    // Replace the stopped reader with a fresh one for that possible A->B->A
    // re-acquisition. The reader's stop() is permanent by design (it refuses
    // to rebuild its client so an unwinding fiber cannot resurrect it during
    // teardown), and run_impl only rebuilds the reader on a config change, so
    // a restarted task would otherwise keep using the dead reader and never
    // sync. No lock needed: the run fibers have been joined, so reset_reader()
    // cannot run and none is mid-request on the old reader.
    _reader = _source_factory->create(_config.api_mode());
    co_return res;
}

ss::future<> mirroring_task::reset_reader() {
    // Serialize with stop() (see _reader_lifecycle): both stop and then free
    // the reader by reassignment, and either can be suspended in the reader's
    // shutdown when the other reaches the free.
    auto reader_units = co_await _reader_lifecycle.get_units();
    if (_reader) {
        auto stopped = co_await ss::coroutine::as_future(_reader->stop());
        if (stopped.failed()) {
            auto ex = stopped.get_exception();
            vlog(
              logger().warn,
              "Error stopping previous Schema Registry source reader: {}",
              ex);
        }
    }
    _reader = _source_factory->create(_config.api_mode());
}

model::enabled_t mirroring_task::is_enabled() const {
    const auto* api = _config.api_mode();
    return model::enabled_t(api != nullptr && bool(api->is_enabled));
}

bool mirroring_task::leads_schema_registry_partition() const {
    return get_link()->partition_manager().is_current_shard_leader(
      ::model::schema_registry_internal_ntp);
}

bool mirroring_task::should_start_impl(ss::shard_id, ::model::node_id) const {
    return leads_schema_registry_partition();
}

bool mirroring_task::should_stop_impl(ss::shard_id, ::model::node_id) const {
    return !leads_schema_registry_partition();
}

bool mirroring_task::should_long_sync() const {
    if (!_last_full_sync.has_value()) {
        return true;
    }
    return ss::lowres_clock::now() - *_last_full_sync
           >= full_sync_interval(_config);
}

model::schema_registry_sync_status
mirroring_task::get_live_sync_status() const {
    auto status = _status;
    // Reflect the in-flight reconcile's live counters for mid-sync
    // progress. Guarded on current_sync so it cannot double-count after the
    // fold (which zeroes _reconcile_stats and bakes them into _status).
    if (status.current_sync.has_value()) {
        status.current_sync->summary.subject_versions_changed
          += _reconcile_stats.versions_changed;
        status.current_sync->summary.errors += _reconcile_stats.errors;
        status.current_sync->summary.unsupported_features_removed
          += _reconcile_stats.unsupported_features_removed;
        status.totals_since_task_start.subject_versions_changed
          += _reconcile_stats.versions_changed;
        status.totals_since_task_start.errors += _reconcile_stats.errors;
        status.totals_since_task_start.unsupported_features_removed
          += _reconcile_stats.unsupported_features_removed;
    }
    return status;
}

model::task_status_report mirroring_task::get_status_report() const {
    auto report = task::get_status_report();
    // Only the shard leading _schemas/0 runs the sync; a stopped shard's empty
    // status must not win the admin aggregation over the leader's, so suppress
    // it.
    if (get_state() != model::task_state::stopped) {
        report.detail = model::task_detail{
          .schema_registry_sync_status = get_live_sync_status()};
    }
    return report;
}

ss::future<> mirroring_task::refresh_destination_inventory(
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>& in_scope,
  ss::abort_source& as) {
    // Destination/internal faults bubble out and become `faulted`. The scan
    // sinks the predicate by value; forward the borrowed one.
    _destination_inventory = co_await scan_destination_inventory(
      *_destination,
      [&in_scope](const ppsr::context_subject& cs) { return in_scope(cs); },
      _mapper,
      as);

    chunked_hash_set<ppsr::context_subject> subjects;
    for (const auto& key : _destination_inventory.active) {
        subjects.insert(key.sub);
    }
    _status.inventory.destination_subjects = static_cast<uint64_t>(
      subjects.size());
    _status.inventory.destination_subject_versions = static_cast<uint64_t>(
      _destination_inventory.active.size());
}

ss::future<> mirroring_task::hard_delete_target(
  const ppsr::context_subject& dest_sub,
  ppsr::schema_version version,
  bool was_active) {
    if (was_active) {
        co_await _destination->soft_delete_schema(dest_sub, version);
    }
    co_await _destination->permanent_delete_schema(dest_sub, version);
}

ss::future<> mirroring_task::purge_one(
  purge_target target,
  ss::abort_source& as,
  uint64_t& purged,
  chunked_vector<purge_target>& next_round) {
    as.check();
    // `node` is source-namespace; forward-map to the destination for the
    // delete.
    auto dest_ctx = _mapper.forward(target.node.sub.ctx);
    if (!dest_ctx.has_value()) {
        record_error(
          fmt::format(
            "cannot hard-delete {}/{}: source context has no destination "
            "mapping",
            target.node.sub,
            target.node.version));
        co_return;
    }
    const auto dest_sub = ppsr::context_subject{*dest_ctx, target.node.sub.sub};
    auto fut = co_await ss::coroutine::as_future(
      hard_delete_target(dest_sub, target.node.version, target.was_active));
    if (fut.failed()) {
        auto ex = fut.get_exception();
        if (ssx::is_shutdown_exception(ex)) {
            std::rethrow_exception(ex);
        }
        if (is_reference_blocked(ex)) {
            // Referenced by a version not yet purged this round; retry later.
            vlog(
              logger().debug,
              "Deferring hard-delete of {}/{}: {}",
              target.node.sub,
              target.node.version,
              ex);
            next_round.push_back(std::move(target));
            co_return;
        }
        record_error(
          fmt::format(
            "failed to hard-delete {}/{}: {}",
            target.node.sub,
            target.node.version,
            ex));
        co_return;
    }
    ++purged;
    ++_status.current_sync->summary.subject_versions_changed;
    ++_status.totals_since_task_start.subject_versions_changed;
}

ss::future<uint64_t> mirroring_task::purge_destination_only_versions(
  chunked_vector<purge_target> targets, ss::abort_source& as) {
    uint64_t purged = 0;
    // Rounds retry reference-blocked deletes (see header); stop when a round
    // makes no progress and count the stuck remainder as errors.
    const auto parallelism = std::max<size_t>(
      1, config::shard_local_cfg().schema_registry_sync_parallelism());
    while (!targets.empty()) {
        const auto before = purged;
        chunked_vector<purge_target> next_round;
        co_await ss::max_concurrent_for_each(
          targets, parallelism, [&](purge_target& t) {
              return purge_one(std::move(t), as, purged, next_round);
          });
        if (purged == before) {
            for (const auto& t : next_round) {
                record_error(
                  fmt::format(
                    "failed to hard-delete {}/{}", t.node.sub, t.node.version));
            }
            break;
        }
        targets = std::move(next_round);
    }
    co_return purged;
}

bool mirroring_task::fail_if_contains_unsupported(
  const ppsr::context_subject& target,
  model::schema_registry_sync_config::unsupported_feature_policy feature_policy,
  const chunked_vector<ppsr::unsupported_feature>& unsupported) {
    using policy_t
      = model::schema_registry_sync_config::unsupported_feature_policy;
    if (feature_policy != policy_t::fail || unsupported.empty()) {
        return false;
    }
    // Per-item failure (as on the schema-body path): count it, log the
    // offending fields, and let the caller skip this subject's config sync
    // while the rest of the work continues.
    record_error(
      fmt::format(
        "{} config carries {} unsupported feature(s) the destination cannot "
        "store; failing it under the FAIL policy: {}",
        target,
        unsupported.size(),
        fmt::join(unsupported, ", ")));
    return true;
}

void mirroring_task::count_if_contains_unsupported_removed(
  const ppsr::context_subject& target,
  model::schema_registry_sync_config::unsupported_feature_policy feature_policy,
  const chunked_vector<ppsr::unsupported_feature>& unsupported) {
    using policy_t
      = model::schema_registry_sync_config::unsupported_feature_policy;
    if (feature_policy != policy_t::remove || unsupported.empty()) {
        return;
    }
    // Only compatibilityLevel is synced, so the unsupported config fields are
    // already dropped; log and count them.
    vlog(
      logger().info,
      "Removed {} unsupported config feature(s) from {}: {}",
      unsupported.size(),
      target,
      fmt::join(unsupported, ", "));
    _status.current_sync->summary.unsupported_features_removed
      += unsupported.size();
    _status.totals_since_task_start.unsupported_features_removed
      += unsupported.size();
}

ss::future<> mirroring_task::sync_mode_and_config(
  const ppsr::context_subject& target,
  model::schema_registry_sync_config::unsupported_feature_policy feature_policy,
  ss::abort_source& as,
  std::optional<source_error>& unavailable) {
    // A peer fiber already hit source_unavailable; skip the remaining work.
    if (unavailable.has_value()) {
        co_return;
    }
    // Read the source in its own (source) namespace; write to the destination
    // under the remapped context. Identity leaves dest_target == target.
    auto dest_ctx = _mapper.forward(target.ctx);
    if (!dest_ctx.has_value()) {
        record_error(
          fmt::format(
            "cannot sync mode/config for {}: source context has no destination "
            "mapping",
            target));
        co_return;
    }
    const auto dest_target = ppsr::context_subject{*dest_ctx, target.sub};
    auto mode = co_await _reader->read_mode(target, as);
    if (!mode.has_value()) {
        if (mode.error().kind == source_error_kind::source_unavailable) {
            if (!unavailable.has_value()) {
                unavailable = std::move(mode.error());
            }
            co_return;
        }
        // Unmappable value or other reachable failure: count and continue.
        record_error(mode.error().message);
    } else {
        // Present -> mirror the override; absent -> remove any destination
        // override
        auto write = co_await ss::coroutine::as_future(
          mode.value().has_value()
            ? _destination->write_mode(dest_target, *mode.value())
            : _destination->delete_mode(dest_target));
        if (write.failed()) {
            auto ex = write.get_exception();
            if (ssx::is_shutdown_exception(ex)) {
                std::rethrow_exception(ex);
            }
            record_error(
              fmt::format("failed to sync mode for {}: {}", dest_target, ex));
        } else if (write.get()) {
            ++_status.current_sync->summary.modes_changed;
            ++_status.totals_since_task_start.modes_changed;
        }
    }

    if (unavailable.has_value()) {
        co_return;
    }
    auto config = co_await _reader->read_config(target, as);
    if (!config.has_value()) {
        if (config.error().kind == source_error_kind::source_unavailable) {
            if (!unavailable.has_value()) {
                unavailable = std::move(config.error());
            }
            co_return;
        }
        record_error(config.error().message);
        co_return;
    }

    auto& cfg = config.value();
    // FAIL rejects before the write; REMOVE accounting runs after it lands.
    // Policy diagnostics name the source subject, write errors the destination.
    if (fail_if_contains_unsupported(target, feature_policy, cfg.unsupported)) {
        co_return;
    }

    auto write = co_await ss::coroutine::as_future(
      cfg.compatibility.has_value()
        ? _destination->write_config(dest_target, *cfg.compatibility)
        : _destination->delete_config(dest_target));
    if (write.failed()) {
        auto ex = write.get_exception();
        if (ssx::is_shutdown_exception(ex)) {
            std::rethrow_exception(ex);
        }
        record_error(
          fmt::format("failed to sync config for {}: {}", dest_target, ex));
        co_return;
    }
    if (write.get()) {
        ++_status.current_sync->summary.compatibility_configs_changed;
        ++_status.totals_since_task_start.compatibility_configs_changed;
    }
    // Counted per completed sync, even when the write was a no-op: the drop
    // recurs on every re-read, and a governance-only config (a no-op delete)
    // must not be silently ignored. Mirrors FAIL's per-sync errors; the schema
    // path counts once because its projection lands durably.
    count_if_contains_unsupported_removed(
      target, feature_policy, cfg.unsupported);
}

void mirroring_task::record_error(std::string_view what) {
    ++_status.current_sync->summary.errors;
    ++_status.totals_since_task_start.errors;
    _status.last_error_message = ss::sstring{what};
    vlog(logger().warn, "Schema Registry sync error: {}", what);
}

ss::future<std::optional<chunked_vector<ppsr::schema_version>>>
mirroring_task::list_versions_once(
  const ppsr::context_subject& subject,
  ppsr::include_deleted include_deleted,
  ss::abort_source& as,
  std::optional<source_error>& unavailable) {
    as.check();
    auto res = co_await _reader->list_subject_versions(
      subject, include_deleted, as);
    if (res.has_value()) {
        co_return std::move(res.value());
    }
    if (res.error().kind == source_error_kind::source_unavailable) {
        if (!unavailable.has_value()) {
            unavailable = std::move(res.error());
        }
        co_return std::nullopt;
    }
    // Not-found means no versions of this kind, not a fault -- how a fully
    // soft-deleted subject's active-only listing reads.
    if (res.error().kind == source_error_kind::subject_not_found) {
        co_return chunked_vector<ppsr::schema_version>{};
    }
    // Reachable but failed (rare delete race): count and skip. Counters are
    // touched only between co_awaits, so sharing them across the concurrent
    // fibers is safe on one reactor.
    record_error(res.error().message);
    co_return std::nullopt;
}

ss::future<> mirroring_task::list_one_subject(
  const ppsr::context_subject& subject,
  ss::abort_source& as,
  chunked_hash_set<ppsr::subject_version>& source_active,
  chunked_hash_set<ppsr::subject_version>& source_deleted,
  chunked_hash_set<ppsr::context_subject>& failed_subjects,
  std::optional<source_error>& unavailable) {
    // A peer fiber already hit source_unavailable; skip the remaining work.
    if (unavailable.has_value()) {
        co_return;
    }
    // A nullopt from a reachable error (not source_unavailable, which sets
    // `unavailable`) leaves this subject's versions undiscovered, so exclude it
    // from the hard-delete: its destination versions must not be read as
    // source-absent.
    // List all versions first: unlike the active-only listing, it succeeds
    // for a fully soft-deleted subject. The two listings partition versions
    // into active vs. soft-deleted.
    auto all = co_await list_versions_once(
      subject, ppsr::include_deleted::yes, as, unavailable);
    if (!all.has_value()) {
        failed_subjects.insert(subject);
        co_return;
    }
    auto active = co_await list_versions_once(
      subject, ppsr::include_deleted::no, as, unavailable);
    if (!active.has_value()) {
        failed_subjects.insert(subject);
        co_return;
    }
    chunked_hash_set<ppsr::schema_version> active_set;
    for (auto version : *active) {
        active_set.insert(version);
        source_active.insert(ppsr::subject_version{subject, version});
    }
    for (auto version : *all) {
        if (!active_set.contains(version)) {
            source_deleted.insert(ppsr::subject_version{subject, version});
        }
    }
}

ss::future<task::state_transition> mirroring_task::full_source_sync(
  ss::abort_source& as,
  const chunked_hash_set<ppsr::context>& contexts,
  model::schema_registry_sync_config::unsupported_feature_policy feature_policy,
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
    in_scope) {
    // Cluster-global, so safe to read mid-sync (unlike the per-link config a
    // concurrent update_config can swap). The one parallelism bound governs
    // both the version-listing fan-out and the reconcile's import concurrency.
    auto limits = reconciler::limits{
      .memory_bytes
      = config::shard_local_cfg().schema_registry_sync_memory_bytes(),
      .parallelism
      = config::shard_local_cfg().schema_registry_sync_parallelism()};

    // Discover the source nodes; the reconciler fetches the bodies. Active and
    // soft-deleted are tracked apart so the work-set diff can treat them
    // differently (see below).
    chunked_hash_set<ppsr::subject_version> source_active;
    chunked_hash_set<ppsr::subject_version> source_deleted;

    // Track failed context and subject listing calls to narrow the hard-delete
    // purge to only what discovery saw whole and avoid hard-deleting subjects
    // based on incomplete discovery.
    chunked_hash_set<ppsr::context> failed_contexts;
    chunked_hash_set<ppsr::context_subject> failed_subjects;

    // Contexts are few, so enumerate their subjects sequentially. in_scope also
    // scopes discovery, keeping source and destination sides consistent.
    chunked_vector<ppsr::context_subject> subjects;
    for (const auto& ctx : contexts) {
        auto subjects_res = co_await _reader->list_subjects(ctx, as);
        if (!subjects_res.has_value()) {
            if (
              subjects_res.error().kind
              == source_error_kind::source_unavailable) {
                co_return make_unavailable(subjects_res.error().message);
            }
            // Reachable but failed (rare delete race): count and skip, and
            // spare this context's destination subjects from the purge.
            record_error(subjects_res.error().message);
            failed_contexts.insert(ctx);
            continue;
        }
        for (auto& subject : subjects_res.value()) {
            if (in_scope(subject)) {
                subjects.push_back(std::move(subject));
            }
        }
    }
    _status.inventory.selected_source_subjects = subjects.size();

    // Bounded concurrency over independent round-trips. list_one_subject is a
    // member, not a coroutine lambda (see its declaration), so the forwarding
    // lambda only returns its future.
    std::optional<source_error> unavailable;
    co_await ss::max_concurrent_for_each(
      subjects,
      std::max<size_t>(1, limits.parallelism),
      [&](const ppsr::context_subject& subject) {
          return list_one_subject(
            subject,
            as,
            source_active,
            source_deleted,
            failed_subjects,
            unavailable);
      });
    if (unavailable.has_value()) {
        co_return make_unavailable(unavailable->message);
    }

    // Every discovered source version is selected for sync, soft-deleted ones
    // included (they are imported below), so count both -- mirroring
    // selected_source_subjects, a discovery count rather than a change count.
    _status.inventory.selected_source_subject_versions = static_cast<uint64_t>(
      source_active.size() + source_deleted.size());

    // State-aware. An active source version missing from the destination's
    // active set is imported (creating it, or reactivating one that is
    // soft-deleted on the destination -- the seed does not suppress work
    // items). A soft-deleted source version is imported unless it is already
    // soft-deleted on the destination: an absent version is imported
    // soft-deleted, and one still active on the destination has its deleted
    // body re-imported to propagate the soft-delete (import overwrites the
    // version's deleted flag). Soft-delete propagation covers source-present
    // versions in both directions; a version present on the destination but
    // absent from the source entirely is hard-deleted below. Detecting
    // divergent same-key content is out of scope (a matching key is assumed to
    // mean matching content -- the destination is a managed mirror).
    work_set work;
    for (const auto& node : source_active) {
        if (!_destination_inventory.active.contains(node)) {
            work.upserts.push_back(node);
        }
    }
    for (const auto& node : source_deleted) {
        const bool dest_deleted = _destination_inventory.all.contains(node)
                                  && !_destination_inventory.active.contains(
                                    node);
        if (!dest_deleted) {
            work.upserts.push_back(node);
            // Listing-derived soft-delete, recorded as the fallback signal: the
            // reconciler prefers the version body's own `deleted` flag and
            // consults this set only when the source omits it from the body.
            work.soft_deleted.insert(node);
        }
    }

    // Hard-delete set: destination versions the source no longer has at all.
    chunked_vector<purge_target> to_purge;
    for (const auto& node : _destination_inventory.all) {
        if (source_active.contains(node) || source_deleted.contains(node)) {
            continue;
        }
        if (
          failed_contexts.contains(node.sub.ctx)
          || failed_subjects.contains(node.sub)) {
            continue;
        }
        to_purge.push_back(
          purge_target{
            .node = node,
            .was_active = _destination_inventory.active.contains(node)});
    }

    // The reconciler sinks the predicate by value; forward the borrowed one.
    // The mapper remaps contexts at the import boundary only.
    auto rec = reconciler{
      _reader.get(),
      _destination,
      [&in_scope](const ppsr::context_subject& cs) { return in_scope(cs); },
      _mapper,
      limits,
      feature_policy};

    // The reconciler increments _reconcile_stats live (reflected mid-sync by
    // get_status_report); the fold below moves them into persistent state.
    _reconcile_stats = reconcile_stats{};
    // Seed with the full (active + soft-deleted) set: soft-deleted nodes still
    // satisfy references. reconcile sinks it by value and
    // _destination_inventory is rebuilt next run, so move `all` in rather than
    // copy it.
    auto result = co_await rec.reconcile(
      std::move(work),
      std::move(_destination_inventory.all),
      _reconcile_stats,
      as);
    if (!result.has_value()) {
        if (result.error().kind == source_error_kind::source_unavailable) {
            co_return make_unavailable(result.error().message);
        }
        // reconcile only surfaces source_unavailable today; treat any other
        // error defensively as a counted per-item failure.
        record_error(result.error().message);
        co_return make_active();
    }

    const auto stats = _reconcile_stats;
    // Fold once into persistent state, then clear so the report-time reflection
    // cannot double-count.
    _reconcile_stats = reconcile_stats{};
    _status.current_sync->summary.subject_versions_changed
      += stats.versions_changed;
    _status.current_sync->summary.errors += stats.errors;
    _status.current_sync->summary.unsupported_features_removed
      += stats.unsupported_features_removed;
    _status.totals_since_task_start.subject_versions_changed
      += stats.versions_changed;
    _status.totals_since_task_start.errors += stats.errors;
    _status.totals_since_task_start.unsupported_features_removed
      += stats.unsupported_features_removed;

    const auto purged = co_await purge_destination_only_versions(
      std::move(to_purge), as);

    // Replicate modes/configs after the import (the destination contexts and
    // subjects now exist)
    chunked_vector<ppsr::context_subject> mode_config_targets;

    // The registry-wide global mode/config (GET /mode/:.__GLOBAL:) is synced
    // only when in scope.
    const auto global = ppsr::context_subject{
      ppsr::global_context, ppsr::subject{""}};
    if (in_scope(global)) {
        mode_config_targets.push_back(global);
    }
    for (const auto& ctx : contexts) {
        // A context in scope only via a subject filter is not in scope as a
        // whole, so its context-level mode/config must not be touched.
        ppsr::context_subject ctx_target{ctx, ppsr::subject{""}};
        if (in_scope(ctx_target)) {
            mode_config_targets.push_back(std::move(ctx_target));
        }
    }
    for (const auto& subject : subjects) {
        mode_config_targets.push_back(subject);
    }
    std::optional<source_error> mc_unavailable;
    co_await ss::max_concurrent_for_each(
      mode_config_targets,
      std::max<size_t>(1, limits.parallelism),
      [&](const ppsr::context_subject& target) {
          return sync_mode_and_config(
            target, feature_policy, as, mc_unavailable);
      });
    if (mc_unavailable.has_value()) {
        co_return make_unavailable(mc_unavailable->message);
    }

    // Re-scan now that imports have landed so the reported destination counts
    // reflect the post-sync state, not the pre-import baseline the diff used.
    co_await refresh_destination_inventory(in_scope, as);

    vlog(
      logger().info,
      "Schema Registry full sync: {} source subjects ({} versions), {} "
      "destination subjects; imported {} versions, hard-deleted {} versions, "
      "{} modes, {} configs, {} errors",
      _status.inventory.selected_source_subjects,
      _status.inventory.selected_source_subject_versions,
      _status.inventory.destination_subjects,
      stats.versions_changed,
      purged,
      _status.current_sync->summary.modes_changed,
      _status.current_sync->summary.compatibility_configs_changed,
      _status.current_sync->summary.errors);

    _status.current_sync->summary.finish_time = ::model::timestamp::now();
    _status.last_full_sync = _status.current_sync->summary;
    // Completed (best-effort, per-item failures counted), so advance the timer
    // and retry on the normal interval.
    _last_full_sync = ss::lowres_clock::now();
    co_return make_active();
}

ss::future<task::state_transition>
mirroring_task::run_impl(ss::abort_source& as) {
    // Stamp the cumulative summary's start time once per leadership acquisition
    // (the proto documents totals_since_task_start.start_time as the task
    // start). stop() clears the status on losing leadership, so the next leader
    // -- a new instance or this same one regaining it -- re-stamps it here.
    if (!_status.totals_since_task_start.start_time.has_value()) {
        _status.totals_since_task_start.start_time = ::model::timestamp::now();
    }

    // Consume the config-changed flag before any co_await so a concurrent
    // update_config during this run is not lost (it re-arms for the next run).
    const bool config_changed = std::exchange(_config_changed, false);
    const bool long_sync = config_changed || should_long_sync();

    // A config change may have altered the source connection (URL, auth, TLS),
    // so rebuild the reader before this run reads from the source.
    if (config_changed) {
        co_await reset_reader();
    }

    _status.current_sync = model::schema_registry_current_sync{
      .sync_type = long_sync ? model::schema_registry_sync_type::full
                             : model::schema_registry_sync_type::tail,
      .summary = {.start_time = ::model::timestamp::now()}};
    // current_sync reflects an in-progress sync only; clear it on every exit
    // (success, unavailable, or a fault that throws out of run_impl) so a stale
    // partial summary is never reported between runs.
    auto clear_current_sync = ss::defer(
      [this] { _status.current_sync.reset(); });

    if (!long_sync) {
        // Incremental tail sync is not implemented yet; nothing to do on a
        // tail tick (in particular, do not rescan the destination).
        vlog(logger().debug, "Schema Registry tail sync not yet implemented");
        co_return make_active();
    }

    // Contexts to replicate = source contexts intersected with the filter.
    auto contexts_res = co_await _reader->list_contexts(as);
    if (!contexts_res.has_value()) {
        if (
          contexts_res.error().kind == source_error_kind::source_unavailable) {
            co_return make_unavailable(contexts_res.error().message);
        }
        record_error(contexts_res.error().message);
        co_return make_active();
    }
    // Filter has union semantics: filter.contexts selects whole contexts,
    // filter.subjects adds individual qualified subjects (each carrying its own
    // context), and an empty filter replicates everything.
    const auto qualified = ppsr::qualified_subjects_enabled{
      config::shard_local_cfg().schema_registry_enable_qualified_subjects()};
    chunked_hash_set<ppsr::context> filter_contexts;
    chunked_hash_set<ppsr::context_subject> filter_subjects;
    const auto* api = _config.api_mode();
    if (api == nullptr) {
        vlog(
          logger().debug,
          "Schema Registry sync disabled mid-run; skipping remainder of sync");
        co_return make_active();
    }
    for (const auto& ctx : api->filter.contexts) {
        filter_contexts.insert(ppsr::context{ctx});
    }
    for (const auto& sub : api->filter.subjects) {
        filter_subjects.insert(
          ppsr::context_subject::from_string(sub, qualified));
    }
    const bool unfiltered = filter_contexts.empty() && filter_subjects.empty();
    // A filtered subject's context must be scanned even when the context filter
    // omits it, else that subject is never discovered.
    chunked_hash_set<ppsr::context> subject_contexts;
    for (const auto& cs : filter_subjects) {
        subject_contexts.insert(cs.ctx);
    }
    chunked_hash_set<ppsr::context> contexts;
    for (auto& ctx : contexts_res.value()) {
        if (
          unfiltered || filter_contexts.contains(ctx)
          || subject_contexts.contains(ctx)) {
            contexts.insert(std::move(ctx));
        }
    }

    if (
      auto reason = check_preconditions(
        _config,
        contexts,
        config::shard_local_cfg().schema_registry_enable_qualified_subjects());
      reason.has_value()) {
        co_return make_faulted(*reason);
    }

    // in_scope is move-only (its filter sets are not copyable) but two by-value
    // sinks consume it per run (the destination scan and the reconciler), so
    // build it once and lend it by reference; each sink forwards a thin
    // wrapper.
    auto in_scope = make_in_scope(
      std::move(filter_contexts), std::move(filter_subjects));

    // Rebuild once per run from the validated config; held on the task so every
    // phase shares one mapping without re-reading _config (which a concurrent
    // update_config could swap mid-run).
    _mapper = context_mapper::make(_config);
    // Snapshot the per-link policy while `api` is known non-null. A concurrent
    // update_config can swap `_config` across the co_awaits below (and inside
    // full_source_sync), so it must not re-read api_mode() after suspending.
    const auto feature_policy = api->feature_policy;

    co_await refresh_destination_inventory(in_scope, as);
    co_return co_await full_source_sync(as, contexts, feature_policy, in_scope);
}

task::state_transition
mirroring_task::make_unavailable(const ss::sstring& reason) {
    vlog(
      logger().warn, "Schema Registry shadowing task unavailable: {}", reason);
    _status.last_error_message = reason;
    // No special backoff: an unavailable run leaves _last_full_sync unadvanced,
    // so the next tail tick re-attempts the full sync and the link recovers on
    // the normal cadence once the source comes back.
    return state_transition{
      .desired_state = model::task_state::link_unavailable, .reason = reason};
}

task::state_transition mirroring_task::make_active() {
    return state_transition{
      .desired_state = model::task_state::active,
      .reason = "Schema Registry shadowing task finished a sync"};
}

task::state_transition mirroring_task::make_faulted(const ss::sstring& reason) {
    vlog(logger().warn, "Schema Registry shadowing task faulted: {}", reason);
    _status.last_error_message = reason;
    return state_transition{
      .desired_state = model::task_state::faulted, .reason = reason};
}

std::string_view mirroring_task_factory::created_task_name() const noexcept {
    return mirroring_task::task_name;
}

std::unique_ptr<task> mirroring_task_factory::create_task(link* link) {
    return std::make_unique<mirroring_task>(
      link, *(link->get_config()), _destination, _source_factory);
}

} // namespace cluster_link::schema_registry_sync
