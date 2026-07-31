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

#include <array>
#include <ranges>
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

// The resource bounds for a sync. Cluster-global, so safe to read mid-sync
// (unlike the per-link config a concurrent update_config can swap). The one
// parallelism bound governs the version-listing fan-out, the reconcile's import
// concurrency and the mode/config fan-out alike.
reconciler::limits sync_limits() {
    return reconciler::limits{
      .memory_bytes
      = config::shard_local_cfg().schema_registry_sync_memory_bytes(),
      .parallelism
      = config::shard_local_cfg().schema_registry_sync_parallelism()};
}

// Stops one reader, logging rather than propagating a failure: a reader that
// cannot shut down cleanly must not block the task's teardown, and its stop()
// is called from a noexcept context.
template<typename ReaderPtr>
ss::future<> stop_reader_logged(
  const ReaderPtr& reader, prefix_logger& logger, std::string_view what) {
    if (!reader) {
        co_return;
    }
    auto stopped = co_await ss::coroutine::as_future(reader->stop());
    if (stopped.failed()) {
        auto ex = stopped.get_exception();
        vlog(logger.warn, "Error stopping Schema Registry {}: {}", what, ex);
    }
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

chunked_vector<ppsr::context_subject> select_in_scope(
  const chunked_hash_set<ppsr::context_subject>& targets,
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
    in_scope) {
    return targets
           | std::views::filter(
             [&in_scope](const ppsr::context_subject& target) {
                 return in_scope(target);
             })
           | std::ranges::to<chunked_vector<ppsr::context_subject>>();
}

chunked_hash_set<ppsr::context> batch_contexts(
  const chunked_vector<ppsr::context_subject>& subjects,
  const chunked_vector<ppsr::context_subject>& mode_config_targets) {
    constexpr auto contexts_of =
      [](const chunked_vector<ppsr::context_subject>& targets) {
          return targets | std::views::transform(&ppsr::context_subject::ctx);
      };
    return std::views::join(
             std::array{
               contexts_of(subjects), contexts_of(mode_config_targets)})
           | std::ranges::to<chunked_hash_set<ppsr::context>>();
}

// A context delete blocked because the context still has subjects (e.g. a
// reference-blocked version survived the purge); retried on the next full sync
// rather than counted as a hard error.
bool is_context_not_empty(const std::exception_ptr& ep) {
    try {
        std::rethrow_exception(ep);
    } catch (const ppsr::exception& e) {
        return e.code() == ppsr::error_code::context_not_empty;
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
  source_reader_factory* source_factory,
  tail_reader_factory* tail_factory)
  : task(
      link,
      tail_interval(link_metadata.configuration.schema_registry_sync_cfg),
      mirroring_task::task_name)
  , _config(link_metadata.configuration.schema_registry_sync_cfg.copy())
  , _destination(destination)
  , _source_factory(source_factory)
  , _reader(_source_factory->create(_config.api_mode()))
  , _tail_factory(tail_factory)
  , _tail(_tail_factory->create(link)) {
    _discovery = make_discovery();
}

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
    // Stop the readers BEFORE joining the run fiber: run_impl can be parked on
    // reader-internal waits that only the reader's own shutdown aborts (the
    // rate limiter's token queue and Retry-After pause -- up to 60s -- and the
    // connection pool's slot queue are deaf to the runner's abort source).
    // Joining first would sequence that abort behind the join that needs it.
    // The readers are built to be stopped under fire: in-flight and queued
    // requests fail promptly with abort-classified errors and run_impl
    // unwinds. as_future guards the noexcept contract.
    //
    // Under _reader_lifecycle: run_impl is still live here (readers-first), so
    // a concurrent reset_reader() could otherwise free a reader while this
    // stop() is suspended mid-shutdown. The lock is dropped before the join
    // below, so it cannot deadlock against a reset_reader() the join waits on.
    {
        auto reader_units = co_await _reader_lifecycle.get_units();
        co_await stop_reader_logged(_reader, logger(), "source reader");
        co_await stop_reader_logged(_tail, logger(), "tail reader");
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
    // Replace the stopped readers with fresh ones for that possible A->B->A
    // re-acquisition. A reader's stop() is permanent by design (it refuses
    // to rebuild its client so an unwinding fiber cannot resurrect it during
    // teardown), and run_impl only rebuilds the readers on a config change, so
    // a restarted task would otherwise keep using the dead readers and never
    // sync. No lock needed: the run fibers have been joined, so reset_reader()
    // cannot run and none is mid-request on the old readers.
    _reader = _source_factory->create(_config.api_mode());
    _tail = _tail_factory->create(get_link());
    co_return res;
}

ss::future<> mirroring_task::reset_reader() {
    // Serialize with stop() (see _reader_lifecycle): both stop and then free
    // the readers by reassignment, and either can be suspended in a reader's
    // shutdown when the other reaches the free.
    auto reader_units = co_await _reader_lifecycle.get_units();
    co_await stop_reader_logged(_reader, logger(), "previous source reader");
    co_await stop_reader_logged(_tail, logger(), "previous tail reader");
    _reader = _source_factory->create(_config.api_mode());
    _tail = _tail_factory->create(get_link());
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

ss::future<> mirroring_task::delete_source_absent_contexts(
  const chunked_hash_set<ppsr::context>& contexts,
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>& in_scope,
  ss::abort_source& as) {
    // Enumerated in the destination namespace. A destination context is a
    // deletion target when it is owned by this link (reverse-maps to a source
    // context), managed as a whole (in scope as a context, not merely via a
    // subject filter), is not the default context (which always exists), and is
    // no longer present at the source. Its subjects were already purged above.
    // `dest_contexts` is a named local, so the lazy view is safe to iterate
    // across the deletes below (which touch the store, not these operands).
    const auto dest_contexts = co_await _destination->list_contexts();
    auto to_delete
      = dest_contexts | std::views::filter([&](const ppsr::context& dest_ctx) {
            if (dest_ctx == ppsr::default_context) {
                return false;
            }
            auto src_ctx = _mapper.reverse(dest_ctx);
            return src_ctx.has_value()
                   && in_scope(
                     ppsr::context_subject{*src_ctx, ppsr::subject{""}})
                   && !contexts.contains(*src_ctx);
        });

    for (const auto& dest_ctx : to_delete) {
        as.check();
        auto fut = co_await ss::coroutine::as_future(
          _destination->delete_context(dest_ctx));
        if (fut.failed()) {
            auto ex = fut.get_exception();
            if (ssx::is_shutdown_exception(ex)) {
                std::rethrow_exception(ex);
            }
            if (is_context_not_empty(ex)) {
                // The purge could not empty it this run (e.g. a
                // reference-blocked version); retry on the next full sync.
                vlog(
                  logger().debug,
                  "Deferring delete of source-absent context {}: {}",
                  dest_ctx,
                  ex);
                continue;
            }
            record_error(
              fmt::format(
                "failed to delete source-absent context {}: {}", dest_ctx, ex));
            continue;
        }
        vlog(
          logger().info, "Deleted context {} absent from the source", dest_ctx);
    }
}

ss::future<std::optional<source_error>> mirroring_task::sync_absent_contexts(
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>& in_scope,
  ss::abort_source& as) {
    auto contexts = co_await _reader->list_contexts(as);
    if (!contexts.has_value()) {
        co_return std::move(contexts.error());
    }
    // Unfiltered, unlike the full sync's list: a wider set can only spare a
    // context from deletion, and the entries a filter would drop are exactly
    // the ones delete_source_absent_contexts rejects anyway.
    co_await delete_source_absent_contexts(
      contexts.value() | std::ranges::to<chunked_hash_set<ppsr::context>>(),
      in_scope,
      as);
    co_return std::nullopt;
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

task::state_transition
mirroring_task::on_source_error(const source_error& error) {
    if (error.kind == source_error_kind::source_unavailable) {
        return make_unavailable(error.message);
    }
    record_error(error.message);
    return make_active();
}

ss::future<task::state_transition>
mirroring_task::on_tail_source_error(const source_error& error) {
    if (error.kind != source_error_kind::source_unavailable) {
        // The source answered, so replaying the batch would collect the same
        // answer; counted as a per-item error and not retried, because a
        // persistent one would rewind forever and stall the tail.
        co_return on_source_error(error);
    }
    // Unapplied but still on the source topic, so put it back for a later tick.
    co_await _tail->rewind();
    co_return make_unavailable(error.message);
}

ss::future<
  std::expected<chunked_hash_set<ppsr::context>, task::state_transition>>
mirroring_task::load_sync_contexts(
  ss::abort_source& as,
  bool unfiltered,
  const chunked_hash_set<ppsr::context>& scan_contexts) {
    auto contexts_res = co_await _reader->list_contexts(as);
    if (!contexts_res.has_value()) {
        co_return std::unexpected(on_source_error(contexts_res.error()));
    }

    chunked_hash_set<ppsr::context> contexts;
    for (auto& ctx : contexts_res.value()) {
        if (unfiltered || scan_contexts.contains(ctx)) {
            contexts.insert(std::move(ctx));
        }
    }

    if (
      auto reason = check_preconditions(
        _config,
        contexts,
        config::shard_local_cfg().schema_registry_enable_qualified_subjects());
      reason.has_value()) {
        co_return std::unexpected(make_faulted(*reason));
    }

    co_return contexts;
}

void mirroring_task::record_error(std::string_view what) {
    ++_status.current_sync->summary.errors;
    ++_status.totals_since_task_start.errors;
    _status.last_error_message = ss::sstring{what};
    vlog(logger().warn, "Schema Registry sync error: {}", what);
}

work_set
mirroring_task::build_work_set(const discovered_versions& discovered) const {
    work_set work;
    // An active source version missing from the destination's active set is
    // imported, creating it or reactivating one that is soft-deleted on the
    // destination -- the seed does not suppress work items.
    for (const auto& node : discovered.active) {
        if (!_destination_inventory.active.contains(node)) {
            work.upserts.push_back(node);
        }
    }
    // A soft-deleted source version is imported unless it is already
    // soft-deleted on the destination: an absent version is imported
    // soft-deleted, and one still active on the destination has its deleted
    // body re-imported to propagate the soft-delete (import overwrites the
    // version's deleted flag). Detecting divergent same-key content is out of
    // scope (a matching key is assumed to mean matching content -- the
    // destination is a managed mirror).
    for (const auto& node : discovered.deleted) {
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
    return work;
}

chunked_vector<mirroring_task::purge_target>
mirroring_task::collect_purge_targets(
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
    in_purge_scope,
  const discovered_versions& discovered,
  const chunked_hash_set<ppsr::context>& failed_contexts,
  const chunked_hash_set<ppsr::context_subject>& failed_subjects) const {
    chunked_vector<purge_target> to_purge;
    for (const auto& node : _destination_inventory.all) {
        if (!in_purge_scope(node.sub)) {
            continue;
        }
        if (
          discovered.active.contains(node)
          || discovered.deleted.contains(node)) {
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
    return to_purge;
}

ss::future<source_result<reconcile_stats>> mirroring_task::run_reconcile(
  work_set work,
  model::schema_registry_sync_config::unsupported_feature_policy feature_policy,
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>& in_scope,
  reconciler::limits limits,
  ss::abort_source& as) {
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
        co_return std::unexpected(result.error());
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
    co_return stats;
}

ss::future<std::optional<source_error>> mirroring_task::sync_mode_configs(
  const chunked_vector<ppsr::context_subject>& targets,
  model::schema_registry_sync_config::unsupported_feature_policy feature_policy,
  reconciler::limits limits,
  ss::abort_source& as) {
    std::optional<source_error> unavailable;
    co_await ss::max_concurrent_for_each(
      targets,
      std::max<size_t>(1, limits.parallelism),
      [&](const ppsr::context_subject& target) {
          return sync_mode_and_config(target, feature_policy, as, unavailable);
      });
    co_return unavailable;
}

ss::future<task::state_transition> mirroring_task::full_source_sync(
  ss::abort_source& as,
  const chunked_hash_set<ppsr::context>& contexts,
  model::schema_registry_sync_config::unsupported_feature_policy feature_policy,
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
    in_scope) {
    const auto limits = sync_limits();

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

    auto versions = co_await _discovery->list_versions(
      *_reader, subjects, limits.parallelism, as);
    for (auto& error : versions.errors) {
        record_error(error);
    }
    if (versions.unavailable.has_value()) {
        co_return make_unavailable(versions.unavailable->message);
    }
    auto discovered = std::move(versions.discovered);
    failed_subjects = std::move(versions.failed_subjects);

    // Every discovered source version is selected for sync, soft-deleted ones
    // included (they are imported too), so count both -- mirroring
    // selected_source_subjects, a discovery count rather than a change count.
    _status.inventory.selected_source_subject_versions = static_cast<uint64_t>(
      discovered.active.size() + discovered.deleted.size());

    auto work = build_work_set(discovered);

    // A full sync discovers every in-scope subject, so nothing is out of purge
    // scope; only the failed listings are spared.
    auto to_purge = collect_purge_targets(
      [](const ppsr::context_subject&) { return true; },
      discovered,
      failed_contexts,
      failed_subjects);

    // run_reconcile() moves `_destination_inventory.all`, so the retained
    // inventory is invalid until the final rescan rebuilds it. If we exit
    // earlier, clear it and `_last_full_sync`: a config-forced full sync can
    // fail while the last successful full sync is still recent, and keeping
    // that timestamp would let the next run tail under the new config instead
    // of redoing the source-wide full sync.
    auto invalidate_inventory = ss::defer([this] {
        _destination_inventory = inventory{};
        _last_full_sync.reset();
    });
    auto result = co_await run_reconcile(
      std::move(work), feature_policy, in_scope, limits, as);
    if (!result.has_value()) {
        // reconcile only surfaces source_unavailable today; any other error is
        // handled defensively as a counted per-item failure.
        co_return on_source_error(result.error());
    }
    const auto stats = result.value();

    const auto purged = co_await purge_destination_only_versions(
      std::move(to_purge), as);

    // Replicate modes/configs after the import (the destination contexts and
    // subjects now exist)
    chunked_vector<ppsr::context_subject> mode_config_targets;

    // The registry-wide global mode/config (GET /mode/:.__GLOBAL:) is synced
    // only when in scope.
    const auto& global = ppsr::global_mode_config_target;
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
    auto mc_unavailable = co_await sync_mode_configs(
      mode_config_targets, feature_policy, limits, as);
    if (mc_unavailable.has_value()) {
        co_return make_unavailable(mc_unavailable->message);
    }

    // Tombstone any destination context whose source context is gone (its
    // subjects were purged above). Runs after mode/config so a
    // source-unavailable run backs off before touching contexts.
    co_await delete_source_absent_contexts(contexts, in_scope, as);

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
    invalidate_inventory.cancel();
    co_return make_active();
}

ss::future<source_result<mirroring_task::batch_sync_stats>>
mirroring_task::sync_batch_subjects(
  const tail_batch& batch,
  const chunked_vector<ppsr::context_subject>& subjects,
  model::schema_registry_sync_config::unsupported_feature_policy feature_policy,
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>& in_scope,
  reconciler::limits limits,
  ss::abort_source& as) {
    co_await refresh_destination_inventory(in_scope, as);

    auto versions = co_await _discovery->list_versions(
      *_reader, subjects, limits.parallelism, as);
    for (auto& error : versions.errors) {
        record_error(error);
    }
    if (versions.unavailable.has_value()) {
        co_return std::unexpected(std::move(*versions.unavailable));
    }
    auto discovered = std::move(versions.discovered);
    auto failed_subjects = std::move(versions.failed_subjects);
    // The inventory counters stay as the full sync left them: they describe the
    // whole selected source, and this pass looked at a handful of subjects.

    // Only the subjects this tick refreshed may be purged; every other subject
    // went unexamined and would otherwise read as source-absent. A subject the
    // source no longer has at all discovers as empty rather than failed, so its
    // destination versions do get purged -- which is how a source-side subject
    // delete propagates.
    auto to_purge = collect_purge_targets(
      [&batch](const ppsr::context_subject& subject) {
          return batch.subjects.contains(subject);
      },
      discovered,
      /*failed_contexts=*/{},
      failed_subjects);

    auto result = co_await run_reconcile(
      build_work_set(discovered), feature_policy, in_scope, limits, as);
    if (!result.has_value()) {
        co_return std::unexpected(std::move(result).error());
    }

    co_return batch_sync_stats{
      .reconciled = std::move(result).value(),
      .purged = co_await purge_destination_only_versions(
        std::move(to_purge), as)};
}

ss::future<task::state_transition> mirroring_task::feed_tail_sync(
  ss::abort_source& as,
  model::schema_registry_sync_config::unsupported_feature_policy feature_policy,
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
    in_scope) {
    const auto polled = co_await _tail->poll(as);
    if (!polled.has_value()) {
        co_return on_source_error(polled.error());
    }
    const auto& batch = polled.value();

    const auto subjects = select_in_scope(batch.subjects, in_scope);
    const auto mode_config_targets = select_in_scope(
      batch.mode_configs, in_scope);
    // Ahead of the empty check: a backlog is worth reporting even when this
    // batch's targets were all out of scope.
    if (batch.truncated) {
        // Not a warning: the consumer keeps its position for the next tick.
        vlog(
          logger().debug,
          "Schema Registry tail sync: source changes exceeded one poll's "
          "budget; the remainder follows on the next tick");
    }
    if (
      subjects.empty() && mode_config_targets.empty()
      && batch.contexts.empty()) {
        // The common case. Notably this does not rescan the destination: a tail
        // tick that found nothing must cost nothing.
        co_return make_active();
    }

    // A context created since the last full sync reaches the destination
    // through the tail first, so vet it the way the full sync vets what it
    // discovered.
    if (
      const auto reason = check_preconditions(
        _config,
        batch_contexts(subjects, mode_config_targets),
        config::shard_local_cfg().schema_registry_enable_qualified_subjects());
      reason.has_value()) {
        co_return make_faulted(*reason);
    }

    const auto limits = sync_limits();

    // CONFIG, MODE and CONTEXT records name no subject, so a batch of only
    // those skips the subject phase -- and the whole-registry destination scan
    // it opens with.
    const auto synced
      = subjects.empty()
          ? source_result<batch_sync_stats>{batch_sync_stats{}}
          : co_await sync_batch_subjects(
              batch, subjects, feature_policy, in_scope, limits, as);
    if (!synced.has_value()) {
        co_return co_await on_tail_source_error(synced.error());
    }

    const auto mc_unavailable = co_await sync_mode_configs(
      mode_config_targets, feature_policy, limits, as);
    if (mc_unavailable.has_value()) {
        co_return co_await on_tail_source_error(*mc_unavailable);
    }

    // After mode/config, as the full sync does, so a source-unavailable run
    // backs off first. The source refuses to delete a context that still has
    // subjects, so the purge above has already emptied it.
    if (!batch.contexts.empty()) {
        auto ctx_error = co_await sync_absent_contexts(in_scope, as);
        if (ctx_error.has_value()) {
            co_return co_await on_tail_source_error(*ctx_error);
        }
    }

    // Only schema imports and purges invalidate the destination inventory
    // counts.
    const bool schemas_changed = synced->reconciled.versions_changed > 0
                                 || synced->purged > 0;
    if (schemas_changed) {
        co_await refresh_destination_inventory(in_scope, as);
    }

    // Ticks are frequent and a replayed record reconciles to nothing, so a tick
    // that changed nothing stays at debug. Mode and config writes count towards
    // that because they are not logged individually.
    const auto& summary = _status.current_sync->summary;
    const bool anything_changed = schemas_changed || summary.modes_changed > 0
                                  || summary.compatibility_configs_changed > 0;
    vlogl(
      logger(),
      anything_changed ? ss::log_level::info : ss::log_level::debug,
      "Schema Registry tail sync: {} subjects, {} mode/config targets, {} "
      "contexts; imported {} versions, hard-deleted {} versions",
      subjects.size(),
      mode_config_targets.size(),
      batch.contexts.size(),
      synced->reconciled.versions_changed,
      synced->purged);

    // _last_full_sync and _status.last_full_sync stay untouched: a tail sync is
    // not a full scan and must not postpone the next one. The per-item failures
    // counted along the way are terminal (see source_error_kind), so they are
    // not queued for retry.
    co_return make_active();
}

reconcile_stats mirroring_task::fold_reconcile_stats() {
    const auto stats = _reconcile_stats;
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
    return stats;
}

std::unique_ptr<discovery> mirroring_task::make_discovery() {
    return std::make_unique<discovery>();
}

ss::future<chunked_hash_set<ppsr::context_subject>>
mirroring_task::known_subjects(
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>& in_scope,
  ss::abort_source& as) {
    // The feed path moves the retained inventory into its reconcile seed and
    // rescans only when something landed, so a feed tick that imported
    // nothing can leave it empty; rebuild it rather than rediscover the
    // whole registry as new.
    if (_destination_inventory.all.empty()) {
        co_await refresh_destination_inventory(in_scope, as);
    }
    chunked_hash_set<ppsr::context_subject> known;
    for (const auto& node : _destination_inventory.all) {
        known.insert(node.sub);
    }
    co_return known;
}

ss::future<task::state_transition> mirroring_task::http_fallback_tail_sync(
  ss::abort_source& as,
  const chunked_hash_set<ppsr::context>& contexts,
  model::schema_registry_sync_config::unsupported_feature_policy feature_policy,
  const ss::noncopyable_function<bool(const ppsr::context_subject&)>&
    in_scope) {
    const auto known = co_await known_subjects(in_scope, as);
    auto listing = co_await _discovery->list_new_subjects(
      *_reader, contexts, known, in_scope, as);
    for (auto& error : listing.errors) {
        record_error(error);
    }
    if (listing.unavailable.has_value()) {
        co_return make_unavailable(listing.unavailable->message);
    }

    if (listing.subjects.empty()) {
        // The common case; costs the listings above and nothing else.
        co_return make_active();
    }

    const auto limits = sync_limits();
    auto versions = co_await _discovery->list_versions(
      *_reader, listing.subjects, limits.parallelism, as);
    for (auto& error : versions.errors) {
        record_error(error);
    }
    if (versions.unavailable.has_value()) {
        co_return make_unavailable(versions.unavailable->message);
    }
    auto discovered = std::move(versions.discovered);

    // Same diff the full sync uses. Not redundant here: listing-leg nodes are
    // new by construction, but probe hits can name nodes the destination
    // already holds.
    auto work = build_work_set(discovered);
    if (work.upserts.empty()) {
        co_return make_active();
    }

    // Not run_reconcile: that moves the retained inventory into its seed,
    // while this path's seed is copied -- the inventory is the next tick's
    // diff baseline and this tick may skip the closing rescan.
    auto rec = reconciler{
      _reader.get(),
      _destination,
      [&in_scope](const ppsr::context_subject& cs) { return in_scope(cs); },
      _mapper,
      limits,
      feature_policy};
    _reconcile_stats = reconcile_stats{};
    auto result = co_await rec.reconcile(
      std::move(work),
      ss::chunked_hash_set_from_range(_destination_inventory.all),
      _reconcile_stats,
      as);
    // Before branching on the result: reconcile can fail partway with
    // versions already landed, and a stale tail inventory is not repaired by
    // the next run -- it would rediscover the same subjects on every tick.
    const auto stats = fold_reconcile_stats();
    // Refresh the retained destination baseline only if reconcile actually
    // imported versions. The next HTTP-tail tick reuses
    // `_destination_inventory`
    // (`all` and `max_id`) to discover deltas, so if a partial reconcile landed
    // writes and then failed, we must repair that baseline here rather than let
    // the next tick rediscover the same items. The rescan reads only the
    // destination, so it costs no source requests and is safe even when the
    // source has just gone away.
    if (stats.versions_changed > 0) {
        co_await refresh_destination_inventory(in_scope, as);
    }
    if (!result.has_value()) {
        if (result.error().kind == source_error_kind::source_unavailable) {
            co_return make_unavailable(result.error().message);
        }
        record_error(result.error().message);
        co_return make_active();
    }

    vlog(
      logger().info,
      "Schema Registry tail sync (HTTP): {} new subjects, imported {} "
      "versions, {} errors",
      listing.subjects.size(),
      stats.versions_changed,
      stats.errors);

    // No finish_time stamp: run_impl clears current_sync on the way out and
    // nothing carries a tail summary anywhere, unlike the full sync copying
    // its own into last_full_sync.
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
    chunked_hash_set<ppsr::context> scan_contexts;
    for (const auto& ctx : filter_contexts) {
        scan_contexts.insert(ctx);
    }
    for (const auto& cs : filter_subjects) {
        scan_contexts.insert(cs.ctx);
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

    if (!long_sync) {
        // A live change feed is authoritative -- cheaper, and it sees
        // deletions and mode/config changes -- so the feed always gets its
        // tick first: poll both serves a live reader and resumes a dead one
        // from its own progress (with backoff), and a reader that never
        // armed polls as a free no-op. Only when the feed comes out of the
        // tick still not serving does this tick discover over HTTP instead.
        auto feed_tick = co_await feed_tail_sync(as, feature_policy, in_scope);
        if (
          _tail->armed()
          || feed_tick.desired_state != model::task_state::active) {
            co_return feed_tick;
        }
        // Discovery needs the in-scope contexts, which the feed path does
        // not: one list_contexts per fallback tick.
        auto contexts = co_await load_sync_contexts(
          as, unfiltered, scan_contexts);
        if (!contexts.has_value()) {
            co_return std::move(contexts.error());
        }
        co_return co_await http_fallback_tail_sync(
          as, contexts.value(), feature_policy, in_scope);
    }

    // Before this sync's first source read: see tail_reader::arm for why that
    // ordering is what makes the tail lossless.
    //
    // The verdict is dropped rather than stored: whether tailing is live is the
    // reader's own state, which it logs, and a copy here would be a second
    // source of truth to keep in step across leadership and config changes.
    //
    // Guarded because tailing is an optimization and the full sync below is
    // not: an exception here would fault the task before a single source read,
    // taking replication down over a spare change feed.
    auto armed = co_await ss::coroutine::as_future(_tail->arm(as));
    if (armed.failed()) {
        auto ex = armed.get_exception();
        if (ssx::is_shutdown_exception(ex)) {
            std::rethrow_exception(ex);
        }
        vlog(
          logger().warn, "Schema Registry topic tailing could not arm: {}", ex);
    }

    auto contexts = co_await load_sync_contexts(as, unfiltered, scan_contexts);
    if (!contexts.has_value()) {
        co_return std::move(contexts.error());
    }

    co_await refresh_destination_inventory(in_scope, as);
    co_return co_await full_source_sync(
      as, contexts.value(), feature_policy, in_scope);
}

task::state_transition
mirroring_task::make_unavailable(const ss::sstring& reason) {
    vlog(
      logger().warn, "Schema Registry shadowing task unavailable: {}", reason);
    _status.last_error_message = reason;
    // No special backoff: an unavailable run leaves _last_full_sync unadvanced,
    // so the next run re-attempts the full sync and the link recovers on the
    // normal cadence once the source comes back.
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
      link,
      *(link->get_config()),
      _destination,
      _source_factory,
      _tail_factory);
}

} // namespace cluster_link::schema_registry_sync
