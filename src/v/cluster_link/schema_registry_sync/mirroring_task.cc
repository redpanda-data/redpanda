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
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/namespace.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>

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

} // namespace

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
  , _reader(_source_factory->create()) {}

void mirroring_task::update_config(const model::metadata& link_metadata) {
    _config = link_metadata.configuration.schema_registry_sync_cfg.copy();
    set_run_interval(tail_interval(_config));
    // The scope (filters/contexts) may have changed; flag a forced full scan so
    // the next run re-derives the inventory. Only a flag is set here: mutating
    // _status/_last_full_sync would race an in-flight run_impl that resumes and
    // overwrites it.
    _config_changed = true;
}

model::enabled_t mirroring_task::is_enabled() const {
    return model::enabled_t(_config.api_mode() != nullptr);
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

model::task_status_report mirroring_task::get_status_report() const {
    auto report = task::get_status_report();
    // The sync runs only on the shard leading _schemas/0; other shards keep the
    // task stopped with default (empty) status. Emitting that empty status
    // would let a non-leader's report win the cross-shard/node admin
    // aggregation over the leader's real status, so only a running task
    // surfaces it.
    if (get_state() != model::task_state::stopped) {
        report.detail = model::task_detail{
          .schema_registry_sync_status = _status};
    }
    return report;
}

ss::future<> mirroring_task::refresh_destination_inventory() {
    // Single scatter-gather over the local registry. Destination/internal
    // faults bubble out and become `faulted`.
    auto subject_versions = co_await _destination->list_subject_versions(
      [](const ppsr::context_subject& cs) {
          return cs.ctx == ppsr::default_context;
      },
      ppsr::include_deleted::no);
    chunked_hash_set<ppsr::context_subject> subjects;
    for (const auto& sv : subject_versions) {
        subjects.insert(sv.sub);
    }
    _status.inventory.destination_subjects = static_cast<uint64_t>(
      subjects.size());
    _status.inventory.destination_subject_versions = static_cast<uint64_t>(
      subject_versions.size());
}

ss::future<task::state_transition> mirroring_task::full_source_sync(
  ss::abort_source& as, model::schema_registry_sync_summary& summary) {
    auto record_error = [this, &summary](std::string_view what) {
        ++summary.errors;
        ++_status.totals_since_task_start.errors;
        _status.last_error_message = ss::sstring{what};
        _status.current_sync->summary = summary;
        vlog(logger().warn, "Schema Registry sync error: {}", what);
    };

    auto subjects_res = co_await _reader->list_subjects(
      ppsr::default_context, as);
    if (!subjects_res.has_value()) {
        if (
          subjects_res.error().kind == source_error_kind::source_unavailable) {
            co_return make_unavailable(subjects_res.error().message);
        }
        // Could not enumerate the source: count the error and retry next cycle
        // without recording a (misleading) completed full sync. The source is
        // reachable, so this is not link_unavailable.
        record_error(subjects_res.error().message);
        co_return make_active();
    }

    const auto& subjects = subjects_res.value();
    uint64_t version_count = 0;
    for (const auto& subject : subjects) {
        as.check();
        auto versions_res = co_await _reader->list_subject_versions(
          subject, as);
        if (!versions_res.has_value()) {
            if (
              versions_res.error().kind
              == source_error_kind::source_unavailable) {
                co_return make_unavailable(versions_res.error().message);
            }
            record_error(versions_res.error().message);
            continue;
        }
        version_count += static_cast<uint64_t>(versions_res.value().size());
    }
    _status.inventory.selected_source_subjects = static_cast<uint64_t>(
      subjects.size());
    _status.inventory.selected_source_subject_versions = version_count;

    // The reconcile/apply step (import missing versions, update, delete, apply
    // config/mode) is not implemented yet.
    vlog(
      logger().info,
      "Schema Registry full sync: {} source subjects ({} versions), {} "
      "destination subjects; reconcile/apply not yet implemented",
      _status.inventory.selected_source_subjects,
      _status.inventory.selected_source_subject_versions,
      _status.inventory.destination_subjects);

    summary.finish_time = ::model::timestamp::now();
    _status.last_full_sync = summary;
    _last_full_sync = ss::lowres_clock::now();
    co_return make_active();
}

ss::future<task::state_transition>
mirroring_task::run_impl(ss::abort_source& as) {
    // Consume the config-changed flag before any co_await so a concurrent
    // update_config during this run is not lost (it re-arms for the next run).
    const bool long_sync = std::exchange(_config_changed, false)
                           || should_long_sync();

    model::schema_registry_sync_summary summary;
    summary.start_time = ::model::timestamp::now();
    _status.current_sync = model::schema_registry_current_sync{
      .sync_type = long_sync ? model::schema_registry_sync_type::full
                             : model::schema_registry_sync_type::tail,
      .summary = summary};
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

    co_await refresh_destination_inventory();

    // full_source_sync advances _last_full_sync only when a scan completes, so
    // a failed enumeration retries on the next tick rather than waiting a full
    // interval.
    co_return co_await full_source_sync(as, summary);
}

task::state_transition
mirroring_task::make_unavailable(const ss::sstring& reason) {
    vlog(
      logger().warn, "Schema Registry shadowing task unavailable: {}", reason);
    _status.last_error_message = reason;
    return state_transition{
      .desired_state = model::task_state::link_unavailable, .reason = reason};
}

task::state_transition mirroring_task::make_active() {
    return state_transition{
      .desired_state = model::task_state::active,
      .reason = "Schema Registry shadowing task finished a sync"};
}

std::string_view mirroring_task_factory::created_task_name() const noexcept {
    return mirroring_task::task_name;
}

std::unique_ptr<task> mirroring_task_factory::create_task(link* link) {
    return std::make_unique<mirroring_task>(
      link, *(link->get_config()), _destination, _source_factory);
}

} // namespace cluster_link::schema_registry_sync
