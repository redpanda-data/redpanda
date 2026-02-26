/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/scheduler.h"

#include "cloud_topics/level_one/maintenance/log_collector.h"
#include "cloud_topics/level_one/maintenance/log_info_collector.h"
#include "cloud_topics/level_one/maintenance/logger.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/scheduling_policies.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

namespace cloud_topics::l1 {

maintenance_scheduler::maintenance_scheduler(
  maintenance_cluster_state state,
  ss::sharded<file_io>* io,
  ss::sharded<l1::replicated_metastore>* metastore,
  ss::sharded<level_one_reader_probe>* l1_reader_probe)
  : _io(io)
  , _metastore(metastore)
  , _log_collector(make_default_log_collector(
      [this](
        const model::ntp& ntp,
        const model::topic_id_partition& tidp,
        std::string_view ctx) { manage_partition(ntp, tidp, ctx); },
      [this](const model::ntp& ntp, std::string_view ctx) {
          unmanage_partition(ntp, ctx);
      },
      [this](const model::ntp& ntp) { return is_managed(ntp); },
      state))
  , _log_info_collector(make_default_log_info_collector(
      &_metastore->local(),
      &state.metadata_cache->local(),
      state.shard_table,
      state.partition_manager))
  , _scheduling_policy(make_default_scheduling_policy())
  , _worker_manager(
      _compaction_queue,
      _leveling_queue,
      io,
      metastore,
      state.metadata_cache,
      _probe,
      l1_reader_probe)
  , _compaction_interval(
      config::shard_local_cfg().cloud_topics_compaction_interval_ms.bind())
  , _leveling_interval(
      config::shard_local_cfg().cloud_topics_leveling_interval_ms.bind())
  , _compaction_queue(_scheduling_policy->get_compaction_comparator())
  , _leveling_queue(_scheduling_policy->get_leveling_comparator()) {
    _compaction_interval.watch([this]() { _compaction_sem.signal(); });
    _leveling_interval.watch([this]() { _leveling_sem.signal(); });
}

maintenance_scheduler::maintenance_scheduler(log_info_collector info_collector)
  : _log_info_collector(std::move(info_collector))
  , _scheduling_policy(make_default_scheduling_policy())
  , _worker_manager(
      _compaction_queue,
      _leveling_queue,
      nullptr,
      nullptr,
      nullptr,
      _probe,
      nullptr)
  , _compaction_interval(
      config::shard_local_cfg().cloud_topics_compaction_interval_ms.bind())
  , _leveling_interval(
      config::shard_local_cfg().cloud_topics_leveling_interval_ms.bind())
  , _compaction_queue(_scheduling_policy->get_compaction_comparator())
  , _leveling_queue(_scheduling_policy->get_leveling_comparator()) {
    _compaction_interval.watch([this]() { _compaction_sem.signal(); });
    _leveling_interval.watch([this]() { _leveling_sem.signal(); });
}

bool maintenance_scheduler::is_managed(const model::ntp& ntp) const noexcept {
    auto it = _ntp_to_tidp.find(ntp);
    if (it == _ntp_to_tidp.end()) {
        return false;
    }

    auto& tidp = it->second;

    return _logs.contains(tidp);
}

void maintenance_scheduler::manage_partition(
  const model::ntp& ntp,
  const model::topic_id_partition& tidp,
  std::string_view ctx) {
    vlog(
      maintenance_log.info, "Asked to manage CTP: {}/{} ({})", ntp, tidp, ctx);
    auto [it, success] = _logs.insert(
      ss::make_lw_shared<log_maintenance_meta>(tidp, ntp));
    _logs_list.push_back(*it->get());
    _ntp_to_tidp.emplace(ntp, tidp);
    vassert(success, "Could not manage CTP {} (concurrency issue?)", ntp);
    _probe.set_log_count(_logs.size());
}

void maintenance_scheduler::unmanage_partition(
  const model::ntp& ntp, std::string_view ctx) {
    auto tidp_entry = _ntp_to_tidp.extract(ntp);
    if (!tidp_entry.has_value()) {
        vassert(false, "Could not unmanage CTP {} (concurrency issue?)", ntp);
    }

    auto& tidp = tidp_entry->second;

    vlog(maintenance_log.info, "Asked to unmanage CTP: {} ({})", ntp, ctx);

    auto handle_opt = _logs.extract(tidp);
    if (!handle_opt) {
        vassert(false, "Could not unmanage CTP {} (concurrency issue?)", ntp);
    }

    auto handle = std::move(handle_opt).value();

    // Manually unlink here to ensure that if the handle also exists in a
    // `log_maintenance_queue`, `is_linked()` still returns `false` when it is
    // eventually considered for maintenance.
    handle->link.unlink();

    // Request that maintenance of this CTP be stopped, if in flight. `handle`
    // is a `lw_shared_ptr`- we can allow it to go out of scope here without
    // fear of UAF elsewhere.
    _worker_manager.request_stop_maintenance(std::move(handle));
    _probe.set_log_count(_logs.size());
}

void maintenance_scheduler::start_bg_loop() {
    ssx::repeat_until_gate_closed_or_aborted(_gate, _as, [this] {
        return compaction_scheduling_loop().handle_exception(
          [](const std::exception_ptr& e) {
              auto log_level = ssx::is_shutdown_exception(e)
                                 ? ss::log_level::debug
                                 : ss::log_level::error;
              vlogl(
                maintenance_log,
                log_level,
                "Encountered exception in compaction scheduling loop: {}",
                e);
          });
    });
    ssx::repeat_until_gate_closed_or_aborted(_gate, _as, [this] {
        return leveling_scheduling_loop().handle_exception(
          [](const std::exception_ptr& e) {
              auto log_level = ssx::is_shutdown_exception(e)
                                 ? ss::log_level::debug
                                 : ss::log_level::error;
              vlogl(
                maintenance_log,
                log_level,
                "Encountered exception in leveling scheduling loop: {}",
                e);
          });
    });
}

ss::future<> maintenance_scheduler::compaction_scheduling_loop() {
    vlog(maintenance_log.debug, "Starting compaction scheduling loop");
    while (!_gate.is_closed() && !_as.abort_requested()) {
        auto interval = _compaction_interval();
        try {
            co_await _compaction_sem.wait(
              interval, std::max(_compaction_sem.current(), size_t(1)));
        } catch (const ss::semaphore_timed_out&) {
            // Fall through
        }

        if (interval != _compaction_interval()) {
            continue;
        }

        _probe.set_compaction_queue_length(_compaction_queue.size());

        co_await _log_info_collector.collect_compaction_info(
          _logs, _logs_list, _compaction_queue);

        co_await _worker_manager.alert_compaction();
    }
}

ss::future<> maintenance_scheduler::leveling_scheduling_loop() {
    vlog(maintenance_log.debug, "Starting leveling scheduling loop");
    while (!_gate.is_closed() && !_as.abort_requested()) {
        auto interval = _leveling_interval();
        try {
            co_await _leveling_sem.wait(
              interval, std::max(_leveling_sem.current(), size_t(1)));
        } catch (const ss::semaphore_timed_out&) {
            // Fall through
        }

        if (interval != _leveling_interval()) {
            continue;
        }

        _probe.set_leveling_queue_length(_leveling_queue.size());

        co_await _log_info_collector.collect_leveling_info(
          _logs, _logs_list, _leveling_queue);

        co_await _worker_manager.alert_leveling();
    }
}

ss::future<> maintenance_scheduler::start() {
    _probe.setup_metrics();
    co_await _worker_manager.start();
    co_await _log_collector->start();
    start_bg_loop();
}

ss::future<> maintenance_scheduler::stop() {
    vlog(maintenance_log.debug, "Stopping maintenance scheduling loops");
    _as.request_abort();
    _compaction_sem.broken();
    _leveling_sem.broken();

    // Stop making new jobs.
    auto close_fut = _gate.close();

    // Stop collecting logs.
    if (_log_collector) {
        co_await _log_collector->stop();
    }

    // Clear logs.
    _logs.clear();

    // Stop workers and inflight maintenance jobs.
    co_await _worker_manager.stop();

    co_await std::move(close_fut);
}

} // namespace cloud_topics::l1
