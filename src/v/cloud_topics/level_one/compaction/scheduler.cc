/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/scheduler.h"

#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/committing_policy.h"
#include "cloud_topics/level_one/compaction/log_collector.h"
#include "cloud_topics/level_one/compaction/log_info_collector.h"
#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/scheduling_policies.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

namespace cloud_topics::l1 {

compaction_scheduler::compaction_scheduler(
  compaction_cluster_state state,
  ss::sharded<file_io>* io,
  ss::sharded<l1::replicated_metastore>* metastore)
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
      io,
      metastore,
      &_committer,
      state.metadata_cache,
      _probe)
  , _compaction_interval(
      config::shard_local_cfg().cloud_topics_compaction_interval_ms.bind())
  , _compaction_queue(_scheduling_policy->get_comparator()) {
    _compaction_interval.watch([this]() { _sem.signal(); });
}

compaction_scheduler::compaction_scheduler(log_info_collector info_collector)
  : _log_info_collector(std::move(info_collector))
  , _scheduling_policy(make_default_scheduling_policy())
  , _worker_manager(
      _compaction_queue, nullptr, nullptr, &_committer, nullptr, _probe)
  , _compaction_interval(
      config::shard_local_cfg().cloud_topics_compaction_interval_ms.bind())
  , _compaction_queue(_scheduling_policy->get_comparator()) {
    _compaction_interval.watch([this]() { _sem.signal(); });
}

bool compaction_scheduler::is_managed(const model::ntp& ntp) const noexcept {
    auto it = _ntp_to_tidp.find(ntp);
    if (it == _ntp_to_tidp.end()) {
        return false;
    }

    auto& tidp = it->second;

    return _logs.contains(tidp);
}

void compaction_scheduler::manage_partition(
  const model::ntp& ntp,
  const model::topic_id_partition& tidp,
  std::string_view ctx) {
    vlog(
      compaction_log.info,
      "Asked to manage compacted CTP: {}/{} ({})",
      ntp,
      tidp,
      ctx);
    auto [it, success] = _logs.insert(
      ss::make_lw_shared<log_compaction_meta>(tidp, ntp));
    _logs_list.push_back(*it->get());
    _ntp_to_tidp.emplace(ntp, tidp);
    vassert(
      success, "Could not manage compacted CTP {} (concurrency issue?)", ntp);
    _probe.set_log_count(_logs.size());
}

void compaction_scheduler::unmanage_partition(
  const model::ntp& ntp, std::string_view ctx) {
    auto tidp_entry = _ntp_to_tidp.extract(ntp);
    if (!tidp_entry.has_value()) {
        vassert(
          false,
          "Could not unmanage compacted CTP {} (concurrency issue?)",
          ntp);
    }

    auto& tidp = tidp_entry->second;

    vlog(
      compaction_log.info,
      "Asked to unmanage compacted CTP: {} ({})",
      ntp,
      ctx);

    auto handle_opt = _logs.extract(tidp);
    if (!handle_opt) {
        vassert(
          false,
          "Could not unmanage compacted CTP {} (concurrency issue?)",
          ntp);
    }

    auto handle = std::move(handle_opt).value();

    // Manually unlink here to ensure that if the handle also exists in the
    // `log_compaction_queue`, `is_linked()` still returns `false` when it is
    // eventually considered for compaction.
    handle->link.unlink();

    // Request that compaction of this CTP be stopped, if in flight. `handle` is
    // a `lw_shared_ptr`- we can allow it to go out of scope here without fear
    // of UAF elsewhere.
    _worker_manager.request_stop_compaction(std::move(handle));
    _probe.set_log_count(_logs.size());
}

void compaction_scheduler::start_bg_loop() {
    ssx::repeat_until_gate_closed_or_aborted(_gate, _as, [this] {
        return scheduling_loop().handle_exception(
          [](const std::exception_ptr& e) {
              auto log_level = ssx::is_shutdown_exception(e)
                                 ? ss::log_level::debug
                                 : ss::log_level::error;
              vlogl(
                compaction_log,
                log_level,
                "Encountered exception in main loop: {}",
                e);
          });
    });
}

ss::future<> compaction_scheduler::scheduling_loop() {
    vlog(compaction_log.debug, "Starting compaction scheduling loop");
    while (!_gate.is_closed() && !_as.abort_requested()) {
        auto compaction_interval = _compaction_interval();
        try {
            co_await _sem.wait(
              _compaction_interval(), std::max(_sem.current(), size_t(1)));
        } catch (const ss::semaphore_timed_out&) {
            // Fall through
        }

        if (compaction_interval != _compaction_interval()) {
            // Cluster config was changed while waiting.
            continue;
        }

        _probe.set_compaction_queue_length(_compaction_queue.size());

        co_await _log_info_collector.collect_info_for_logs(
          _logs, _logs_list, _compaction_queue);

        co_await _worker_manager.alert_workers();
    }
}

ss::future<> compaction_scheduler::start() {
    _probe.setup_metrics();
    co_await _committer.start(
      ss::sharded_parameter([] { return make_default_committing_policy(); }),
      ss::sharded_parameter([this] { return &_io->local(); }),
      ss::sharded_parameter([this] { return &_metastore->local(); }));
    co_await _committer.invoke_on_all(&compaction_committer::start);
    co_await _worker_manager.start();
    co_await _log_collector->start();
    start_bg_loop();
}

ss::future<> compaction_scheduler::stop() {
    vlog(compaction_log.debug, "Stopping compaction scheduling loop");
    _as.request_abort();
    _sem.broken();

    // Stop making new jobs.
    auto close_fut = _gate.close();

    // Stop collecting logs.
    if (_log_collector) {
        co_await _log_collector->stop();
    }

    // Clear logs.
    _logs.clear();

    // Stop workers and inflight compactions.
    co_await _worker_manager.stop();

    // Destruct committer.
    co_await _committer.stop();

    co_await std::move(close_fut);
}

} // namespace cloud_topics::l1
