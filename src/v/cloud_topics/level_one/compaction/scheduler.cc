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
#include "cloud_topics/level_one/compaction/log_sampler.h"
#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/scheduling_policies.h"
#include "compaction/utils.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

namespace cloud_topics::l1 {

compaction_scheduler::compaction_scheduler(
  compaction_cluster_state state,
  std::unique_ptr<scheduling_policy> policy,
  ss::sharded<file_io>* io)
  : _io(io)
  , _metastore(nullptr)
  , _log_collector(make_default_log_collector(this, state))
  , _log_sampler(_metastore, state.topic_table)
  , _scheduling_policy(std::move(policy))
  , _executor(io, &_committer)
  , _compaction_interval(
      config::shard_local_cfg().log_compaction_interval_ms.bind()) {
    _compaction_interval.watch([this]() { _sem.signal(); });
}

bool compaction_scheduler::is_managed(const model::ntp& ntp) const noexcept {
    return _logs.contains(ntp);
}

void compaction_scheduler::manage_partition(
  const model::ntp& ntp,
  const model::topic_id_partition& tid_p,
  std::string_view ctx) {
    vlog(
      compaction_log.info, "Asked to manage compacted log: {} ({})", ntp, ctx);
    auto [it, success] = _logs.insert(
      std::make_unique<log_compaction_meta>(tid_p, ntp));
    _logs_list.push_back(*it->get());
    vassert(
      success, "Could not manage compacted log {} (concurrency issue?)", ntp);
}

ss::future<> compaction_scheduler::unmanage_partition(
  const model::ntp& ntp, std::string_view ctx) {
    vlog(
      compaction_log.info,
      "Asked to unmanage compacted log: {} ({})",
      ntp,
      ctx);
    auto handle_opt = _logs.extract(ntp);
    if (!handle_opt) {
        co_return;
    }

    auto handle = std::move(handle_opt).value();

    auto close_fut = handle->gate.close();

    // Request that compaction of this ntp be stopped, if in flight.
    co_await _executor.request_stop_compaction(ntp);

    co_await std::move(close_fut);
}

void compaction_scheduler::start_bg_loop() {
    ssx::repeat_until_gate_closed_or_aborted(_gate, _as, [this] {
        return scheduling_loop().handle_exception(
          [](const std::exception_ptr& e) {
              auto log_level = ssx::is_shutdown_exception(e)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
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
    auto holder = _gate.hold();
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

        co_await schedule_some();
    }
}

ss::future<> compaction_scheduler::schedule_some() {
    auto log_infos = co_await _log_sampler.sample_logs(_logs_list);

    if (log_infos.empty()) {
        co_return;
    }

    filter_log_infos(log_infos);

    auto logs = _scheduling_policy->sort_log_infos(std::move(log_infos));

    while (!logs.empty() && !_gate.is_closed() && !_as.abort_requested()) {
        auto next = std::move(logs.front());
        logs.pop_front();
        co_await _executor.compact_log(std::move(next));
    }
}

ss::future<> compaction_scheduler::start() {
    co_await _log_collector->start();
    co_await _committer.start(
      ss::sharded_parameter([] { return make_default_committing_policy(); }),
      _metastore,
      ss::sharded_parameter([this] { return &_io->local(); }));
    co_await _executor.start();
    start_bg_loop();
}

ss::future<> compaction_scheduler::stop() {
    vlog(compaction_log.debug, "Stopping compaction scheduling loop");
    _as.request_abort();
    _sem.broken();

    chunked_vector<ss::future<>> futs;
    futs.reserve(4);
    // Stop making new jobs.
    futs.push_back(_gate.close());
    // Stop collecting logs.
    futs.push_back(_log_collector->stop());
    // Stop committing data.
    futs.push_back(_committer.stop());
    // Request to stop inflight compactions and execution waiters.
    futs.push_back(_executor.request_stop_executor());

    static constexpr size_t max_concurrent_close = 1024;

    // Empty list of logs.
    co_await ss::max_concurrent_for_each(
      _logs.begin(), _logs.end(), max_concurrent_close, [](auto& log) {
          return log->gate.close();
      });

    _logs.clear();

    co_await ss::when_all_succeed(futs.begin(), futs.end());

    // It is only safe to stop the executor once all gates have been closed.
    co_await _executor.stop();
}

void compaction_scheduler::filter_log_infos(
  chunked_vector<log_info_and_meta>& logs) const {
    auto needs_compaction = [](const log_info_and_meta& log) {
        auto min_cleanable_dirty_ratio = 0.5;
        auto max_compaction_lag_ms = std::chrono::milliseconds(
          std::numeric_limits<uint32_t>::max());
        return compaction::log_needs_compaction(
          log.info.dirty_ratio,
          min_cleanable_dirty_ratio,
          log.info.earliest_dirty_ts,
          max_compaction_lag_ms);
    };
    chunked_vector<log_info_and_meta> filtered_logs;
    std::copy_if(
      std::make_move_iterator(logs.begin()),
      std::make_move_iterator(logs.end()),
      std::back_inserter(filtered_logs),
      needs_compaction);
    logs = std::move(filtered_logs);
}

std::unique_ptr<compaction_scheduler> make_default_compaction_scheduler(
  compaction_cluster_state state, ss::sharded<file_io>* io) {
    return std::make_unique<compaction_scheduler>(
      state, make_default_scheduling_policy(), io);
}

} // namespace cloud_topics::l1
