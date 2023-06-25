/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "storage/api.h"

#include "storage/log.h"
#include "utils/human.h"

namespace storage {

ss::future<> api::start() {
    _kvstore = std::make_unique<kvstore>(
      _kv_conf_cb(), _resources, _feature_table);
    co_await _kvstore->start();
    _log_mgr = std::make_unique<log_manager>(
      _log_conf_cb(), kvs(), _resources, _feature_table, &container());
    co_await _log_mgr->start();

    if (ss::this_shard_id() == 0) {
        ssx::spawn_with_gate(_gate, [this] { return monitor(); });
        _log_storage_max_size.watch([this] { _monitor_sem.signal(); });
    }
}

ss::future<> api::stop() {
    _monitor_sem.broken();
    co_await _gate.close();

    if (_log_mgr) {
        co_await _log_mgr->stop();
    }
    if (_kvstore) {
        co_await _kvstore->stop();
    }
}

ss::future<usage_report> api::disk_usage() {
    co_return co_await container().map_reduce0(
      [](api& api) {
          return ss::when_all_succeed(
                   api._log_mgr->disk_usage(), api._kvstore->disk_usage())
            .then([](std::tuple<usage_report, usage_report> usage) {
                const auto& [disk, kvs] = usage;
                return disk + kvs;
            });
      },
      usage_report{},
      [](usage_report acc, usage_report update) { return acc + update; });
}

void api::handle_disk_notification(
  uint64_t total_space, uint64_t free_space, storage::disk_space_alert alert) {
    _resources.update_allowance(total_space, free_space);
    if (_log_mgr) {
        _log_mgr->handle_disk_notification(alert);
    }
}

ss::future<> api::monitor() {
    vassert(ss::this_shard_id() == 0, "Run on wrong core");

    /*
     * The control loop should run periodically, but the current setting of 2
     * seconds which provides decent responsiveness is too low to be used in
     * practice long term. A follow up change will increase this setting and
     * then allow cores to wake up this thread on demand to provide reactivity
     * without frequent polling.
     */
    constexpr auto frequency = std::chrono::seconds(2);

    while (true) {
        try {
            if (_log_storage_max_size().has_value()) {
                co_await _monitor_sem.wait(
                  frequency, std::max(_monitor_sem.current(), size_t(1)));
            } else {
                co_await _monitor_sem.wait();
            }
        } catch (const ss::semaphore_timed_out&) {
            // time for some controlling
        }

        if (!_log_storage_max_size().has_value()) {
            continue;
        }

        auto usage = co_await disk_usage();

        const auto max_exceeded = _log_storage_max_size().has_value()
                                  && usage.usage.total()
                                       > _log_storage_max_size().value();

        // broadcast if state changed
        if (max_exceeded != _max_size_exceeded) {
            co_await container().invoke_on_all([max_exceeded](api& api) {
                vlog(
                  stlog.info,
                  "{} maximum size exceeded flag",
                  max_exceeded ? "Setting" : "Clearing");
                api._max_size_exceeded = max_exceeded;
            });
        }

        if (_max_size_exceeded) {
            vlog(
              stlog.warn,
              "Log storage usage {} exceeds configured max {}",
              human::bytes(usage.usage.total()),
              human::bytes(_log_storage_max_size().value()));

            // wake up housekeeping on every core to run garbage collection
            co_await container().invoke_on_all(
              [](auto& api) { api._log_mgr->trigger_housekeeping(); });
        }
    }
}

bool api::max_size_exceeded() const { return _max_size_exceeded; }

} // namespace storage
