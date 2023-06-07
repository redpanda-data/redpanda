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

#include "storage.h"

#include "cluster/partition_manager.h"
#include "vlog.h"

#include <seastar/util/log.hh>

static ss::logger rlog("resource_mgmt");

namespace storage {

disk_space_manager::disk_space_manager(
  config::binding<bool> enabled,
  ss::sharded<storage::api>* storage,
  ss::sharded<cloud_storage::cache>* cache,
  ss::sharded<cluster::partition_manager>* pm)
  : _enabled(std::move(enabled))
  , _storage(storage)
  , _cache(cache->local_is_initialized() ? cache : nullptr)
  , _pm(pm) {
    _enabled.watch([this] {
        vlog(
          rlog.info,
          "{} disk space manager control loop",
          _enabled() ? "Enabling" : "Disabling");
        _control_sem.signal();
    });
}

ss::future<> disk_space_manager::start() {
    vlog(
      rlog.info,
      "Starting disk space manager service ({})",
      _enabled() ? "enabled" : "disabled");
    ssx::spawn_with_gate(_gate, [this] { return run_loop(); });
    co_return;
}

ss::future<> disk_space_manager::stop() {
    vlog(rlog.info, "Stopping disk space manager service");
    _control_sem.broken();
    co_await _gate.close();
}

ss::future<> disk_space_manager::run_loop() {
    /*
     * we want the code here to actually run a little, but the final shape of
     * configuration options is not yet known.
     */
    constexpr auto frequency = std::chrono::seconds(30);

    while (true) {
        try {
            if (_enabled()) {
                co_await _control_sem.wait(
                  frequency, std::max(_control_sem.current(), size_t(1)));
            } else {
                co_await _control_sem.wait();
            }
        } catch (const ss::semaphore_timed_out&) {
            // time for some controlling
        }

        if (!_enabled()) {
            continue;
        }

        /*
         * Collect cache and logs storage usage information. These accumulate
         * across all shards (despite the local() accessor). If a failure occurs
         * we wait rather than operate with a reduced set of information.
         */
        cloud_storage::cache_usage_target cache_usage_target;
        try {
            cache_usage_target
              = co_await _pm->local().get_cloud_cache_disk_usage_target();
        } catch (...) {
            vlog(
              rlog.info,
              "Unable to collect cloud cache usage: {}",
              std::current_exception());
            continue;
        }

        storage::usage_report logs_usage;
        try {
            logs_usage = co_await _storage->local().disk_usage();
        } catch (...) {
            vlog(
              rlog.info,
              "Unable to collect log storage usage: {}",
              std::current_exception());
            continue;
        }

        vlog(
          rlog.debug,
          "Cloud storage cache target minimum size {} nice to have {}",
          cache_usage_target.target_min_bytes,
          cache_usage_target.target_bytes);

        vlog(
          rlog.debug,
          "Log storage usage total {} - data {} index {} compaction {}",
          logs_usage.usage.total(),
          logs_usage.usage.data,
          logs_usage.usage.index,
          logs_usage.usage.compaction);

        vlog(
          rlog.debug,
          "Log storage usage available for reclaim local {} total {}",
          logs_usage.reclaim.retention,
          logs_usage.reclaim.available);

        vlog(
          rlog.debug,
          "Log storage usage target minimum size {} nice to have {}",
          logs_usage.target.min_capacity,
          logs_usage.target.min_capacity_wanted);
    }
}

} // namespace storage
