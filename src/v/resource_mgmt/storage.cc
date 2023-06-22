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

#include "cloud_storage/cache_service.h"
#include "cluster/partition_manager.h"
#include "vlog.h"

#include <seastar/util/log.hh>

static ss::logger rlog("resource_mgmt");

namespace storage {

disk_space_manager::disk_space_manager(
  config::binding<bool> enabled,
  ss::sharded<storage::api>* storage,
  ss::sharded<storage::node>* storage_node,
  ss::sharded<cloud_storage::cache>* cache,
  ss::sharded<cluster::partition_manager>* pm)
  : _enabled(std::move(enabled))
  , _storage(storage)
  , _storage_node(storage_node)
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
    if (ss::this_shard_id() == run_loop_core) {
        ssx::spawn_with_gate(_gate, [this] { return run_loop(); });
        _cache_disk_nid = _storage_node->local().register_disk_notification(
          node::disk_type::cache,
          [this](node::disk_space_info info) { _cache_disk_info = info; });
    }
    co_return;
}

ss::future<> disk_space_manager::stop() {
    vlog(rlog.info, "Stopping disk space manager service");
    if (ss::this_shard_id() == run_loop_core) {
        _storage_node->local().unregister_disk_notification(
          node::disk_type::cache, _cache_disk_nid);
    }
    _control_sem.broken();
    co_await _gate.close();
}

ss::future<> disk_space_manager::run_loop() {
    vassert(ss::this_shard_id() == run_loop_core, "Run on wrong core");

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
    }
}

} // namespace storage
