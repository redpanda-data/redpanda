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

#include "kafka/server/usage_manager.h"

#include "base/vlog.h"
#include "cluster/controller.h"
#include "cluster/health_monitor_frontend.h"
#include "config/configuration.h"
#include "kafka/server/logger.h"
#include "ssx/future-util.h"
#include "storage/api.h"

namespace kafka {

usage_manager::usage_accounting_fiber::usage_accounting_fiber(
  cluster::controller* controller,
  ss::sharded<usage_manager>& um,
  ss::sharded<cluster::health_monitor_frontend>& health_monitor,
  ss::sharded<storage::api>& storage,
  size_t usage_num_windows,
  std::chrono::seconds usage_window_width_interval,
  std::chrono::seconds usage_disk_persistance_interval)
  : usage_aggregator<>(
      storage.local().kvs(),
      usage_num_windows,
      usage_window_width_interval,
      usage_disk_persistance_interval)
  , _controller(controller)
  , _health_monitor(health_monitor.local())
  , _um(um) {}

/// The fiber running on the timer has a mutable effect when sample() is
/// called, to prevent issues when open bucket is querying all shards for
/// current stats, guard this area of code with a mutex.
ss::future<usage>
usage_manager::usage_accounting_fiber::close_current_window() {
    auto u = co_await _um.map_reduce0(
      [](usage_manager& um) { return um.sample(); }, usage{}, std::plus<>());
    u.bytes_cloud_storage = co_await get_cloud_usage_data();
    co_return u;
}

ss::future<std::optional<uint64_t>>
usage_manager::usage_accounting_fiber::get_cloud_usage_data() {
    vassert(
      ss::this_shard_id() == usage_manager::usage_manager_main_shard
        && ss::this_shard_id() == ss::shard_id(0),
      "Usage manager accounting fiber must run on shard 0");
    const auto is_leader = _controller->is_raft0_leader();
    if (!is_leader) {
        co_return std::nullopt;
    }
    const auto expiry = std::min<std::chrono::seconds>(
      max_history(), std::chrono::seconds(10));
    auto health_overview = co_await _health_monitor.get_cluster_health_overview(
      ss::lowres_clock::now() + expiry);
    co_return health_overview.bytes_in_cloud_storage;
}

usage_manager::usage_manager(
  cluster::controller* controller,
  ss::sharded<cluster::health_monitor_frontend>& health_monitor,
  ss::sharded<storage::api>& storage)
  : _usage_enabled(config::shard_local_cfg().enable_usage.bind())
  , _usage_num_windows(config::shard_local_cfg().usage_num_windows.bind())
  , _usage_window_width_interval(
      config::shard_local_cfg().usage_window_width_interval_sec.bind())
  , _usage_disk_persistance_interval(
      config::shard_local_cfg().usage_disk_persistance_interval_sec.bind())
  , _controller(controller)
  , _health_monitor(health_monitor)
  , _storage(storage) {}

ss::future<> usage_manager::reset() {
    oncore_debug_verify(_verify_shard);
    try {
        auto h = _background_gate.hold();
        auto u = co_await _background_mutex.get_units();
        if (_accounting_fiber) {
            /// Deallocate the accounting_fiber if the feature is disabled,
            /// otherwise it will keep in memory the number of configured
            /// historical buckets
            auto accounting_fiber = std::exchange(_accounting_fiber, nullptr);
            co_await accounting_fiber->stop();
        }
        co_await start_accounting_fiber();
    } catch (ss::gate_closed_exception&) {
        // shutting down
    }
}

ss::future<> usage_manager::start_accounting_fiber() {
    if (!_usage_enabled()) {
        co_return; /// Feature is disabled in config
    }
    if (_accounting_fiber) {
        co_return; /// Double start called, do-nothing
    }
    _accounting_fiber = std::make_unique<usage_accounting_fiber>(
      _controller,
      this->container(),
      _health_monitor,
      _storage,
      _usage_num_windows(),
      _usage_window_width_interval(),
      _usage_disk_persistance_interval());

    co_await _accounting_fiber->start();

    /// Reset all state
    co_await container().invoke_on_all(
      [](usage_manager& um) { (void)um.sample(); });
}

ss::future<> usage_manager::start() {
    if (ss::this_shard_id() != usage_manager_main_shard) {
        co_return; /// Async work only occurs on shard-0
    }
    co_await start_accounting_fiber();

    _usage_enabled.watch([this] { (void)reset(); });
    _usage_num_windows.watch([this] { (void)reset(); });
    _usage_window_width_interval.watch([this] { (void)reset(); });
    _usage_disk_persistance_interval.watch([this] { (void)reset(); });
}

ss::future<> usage_manager::stop() {
    co_await _background_gate.close();
    if (!_accounting_fiber) {
        co_return;
    }
    /// Logic could only possibly execute on core-0 since no other cores would
    /// initialize a local _accounting_fiber
    co_await _accounting_fiber->stop();
}

ss::future<std::vector<usage_window>> usage_manager::get_usage_stats() const {
    if (ss::this_shard_id() != usage_manager_main_shard) {
        throw std::runtime_error(
          "Attempt to query results of "
          "kafka::usage_manager off the main accounting shard");
    }
    if (!_accounting_fiber) {
        co_return std::vector<usage_window>{}; // no stats
    }
    co_return co_await _accounting_fiber->get_usage_stats();
}

} // namespace kafka
