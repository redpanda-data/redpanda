/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/node/local_monitor.h"

#include "base/seastarx.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "cluster/logger.h"
#include "cluster/node/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "storage/api.h"
#include "storage/node.h"
#include "storage/types.h"
#include "utils/human.h"
#include "version/version.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>

#include <fmt/core.h>

#include <algorithm>
#include <cassert>
#include <chrono>

using namespace std::chrono_literals;

namespace cluster::node {

// Period between updates where we will issue system call to get free space
constexpr ss::lowres_clock::duration tick_period = 1s;

local_monitor::local_monitor(
  config::binding<size_t> alert_bytes,
  config::binding<unsigned> alert_percent,
  ss::sharded<storage::node>& node_api)
  : _free_bytes_alert_threshold(std::move(alert_bytes))
  , _free_percent_alert_threshold(std::move(alert_percent))
  , _storage_node_api(node_api) {}

ss::future<> local_monitor::_update_loop() {
    while (!_abort_source.abort_requested()) {
        co_await update_state();
        co_await ss::sleep_abortable(tick_period, _abort_source);
    }
}

ss::future<> local_monitor::start() {
    // Load disk stats inline on start, so that anything relying on these
    // stats downstream can get them without waiting for our first tick.
    co_await update_state();

    ssx::spawn_with_gate(_gate, [this]() { return _update_loop(); });

    co_return;
}

ss::future<> local_monitor::stop() {
    _abort_source.request_abort();

    co_await _gate.close();
}

ss::future<> local_monitor::update_state() {
    // grab new snapshot of local state
    auto new_state = local_state{
      .redpanda_version = application_version(ss::sstring(redpanda_version())),
      .uptime = std::chrono::duration_cast<std::chrono::milliseconds>(
        ss::engine().uptime()),
      .recovery_mode_enabled = config::node().recovery_mode_enabled(),
    };
    co_await update_disks(new_state);
    update_alert_state(new_state);

    _state = new_state;

    /*
     * this is overriden by space management once it starts up. the default
     * value is nullopt which indicates this early state.
     */
    _state.log_data_size = _log_data_state;

    co_return co_await update_disk_metrics();
}

const local_state& local_monitor::get_state_cached() const { return _state; }

namespace {
size_t alert_percent_in_bytes(unsigned alert_percent, size_t bytes_available) {
    long double percent_factor = alert_percent / 100.0;
    return percent_factor * bytes_available;
}

/*
 * calculcate the free disk space alert thresholds
 *
 * <low_space, degraded>
 */
std::pair<size_t, size_t> calc_alert_thresholds(const storage::disk& d) {
    auto& cfg = config::shard_local_cfg();
    unsigned alert_percent
      = cfg.storage_space_alert_free_threshold_percent.value();
    size_t alert_bytes = cfg.storage_space_alert_free_threshold_bytes.value();
    size_t min_bytes = cfg.storage_min_free_bytes();

    size_t min_by_percent = alert_percent_in_bytes(alert_percent, d.total);
    auto alert_min = std::max(min_by_percent, alert_bytes);

    return std::make_pair(alert_min, min_bytes);
}
} // namespace

storage::disk
local_monitor::statvfs_to_disk(const storage::node::stat_info& info) {
    // f_bsize is a historical linux-ism, use f_frsize
    const auto& svfs = info.stat;
    uint64_t free = svfs.f_bfree * svfs.f_frsize;
    uint64_t total = svfs.f_blocks * svfs.f_frsize;

    return storage::disk{
      .path = info.path,
      .free = free,
      .total = total,
      .fsid = svfs.f_fsid,
    };
}

ss::future<> local_monitor::update_disks(local_state& state) {
    using dt = storage::node::disk_type;
    auto data_svfs = co_await _storage_node_api.local().get_statvfs(dt::data);
    auto cache_svfs = co_await _storage_node_api.local().get_statvfs(dt::cache);
    state.data_disk = statvfs_to_disk(data_svfs);
    if (cache_svfs.stat.f_fsid != data_svfs.stat.f_fsid) {
        state.cache_disk = statvfs_to_disk(cache_svfs);
    } else {
        state.cache_disk = std::nullopt;
    }
}

float local_monitor::percent_free(const storage::disk& disk) {
    long double free = disk.free, total = disk.total;
    return float((free / total) * 100.0);
}

void local_monitor::maybe_log_space_error(const storage::disk& disk) {
    if (disk.alert == storage::disk_space_alert::ok) {
        return;
    }
    size_t min_by_bytes = _free_bytes_alert_threshold();
    size_t min_by_percent = alert_percent_in_bytes(
      _free_percent_alert_threshold(), disk.total);

    auto min_space = std::min(min_by_percent, min_by_bytes);
    constexpr auto alert_text = "avoid running out of space";
    constexpr auto degraded_text = "allow writing again";
    clusterlog.log(
      ss::log_level::error,
      _despam_interval,
      "{}: free space at {:.3f}\% on {}: {} total, {} free, min. free {}. "
      "Please adjust retention policies as needed to {}",
      stable_alert_string,
      percent_free(disk),
      disk.path,
      // TODO: generalize human::bytes for unsigned long
      human::bytes(disk.total), // NOLINT narrowing conv.
      human::bytes(disk.free),  // NOLINT  "  "
      human::bytes(min_space),  // NOLINT  "  "
      disk.alert == storage::disk_space_alert::degraded ? degraded_text
                                                        : alert_text);
}

void local_monitor::update_alert(storage::disk& d) {
    if (unlikely(d.total == 0.0)) {
        vlog(
          clusterlog.error,
          "Disk reported zero total bytes, ignoring free space.");
        d.alert = storage::disk_space_alert::ok;
    } else {
        const auto [alert_min, min_bytes] = calc_alert_thresholds(d);
        if (unlikely(d.free <= min_bytes)) {
            d.alert = storage::disk_space_alert::degraded;
        } else if (unlikely(d.free <= alert_min)) {
            d.alert = storage::disk_space_alert::low_space;
        } else {
            d.alert = storage::disk_space_alert::ok;
        }
    }
}

void local_monitor::update_alert_state(local_state& state) {
    update_alert(state.data_disk);
    maybe_log_space_error(state.data_disk);
    if (!state.shared_disk()) {
        update_alert(state.get_cache_disk());
        maybe_log_space_error(state.get_cache_disk());
    }
}

ss::future<> local_monitor::update_disk_metrics() {
    const auto [data_low, data_degraded] = calc_alert_thresholds(
      _state.data_disk);
    co_await _storage_node_api.invoke_on_all(
      &storage::node::set_disk_metrics,
      storage::node::disk_type::data,
      storage::node::disk_space_info{
        .total = _state.data_disk.total,
        .free = _state.data_disk.free,
        .alert = _state.data_disk.alert,
        .degraded_threshold = data_degraded,
        .low_space_threshold = data_low,
        .fsid = _state.data_disk.fsid,
      });

    // Always notify for cache disk, even if it's the same underlying drive:
    // subscribers to updates on cache disk space still need to get updates.
    auto cache_disk = _state.get_cache_disk();
    const auto [cache_low, cache_degraded] = calc_alert_thresholds(cache_disk);
    co_await _storage_node_api.invoke_on_all(
      &storage::node::set_disk_metrics,
      storage::node::disk_type::cache,
      storage::node::disk_space_info{
        .total = cache_disk.total,
        .free = cache_disk.free,
        .alert = cache_disk.alert,
        .degraded_threshold = cache_degraded,
        .low_space_threshold = cache_low,
        .fsid = cache_disk.fsid,
      });
}

} // namespace cluster::node
