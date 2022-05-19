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

#include "cluster/logger.h"
#include "cluster/node/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "storage/api.h"
#include "storage/types.h"
#include "utils/human.h"
#include "vassert.h"
#include "version.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>

#include <fmt/core.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <seastarx.h>

namespace cluster::node {

local_monitor::local_monitor(
  config::binding<size_t> alert_bytes,
  config::binding<unsigned> alert_percent,
  config::binding<size_t> min_bytes,
  ss::sharded<storage::node_api>& api)
  : _free_bytes_alert_threshold(alert_bytes)
  , _free_percent_alert_threshold(alert_percent)
  , _min_free_bytes(min_bytes)
  , _storage_api(api) {}

ss::future<> local_monitor::update_state() {
    // grab new snapshot of local state
    auto disks = co_await get_disks();
    auto vers = application_version(ss::sstring(redpanda_version()));
    auto uptime = std::chrono::duration_cast<std::chrono::milliseconds>(
      ss::engine().uptime());
    vassert(!disks.empty(), "No disk available to monitor.");
    _state = {
      .redpanda_version = vers,
      .uptime = uptime,
      .disks = disks,
    };
    update_alert_state();
    co_return co_await update_disk_metrics();
}

const local_state& local_monitor::get_state_cached() const { return _state; }

void local_monitor::testing_only_set_path(const ss::sstring& path) {
    _path_for_test = path;
}

void local_monitor::testing_only_set_statvfs(
  std::function<struct statvfs(const ss::sstring)> func) {
    _statvfs_for_test = std::move(func);
}

std::tuple<size_t, size_t>
local_monitor::minimum_free_by_bytes_and_percent(size_t bytes_available) const {
    long double percent_factor = _free_percent_alert_threshold() / 100.0;
    return std::make_tuple(
      _free_bytes_alert_threshold(), percent_factor * bytes_available);
}

ss::future<std::vector<storage::disk>> local_monitor::get_disks() {
    auto path = _path_for_test.empty()
                  ? config::node().data_directory().as_sstring()
                  : _path_for_test;

    auto svfs = co_await get_statvfs(path);

    co_return std::vector<storage::disk>{storage::disk{
      .path = config::node().data_directory().as_sstring(),
      // f_bsize is a historical linux-ism, use f_frsize
      .free = svfs.f_bfree * svfs.f_frsize,
      .total = svfs.f_blocks * svfs.f_frsize,
    }};
}

// NOLINTNEXTLINE (performance-unnecessary-value-param)
ss::future<struct statvfs> local_monitor::get_statvfs(const ss::sstring path) {
    if (unlikely(_statvfs_for_test)) {
        co_return _statvfs_for_test(path);
    } else {
        co_return co_await ss::engine().statvfs(path);
    }
}

float local_monitor::percent_free(const storage::disk& disk) {
    long double free = disk.free, total = disk.total;
    return float((free / total) * 100.0);
}

void local_monitor::maybe_log_space_error(const storage::disk& disk) {
    auto state = _state.storage_space_alert;
    if (state == storage::disk_space_alert::ok) {
        return;
    }
    auto [min_by_bytes, min_by_percent] = minimum_free_by_bytes_and_percent(
      disk.total);
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
      state == storage::disk_space_alert::degraded ? degraded_text
                                                   : alert_text);
}

void local_monitor::update_alert_state() {
    _state.storage_space_alert = storage::disk_space_alert::ok;
    for (const auto& d : _state.disks) {
        vassert(d.total != 0.0, "Total disk space cannot be zero.");
        // 1. Check alert thresholds.
        auto [min_by_bytes, min_by_percent] = minimum_free_by_bytes_and_percent(
          d.total);
        auto alert_min = std::max(min_by_percent, min_by_bytes);
        auto min_bytes = _min_free_bytes();
        if (unlikely(d.free <= alert_min)) {
            _state.storage_space_alert = storage::disk_space_alert::low_space;
        }
        // 2. Check degraded (read-only) threshold
        if (unlikely(d.free <= min_bytes)) {
            _state.storage_space_alert = storage::disk_space_alert::degraded;
        }
        maybe_log_space_error(d);
        clusterlog.debug(
          "alert % {}, alert bytes {}, min bytes {} disk.free {} -> alert {}",
          min_by_percent,
          min_by_bytes,
          min_bytes,
          d.free,
          _state.storage_space_alert);
    }
}

// Preconditions: _state.disks is non-empty.
ss::future<> local_monitor::update_disk_metrics() {
    auto& d = _state.disks[0];
    return _storage_api.invoke_on_all(
      &storage::node_api::set_disk_metrics,
      d.total,
      d.free,
      _state.storage_space_alert);
}

} // namespace cluster::node