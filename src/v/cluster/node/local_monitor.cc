/*
 * Copyright 2021 Vectorized, Inc.
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
#include "config/node_config.h"
#include "version.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <seastarx.h>

namespace cluster::node {

ss::future<> local_monitor::update_state() {
    auto disks = co_await get_disks();
    auto vers = application_version(ss::sstring(redpanda_version()));
    auto uptime = std::chrono::duration_cast<std::chrono::milliseconds>(
      ss::engine().uptime());

    _state = {
      .redpanda_version = vers,
      .uptime = uptime,
      .disks = disks,
    };
    update_alert_state();
    co_return;
}

const local_state& local_monitor::get_state_cached() const { return _state; }

void local_monitor::set_path_for_test(const ss::sstring& path) {
    _path_for_test = path;
}

void local_monitor::set_statvfs_for_test(
  std::function<struct statvfs(const ss::sstring&)> func) {
    _statvfs_for_test = std::move(func);
}

ss::future<std::vector<disk>> local_monitor::get_disks() {
    auto path = _path_for_test.empty()
                  ? config::node().data_directory().as_sstring()
                  : _path_for_test;

    auto svfs = co_await get_statvfs(path);

    co_return std::vector<disk>{disk{
      .path = config::node().data_directory().as_sstring(),
      // f_bsize is a historical linux-ism, use f_frsize
      .free = svfs.f_bfree * svfs.f_frsize,
      .total = svfs.f_blocks * svfs.f_frsize,
    }};
}

ss::future<struct statvfs> local_monitor::get_statvfs(const ss::sstring& path) {
    if (_statvfs_for_test) {
        co_return _statvfs_for_test.value()(path);
    } else {
        co_return co_await ss::engine().statvfs(path);
    }
}

void local_monitor::update_alert_state() {
    _state.storage_space_alert = disk_space_alert::ok;
    for (const auto& d : _state.disks) {
        vassert(d.total != 0.0, "Total disk space cannot be zero.");
        double min_space = double(d.total) * alert_min_free_space_percent;
        min_space = std::min(min_space, alert_min_free_space_bytes);
        clusterlog.debug(
          "{}: min by % {}, min bytes {}, disk.free {} -> alert {}",
          __func__,
          double(d.total) * alert_min_free_space_percent,
          alert_min_free_space_bytes,
          d.free,
          double(d.free) <= min_space);

        if (double(d.free) <= min_space) {
            _state.storage_space_alert = disk_space_alert::low_space;
        }
    }
}

} // namespace cluster::node