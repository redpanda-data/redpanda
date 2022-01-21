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

#include "cluster/node/types.h"
#include "config/node_config.h"
#include "version.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>

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
} // namespace cluster::node