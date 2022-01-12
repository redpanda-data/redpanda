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

// TODO make async
ss::future<> local_monitor::update_state() {
    auto disks = get_disks();
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

const local_state& local_monitor::get_state_cached() { return _state; }

// TODO make async
std::vector<disk> local_monitor::get_disks() {
    auto space_info = std::filesystem::space(
      config::node().data_directory().path);

    return {disk{
      .path = config::node().data_directory().as_sstring(),
      .free = space_info.free,
      .total = space_info.capacity,
    }};
}

} // namespace cluster::node