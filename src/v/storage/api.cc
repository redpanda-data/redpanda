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

#include "base/vlog.h"
#include "storage/logger.h"
#include "syschecks/syschecks.h"

#include <seastar/core/seastar.hh>

namespace storage {

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

void api::trigger_gc() { _log_mgr->trigger_gc(); }

ss::future<bool> api::wait_for_cluster_uuid() {
    if (_cluster_uuid.has_value()) {
        co_return true;
    }
    try {
        co_await _has_cluster_uuid_cond.wait();
        vassert(
          _cluster_uuid.has_value(), "Expected cluster UUID after waiting");
        co_return true;
    } catch (const ss::broken_condition_variable&) {
        vlog(stlog.info, "Stopped waiting for cluster UUID");
    }
    co_return false;
}

namespace directories {

ss::future<> initialize(ss::sstring dir) {
    return recursive_touch_directory(dir)
      .handle_exception([dir](std::exception_ptr ep) {
          stlog.error(
            "Directory `{}` cannot be initialized. Failed with {}", dir, ep);
          return ss::make_exception_future<>(std::move(ep));
      })
      .then([dir] {
          vlog(stlog.info, "Checking `{}` for supported filesystems", dir);
          return syschecks::disk(dir);
      });
}

} // namespace directories

} // namespace storage
