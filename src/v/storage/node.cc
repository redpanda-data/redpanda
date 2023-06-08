/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "storage/node.h"

namespace storage {

ss::future<> node::start() {
    _probe.setup_node_metrics();
    co_return;
}

ss::future<> node::stop() { co_return; }

void node::set_disk_metrics(
  disk_type t,
  uint64_t total_bytes,
  uint64_t free_bytes,
  disk_space_alert alert) {
    if (t == disk_type::data) {
        _data_watchers.notify(
          uint64_t(total_bytes), uint64_t(free_bytes), alert);
    } else if (t == disk_type::cache) {
        _cache_watchers.notify(
          uint64_t(total_bytes), uint64_t(free_bytes), alert);
    }
    _probe.set_disk_metrics(total_bytes, free_bytes, alert);
}

node::notification_id
node::register_disk_notification(disk_type t, disk_cb_t cb) {
    if (t == disk_type::data) {
        return _data_watchers.register_cb(std::move(cb));
    } else if (t == disk_type::cache) {
        return _cache_watchers.register_cb(std::move(cb));
    } else {
        vassert(
          false,
          "Unknown disk type {}",
          static_cast<std::underlying_type<disk_type>::type>(t));
    }
}

void node::unregister_disk_notification(disk_type t, notification_id id) {
    if (t == disk_type::data) {
        return _data_watchers.unregister_cb(id);
    } else if (t == disk_type::cache) {
        return _cache_watchers.unregister_cb(id);
    } else {
        vassert(
          false,
          "Unknown disk type {}",
          static_cast<std::underlying_type<disk_type>::type>(t));
    }
}

ss::future<struct statvfs> node::get_statvfs(ss::sstring path) {
    if (unlikely(_statvfs_for_test)) {
        co_return _statvfs_for_test(path);
    } else {
        co_return co_await ss::engine().statvfs(path);
    }
}

void node::testing_only_set_statvfs(
  std::function<struct statvfs(ss::sstring)> func) {
    _statvfs_for_test = std::move(func);
}

} // namespace storage
