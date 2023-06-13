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

node::node(ss::sstring data_directory, ss::sstring cache_directory)
  : _data_directory(std::move(data_directory))
  , _cache_directory(std::move(cache_directory)) {}

ss::future<> node::start() {
    // Intentionally undocumented environment variable, only for use
    // in integration tests.
    const char* test_disk_size_str = std::getenv("__REDPANDA_TEST_DISK_SIZE");
    if (test_disk_size_str) {
        _disk_size_for_test = std::stoul(std::string(test_disk_size_str));
    }
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

ss::future<node::stat_info> node::get_statvfs(disk_type type) {
    const auto path = [&] {
        switch (type) {
        case disk_type::data:
            return _data_directory;
        case disk_type::cache:
            return _cache_directory;
        }
    }();

    stat_info info{};
    info.path = path;
    auto& stat = info.stat;

    if (unlikely(_statvfs_for_test)) {
        stat = _statvfs_for_test(path);
    } else {
        stat = co_await ss::engine().statvfs(path);
    }

    if (_disk_size_for_test.has_value()) {
        const auto used_blocks = stat.f_blocks - stat.f_bfree;
        const auto total_blocks = std::max(
          _disk_size_for_test.value() / stat.f_frsize, 1UL);

        if (total_blocks < used_blocks) {
            vlog(
              stlog.error,
              "Mocked disk has more used space {} than total space {}: "
              "ignoring override",
              used_blocks,
              total_blocks);
        } else {
            stat.f_blocks = total_blocks;
            stat.f_bfree = total_blocks - used_blocks;
        }
    }

    co_return info;
}

void node::testing_only_set_statvfs(
  std::function<struct statvfs(ss::sstring)> func) {
    _statvfs_for_test = std::move(func);
}

} // namespace storage
