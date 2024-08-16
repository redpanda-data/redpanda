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

#include "storage/logger.h"

#include <seastar/core/reactor.hh>

namespace storage {

node::node(ss::sstring data_directory, ss::sstring cache_directory)
  : _data_directory(std::move(data_directory))
  , _cache_directory(std::move(cache_directory)) {}

ss::future<> node::start() {
    /*
     * Data disk override via environment variable for control in early boot.
     * Currently restricted to data disk only.
     */
    const char* test_disk_size_str = std::getenv("__REDPANDA_TEST_DISK_SIZE");
    if (test_disk_size_str) {
        _data_overrides.total_bytes = std::stoul(
          std::string(test_disk_size_str));
    }
    _probe.setup_node_metrics();
    co_return;
}

ss::future<> node::stop() { co_return; }

void node::set_disk_metrics(disk_type t, disk_space_info info) {
    if (t == disk_type::data) {
        _data_watchers.notify(info);
    } else if (t == disk_type::cache) {
        _cache_watchers.notify(info);
    }
    _probe.set_disk_metrics(info.total, info.free, info.alert);
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
    const auto [path, overrides] = [&] {
        switch (type) {
        case disk_type::data:
            return std::make_pair(_data_directory, _data_overrides);
        case disk_type::cache:
            return std::make_pair(_cache_directory, _cache_overrides);
        }
    }();

    stat_info info{};
    info.path = path;
    auto& stat = info.stat;

    stat = co_await ss::engine().statvfs(path);

    if (unlikely(overrides.has_overrides())) {
        auto used_blocks = stat.f_blocks - stat.f_bfree;

        /*
         * total size
         */
        auto total_blocks = stat.f_blocks;
        if (overrides.total_bytes.has_value()) {
            total_blocks = std::max(
              overrides.total_bytes.value() / stat.f_frsize, 1UL);
        }

        /*
         * free space
         */
        if (overrides.free_bytes.has_value()) {
            const auto free_blocks = std::max(
              overrides.free_bytes.value() / stat.f_frsize, 1UL);
            used_blocks = total_blocks - free_blocks;
        }

        /*
         * free bytes delta can adjust free size up/down. it's a signed value.
         */
        if (overrides.free_bytes_delta.has_value()) {
            const auto free_blocks_delta = overrides.free_bytes_delta.value()
                                           / ssize_t(stat.f_frsize);
            used_blocks -= free_blocks_delta;
        }

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

void node::set_statvfs_overrides(disk_type type, statvfs_overrides overrides) {
    switch (type) {
    case disk_type::data:
        _data_overrides = overrides;
        break;
    case disk_type::cache:
        _cache_overrides = overrides;
        break;
    }
}

} // namespace storage
