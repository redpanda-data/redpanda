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
#pragma once

#include "resource_mgmt/storage.h"
#include "seastarx.h"
#include "storage/probe.h"
#include "utils/named_type.h"
#include "utils/notification_list.h"

#include <seastar/core/future.hh>

namespace storage {

/**
 * Node-wide storage state: only maintained on shard 0.
 *
 * The disk usage stats in this class are periodically updated
 * from the code in cluster::node.
 */
class node {
public:
    enum class disk_type : uint8_t {
        data = 0,
        cache = 1,
    };

    ss::future<> start() {
        _probe.setup_node_metrics();
        return ss::now();
    }

    ss::future<> stop() { return ss::now(); }

    using notification_id = named_type<int32_t, struct notification_id_t>;
    using disk_cb_t
      = ss::noncopyable_function<void(uint64_t, uint64_t, disk_space_alert)>;

    void set_disk_metrics(
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

    const storage::disk_metrics& get_disk_metrics() const {
        return _probe.get_disk_metrics();
    };

    notification_id register_disk_notification(disk_type t, disk_cb_t cb) {
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

    void unregister_disk_notification(disk_type t, notification_id id) {
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

private:
    storage::node_probe _probe;

    notification_list<disk_cb_t, notification_id> _data_watchers;
    notification_list<disk_cb_t, notification_id> _cache_watchers;
};

} // namespace storage
