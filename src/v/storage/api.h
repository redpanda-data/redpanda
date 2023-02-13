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

#include "features/feature_table.h"
#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/probe.h"
#include "storage/storage_resources.h"
#include "utils/notification_list.h"

namespace storage {

/**
 * Node-wide storage state: only maintained on shard 0.
 *
 * The disk usage stats in this class are periodically updated
 * from the code in cluster::node.
 */
class node_api {
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

// Top-level sharded storage API.
class api {
public:
    explicit api(
      std::function<kvstore_config()> kv_conf_cb,
      std::function<log_config()> log_conf_cb,
      ss::sharded<features::feature_table>& feature_table) noexcept
      : _kv_conf_cb(std::move(kv_conf_cb))
      , _log_conf_cb(std::move(log_conf_cb))
      , _feature_table(feature_table) {}

    ss::future<> start() {
        _kvstore = std::make_unique<kvstore>(
          _kv_conf_cb(), _resources, _feature_table);
        return _kvstore->start().then([this] {
            _log_mgr = std::make_unique<log_manager>(
              _log_conf_cb(), kvs(), _resources, _feature_table);
        });
    }

    ss::future<> stop() {
        auto f = ss::now();
        if (_log_mgr) {
            f = _log_mgr->stop();
        }
        if (_kvstore) {
            return f.then([this] { return _kvstore->stop(); });
        }
        return f;
    }

    void set_node_uuid(const model::node_uuid& node_uuid) {
        _node_uuid = node_uuid;
    }

    model::node_uuid node_uuid() const { return _node_uuid; }

    void set_cluster_uuid(const model::cluster_uuid& cluster_uuid) {
        _cluster_uuid = cluster_uuid;
    }
    const std::optional<model::cluster_uuid>& get_cluster_uuid() const {
        return _cluster_uuid;
    }

    kvstore& kvs() { return *_kvstore; }
    log_manager& log_mgr() { return *_log_mgr; }
    storage_resources& resources() { return _resources; }

private:
    storage_resources _resources;

    std::function<kvstore_config()> _kv_conf_cb;
    std::function<log_config()> _log_conf_cb;
    ss::sharded<features::feature_table>& _feature_table;

    std::unique_ptr<kvstore> _kvstore;
    std::unique_ptr<log_manager> _log_mgr;

    // UUID that uniquely identifies the contents of this node's data
    // directory. Should be generated once upon first starting up and
    // immediately persisted into `_kvstore`.
    model::node_uuid _node_uuid;

    std::optional<model::cluster_uuid> _cluster_uuid;
};

} // namespace storage
