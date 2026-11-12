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

#include "base/seastarx.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "storage/chunk_cache.h"
#include "storage/disk.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/storage_resources.h"
#include "utils/notification_list.h"

#include <seastar/core/condition-variable.hh>

namespace storage {

// Top-level sharded storage API.
class api : public ss::peering_sharded_service<api> {
public:
    explicit api(
      std::function<kvstore_config()> kv_conf_cb,
      std::function<log_config()> log_conf_cb,
      ss::sharded<features::feature_table>& feature_table) noexcept
      : _kv_conf_cb(std::move(kv_conf_cb))
      , _log_conf_cb(std::move(log_conf_cb))
      , _feature_table(feature_table)
      , _kvstore(
          std::make_unique<kvstore>(
            _kv_conf_cb(), ss::this_shard_id(), _resources, _feature_table)) {}

    ss::future<> start(
      internal::chunk_cache::prealloc prealloc
      = internal::chunk_cache::prealloc::no) {
        // Start the per-shard chunk cache before anything can write segments.
        co_await _resources.start(prealloc);
        if (!_kvstore) {
            _kvstore = std::make_unique<kvstore>(
              _kv_conf_cb(), ss::this_shard_id(), _resources, _feature_table);
        }
        co_await _kvstore->start();
        if (!_log_mgr) {
            _log_mgr = std::make_unique<log_manager>(
              _log_conf_cb(), kvs(), _resources, _feature_table);
        }
        co_await _log_mgr->start();
    }

    ss::future<> recover_kvstore() { co_await _kvstore->recover(); }

    ss::future<std::unique_ptr<storage::kvstore>>
    make_extra_kvstore(ss::shard_id s) {
        vassert(
          s >= ss::this_smp_shard_count(),
          "can't make extra kvstore for existing shard {}",
          s);
        auto kvs = std::make_unique<kvstore>(
          _kv_conf_cb(), s, _resources, _feature_table);
        co_await kvs->start();
        co_return kvs;
    }

    void stop_cluster_uuid_waiters() { _has_cluster_uuid_cond.broken(); }

    ss::future<> stop() {
        stop_cluster_uuid_waiters();
        if (_log_mgr) {
            co_await _log_mgr->stop();
        }
        if (_kvstore) {
            co_await _kvstore->stop();
        }

        co_await _resources.stop();
    }

    void reset() {
        _log_mgr.reset();
        _kvstore.reset();
    }

    /// Stamp this shard's copy of the node identity. UUID and boot id are
    /// always written together at bootstrap, so they share a setter.
    void set_node_identity(model::node_uuid uuid, model::node_boot_id boot) {
        _node_uuid = uuid;
        _node_boot_id = boot;
    }

    model::node_uuid node_uuid() const { return _node_uuid; }
    model::node_boot_id node_boot_id() const { return _node_boot_id; }

    void set_cluster_uuid(const model::cluster_uuid& cluster_uuid) {
        _cluster_uuid = cluster_uuid;
        _has_cluster_uuid_cond.signal();
    }
    const std::optional<model::cluster_uuid>& get_cluster_uuid() const {
        return _cluster_uuid;
    }

    ss::future<bool> wait_for_cluster_uuid();

    kvstore& kvs() { return *_kvstore; }
    log_manager& log_mgr() { return *_log_mgr; }
    storage_resources& resources() { return _resources; }

    /*
     * Return disk space usage for kvstore and all logs. The information
     * returned is accumulated from all cores.
     */
    ss::future<usage_report> disk_usage();

    /*
     * Handle a disk notification (e.g. low space)
     */
    void handle_disk_notification(
      uint64_t total_space,
      uint64_t free_space,
      storage::disk_space_alert alert);

    /*
     * ask local log manager to trigger gc
     */
    void trigger_gc();

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

    // Per-boot id, persisted in `_kvstore` and bumped on every startup.
    // Stamped onto health reports so peers can distinguish a post-restart
    // report from a pre-restart one when the in-memory health version
    // counter has reset to 1.
    model::node_boot_id _node_boot_id;

    std::optional<model::cluster_uuid> _cluster_uuid;
    ss::condition_variable _has_cluster_uuid_cond;
};

} // namespace storage
