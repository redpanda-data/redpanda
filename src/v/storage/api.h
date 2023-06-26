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

// Top-level sharded storage API.
class api : public ss::peering_sharded_service<api> {
public:
    explicit api(
      std::function<kvstore_config()> kv_conf_cb,
      std::function<log_config()> log_conf_cb,
      ss::sharded<features::feature_table>& feature_table,
      config::binding<std::optional<uint64_t>> log_storage_max_size) noexcept
      : _kv_conf_cb(std::move(kv_conf_cb))
      , _log_conf_cb(std::move(log_conf_cb))
      , _feature_table(feature_table)
      , _log_storage_max_size(std::move(log_storage_max_size)) {}

    ss::future<> start();
    ss::future<> stop();

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

    bool max_size_exceeded() const;
    bool high_watermark_exceeded() const;

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

    ss::gate _gate;
    ss::future<> monitor();
    ssx::semaphore _monitor_sem{0, "storage::api::monitor"};
    config::binding<std::optional<uint64_t>> _log_storage_max_size;
    // updated lazily by monitor on core 0
    bool _max_size_exceeded{false};
    bool _high_watermark_exceeded{false};
};

} // namespace storage
