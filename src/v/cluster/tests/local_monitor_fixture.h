/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/node/local_monitor.h"
#include "storage/api.h"

#include <seastar/core/sstring.hh>

#include <string_view>

struct local_monitor_fixture {
    local_monitor_fixture();
    local_monitor_fixture(const local_monitor_fixture&) = delete;
    ~local_monitor_fixture();

    std::filesystem::path _test_path;
    ss::sharded<features::feature_table> _feature_table;
    ss::sharded<storage::api> _storage_api;
    ss::sharded<storage::node_api> _storage_node_api;
    cluster::node::local_monitor _local_monitor;

    cluster::node::local_state update_state();
    static struct statvfs
    make_statvfs(size_t blk_free, size_t blk_total, size_t blk_size);

    static void set_config_free_thresholds(
      unsigned alert_percent, size_t alert_bytes, size_t min_bytes);
    void assert_space_alert(
      size_t volume,
      size_t bytes_alert,
      size_t percent_alert_bytes,
      size_t min_bytes,
      size_t free,
      storage::disk_space_alert expected);
};
