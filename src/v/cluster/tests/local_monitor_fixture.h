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
    static constexpr unsigned default_percent_alert = 5;
    static constexpr size_t default_bytes_alert = 2_GiB;
    static constexpr size_t default_min_bytes = 1_GiB;

    std::filesystem::path _test_path;
    ss::sharded<storage::node_api> _storage_api;
    cluster::node::local_monitor _local_monitor;

    cluster::node::local_state update_state();
    static struct statvfs make_statvfs(
      unsigned long blk_free, unsigned long blk_total, unsigned long blk_size);
    void set_config_alert_thresholds(unsigned percent, size_t bytes);
};
