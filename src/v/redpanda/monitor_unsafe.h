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

#pragma once

#include "config/property.h"
#include "features/feature_table.h"

#include <seastar/core/sharded.hh>

class monitor_unsafe {
public:
    static constexpr ss::shard_id backend_shard = 0;
    // Flag introduced in version v23.2.1 (cluster version 10)
    static constexpr cluster::cluster_version flag_introduction_version
      = to_cluster_version(features::release_version::v23_2_1);
    explicit monitor_unsafe(
      ss::sharded<features::feature_table>& feature_table);

    static void invoke_unsafe_log_update(
      cluster::cluster_version original_version, bool flag_value);
    ss::future<> start();
    ss::future<> stop();

private:
    void unsafe_log_update();
    void log_development_feature_warning();
    ss::future<> maybe_log_unsafe_nag();

    ss::sharded<features::feature_table>& _feature_table;
    config::binding<bool> _legacy_permit_unsafe_log_operation;
    ss::abort_source _as;
    ss::gate _gate;
};
