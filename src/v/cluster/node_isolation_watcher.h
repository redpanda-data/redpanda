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

#include "cluster/health_monitor_frontend.h"
#include "cluster/metadata_cache.h"
#include "cluster/node_status_table.h"
#include "seastarx.h"

namespace cluster {

class node_isolation_watcher {
public:
    node_isolation_watcher(
      ss::sharded<metadata_cache>& metadata_cache,
      ss::sharded<health_monitor_frontend>& health_monitor,
      ss::sharded<node_status_table>& node_status_table);

    void start();
    ss::future<> stop();

private:
    void start_isolation_watch_timer();
    void rearm_isolation_watch_timer();

    void update_isolation_status();
    ss::future<> do_update_isolation_status();
    ss::future<bool> is_node_isolated();

    ss::gate _gate;
    ss::timer<model::timeout_clock> _isolation_watch_timer;
    std::chrono::milliseconds _isolation_check_ms{700};

    ss::sharded<metadata_cache>& _metadata_cache;
    ss::sharded<health_monitor_frontend>& _health_monitor;
    ss::sharded<node_status_table>& _node_status_table;

    bool _last_is_isolated_status{false};
};

} // namespace cluster