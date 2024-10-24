/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "base/units.h"
#include "cluster/node/types.h"
#include "config/property.h"
#include "storage/node.h"
#include "storage/types.h"

#include <seastar/core/sstring.hh>

#include <sys/statvfs.h>

#include <chrono>
#include <functional>

// Local node state monitoring is kept separate in case we want to access it
// pre-quorum formation in the future, etc.
namespace cluster::node {

class local_monitor {
public:
    static const ss::shard_id shard = 0;

    local_monitor(
      config::binding<size_t> min_bytes_alert,
      config::binding<unsigned> min_percent_alert,
      ss::sharded<storage::node>&);

    local_monitor(const local_monitor&) = delete;
    local_monitor(local_monitor&&) = default;
    ~local_monitor() = default;
    local_monitor& operator=(const local_monitor&) = delete;
    local_monitor& operator=(local_monitor&&) = delete;

    ss::future<> start();
    ss::future<> stop();

    ss::future<> update_state();
    const local_state& get_state_cached() const;
    static void update_alert(storage::disk&);

    static constexpr std::string_view stable_alert_string
      = "storage space alert"; // for those who grep the logs..

    /*
     * used by the disk space manager to report the latest information about the
     * data log configuration and usage so it can be added to health report.
     */
    void set_log_data_state(std::optional<local_state::log_data_state> state) {
        _log_data_state = state;
    }

private:
    /// Periodically check node status until stopped by abort source
    ss::future<> _update_loop();

    ss::future<struct statvfs> get_statvfs(const ss::sstring);
    ss::future<> update_disks(local_state& state);
    void update_alert_state(local_state&);
    storage::disk statvfs_to_disk(const storage::node::stat_info&);

    ss::future<> update_disk_metrics();
    float percent_free(const storage::disk& disk);
    void maybe_log_space_error(const storage::disk&);

    // state
    local_state _state;
    // folded/merged into _state by the monitor.
    std::optional<local_state::log_data_state> _log_data_state{std::nullopt};

    ss::logger::rate_limit _despam_interval = ss::logger::rate_limit(
      std::chrono::hours(1));
    config::binding<size_t> _free_bytes_alert_threshold;
    config::binding<unsigned> _free_percent_alert_threshold;

    ss::sharded<storage::node>& _storage_node_api; // single instance

    ss::gate _gate;
    ss::abort_source _abort_source;
};

} // namespace cluster::node
