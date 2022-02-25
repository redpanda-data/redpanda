/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "cluster/node/types.h"
#include "config/property.h"
#include "units.h"

#include <seastar/core/sstring.hh>

#include <sys/statvfs.h>

#include <chrono>
#include <functional>

// Local node state monitoring is kept separate in case we want to access it
// pre-quorum formation in the future, etc.
namespace cluster::node {

class local_monitor {
public:
    local_monitor(
      config::binding<size_t> min_bytes,
      config::binding<unsigned> min_percent,
      ss::sharded<storage::node_api>& api);
    local_monitor(local_monitor&) = default;
    local_monitor(local_monitor&&) = default;
    ~local_monitor() = default;
    local_monitor& operator=(local_monitor const&) = delete;
    local_monitor& operator=(local_monitor&&) = delete;

    ss::future<> update_state();
    const local_state& get_state_cached() const;

    static constexpr std::string_view stable_alert_string
      = "storage space alert"; // for those who grep the logs..

    void testing_only_set_path(const ss::sstring& path);
    void testing_only_set_statvfs(
      std::function<struct statvfs(const ss::sstring)>);

private:
    // helpers
    std::tuple<size_t, size_t>
    minimum_free_by_bytes_and_percent(size_t bytes_available) const;
    ss::future<std::vector<storage::disk>> get_disks();
    ss::future<struct statvfs> get_statvfs(const ss::sstring);
    void update_alert_state();
    ss::future<> update_disk_metrics();
    void maybe_log_space_error(const storage::disk&);

    // configuration
    void refresh_configuration();
    std::size_t get_config_alert_threshold_bytes();
    unsigned get_config_alert_threshold_percent();

    // state
    local_state _state;
    ss::logger::rate_limit despam_interval = ss::logger::rate_limit(
      std::chrono::hours(1));
    config::binding<size_t> _free_bytes_alert_threshold;
    config::binding<unsigned> _free_percent_alert_threshold;

    ss::sharded<storage::node_api>& _storage_api; // single instance

    // Injection points for unit tests
    ss::sstring _path_for_test;
    std::function<struct statvfs(const ss::sstring)> _statvfs_for_test;
};

} // namespace cluster::node