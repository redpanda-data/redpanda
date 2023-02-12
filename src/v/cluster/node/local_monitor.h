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
#include "cluster/node/types.h"
#include "config/property.h"
#include "resource_mgmt/storage.h"
#include "storage/types.h"
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
    static const ss::shard_id shard = 0;

    local_monitor(
      config::binding<size_t> min_bytes_alert,
      config::binding<unsigned> min_percent_alert,
      config::binding<size_t> min_bytes,
      ss::sstring data_directory,
      ss::sstring cache_directory,
      ss::sharded<storage::node_api>&,
      ss::sharded<storage::api>&);
    local_monitor(const local_monitor&) = delete;
    local_monitor(local_monitor&&) = default;
    ~local_monitor() = default;
    local_monitor& operator=(local_monitor const&) = delete;
    local_monitor& operator=(local_monitor&&) = delete;

    ss::future<> start();
    ss::future<> stop();

    ss::future<> update_state();
    const local_state& get_state_cached() const;
    static void update_alert(storage::disk&);

    static constexpr std::string_view stable_alert_string
      = "storage space alert"; // for those who grep the logs..

    void testing_only_set_path(const ss::sstring& path);
    void testing_only_set_statvfs(
      std::function<struct statvfs(const ss::sstring)>);

private:
    // helpers
    static size_t
    alert_percent_in_bytes(unsigned alert_percent, size_t bytes_available);

    /// Periodically check node status until stopped by abort source
    ss::future<> _update_loop();

    ss::future<struct statvfs> get_statvfs(const ss::sstring);
    ss::future<> update_disks(local_state& state);
    void update_alert_state(local_state&);
    storage::disk statvfs_to_disk(const struct statvfs& svfs);

    ss::future<> update_disk_metrics();
    float percent_free(const storage::disk& disk);
    void maybe_log_space_error(const storage::disk&);

    // state
    local_state _state;
    ss::logger::rate_limit _despam_interval = ss::logger::rate_limit(
      std::chrono::hours(1));
    config::binding<size_t> _free_bytes_alert_threshold;
    config::binding<unsigned> _free_percent_alert_threshold;
    config::binding<size_t> _min_free_bytes;

    // We must carry a copy of data dir, because fixture tests mutate the
    // global node_config::data_directory
    ss ::sstring _data_directory;
    ss ::sstring _cache_directory;

    ss::sharded<storage::node_api>& _storage_node_api; // single instance
    ss::sharded<storage::api>& _storage_api;

    // Injection points for unit tests
    ss::sstring _path_for_test;
    std::function<struct statvfs(const ss::sstring)> _statvfs_for_test;

    std::optional<size_t> _disk_size_for_test;

    ss::gate _gate;
    ss::abort_source _abort_source;
};

} // namespace cluster::node
