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
#include "units.h"

#include <seastar/core/sstring.hh>

#include <sys/statvfs.h>

// Local node state monitoring is kept separate in case we want to access it
// pre-quorum formation in the future, etc.
namespace cluster::node {

class local_monitor {
public:
    local_monitor() = default;
    local_monitor(local_monitor&) = delete;
    local_monitor(local_monitor&&) = default;
    ~local_monitor() = default;
    local_monitor& operator=(local_monitor const&) = delete;
    local_monitor& operator=(local_monitor&&) = default;

    ss::future<> update_state();
    const local_state& get_state_cached() const;

    // Visible for test...
    static constexpr double alert_min_free_space_percent = 0.05;
    static constexpr double alert_min_free_space_bytes = 1_GiB;

    void set_path_for_test(const ss::sstring& path);
    void
      set_statvfs_for_test(std::function<struct statvfs(const ss::sstring&)>);

private:
    ss::future<std::vector<disk>> get_disks();
    ss::future<struct statvfs> get_statvfs(const ss::sstring&);
    void update_alert_state();

    local_state _state;

    // Injection points for unit tests
    ss::sstring _path_for_test;
    std::optional<std::function<struct statvfs(const ss::sstring&)>>
      _statvfs_for_test;
};

} // namespace cluster::node