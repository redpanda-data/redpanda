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

#include <seastar/core/abort_source.hh>

// Local node state monitoring is kept separate in case we want to access it
// pre-quorum formation in the future, etc.
namespace cluster::node {

class local_monitor {
public:
    local_monitor() = default;
    local_monitor(local_monitor&) = default;
    local_monitor(local_monitor&&) = default;
    ~local_monitor() = default;
    local_monitor& operator=(local_monitor const&) = default;
    local_monitor& operator=(local_monitor&&) = default;

    void update_state();
    local_state get_state_cached() const;

private:
    std::vector<disk> get_disks();
    local_state _state;
};

} // namespace cluster::node