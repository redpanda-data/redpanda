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
#include "seastarx.h"
#include "ssx/semaphore.h"
#include "storage/node.h"

#include <seastar/core/sharded.hh>

#include <iostream>

namespace cloud_storage {
class cache;
}

namespace cluster {
class partition_manager;
}

namespace storage {

class api;
class node;

/*
 *
 */
class disk_space_manager {
    static constexpr ss::shard_id run_loop_core = 0;

public:
    disk_space_manager(
      config::binding<bool> enabled,
      ss::sharded<storage::api>* storage,
      ss::sharded<storage::node>* storage_node,
      ss::sharded<cloud_storage::cache>* cache,
      ss::sharded<cluster::partition_manager>* pm);

    disk_space_manager(disk_space_manager&&) noexcept = delete;
    disk_space_manager& operator=(disk_space_manager&&) noexcept = delete;
    disk_space_manager(const disk_space_manager&) = delete;
    disk_space_manager& operator=(const disk_space_manager&) = delete;
    ~disk_space_manager() = default;

    ss::future<> start();
    ss::future<> stop();

private:
    config::binding<bool> _enabled;
    ss::sharded<storage::api>* _storage;
    ss::sharded<storage::node>* _storage_node;
    ss::sharded<cloud_storage::cache>* _cache;
    ss::sharded<cluster::partition_manager>* _pm;

    ss::gate _gate;
    ss::future<> run_loop();
    ssx::semaphore _control_sem{0, "resource_mgmt::space_manager"};
};

} // namespace storage
