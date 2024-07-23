/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/archival/probe.h"
#include "cluster/archival/types.h"
#include "cluster/fwd.h"
#include "cluster/partition_leaders_table.h"
#include "container/fragmented_vector.h"

#include <seastar/core/shared_ptr.hh>

namespace archival {

/// Maintain an archiver instance per partition. Get notifications
/// on membership/leadership changes and make corresponding changes
/// to the set of managed instances.
class archiver_manager {
    friend struct managed_partition;
    friend struct managed_partition_fsm;
    class impl;

public:
    archiver_manager(
      model::node_id self,
      ss::sharded<cluster::partition_manager>& pm,
      ss::sharded<raft::group_manager>& gm,
      ss::sharded<cloud_storage::remote>& api,
      ss::sharded<cloud_storage::cache>& cache,
      ss::sharded<archival::upload_housekeeping_service>& upload_housekeeping,
      ss::lw_shared_ptr<const configuration> config);
    ~archiver_manager();
    ss::future<> start();
    ss::future<> stop();

    /// Snapshot of managed partitions
    fragmented_vector<model::ntp> managed_partitions() const;

    /// Snapshot of managed partitions which are leaders
    fragmented_vector<model::ntp> leader_partitions() const;

private:
    std::unique_ptr<impl> _impl;
};

} // namespace archival
