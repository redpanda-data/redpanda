// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"
#include "cluster/notification.h"
#include "model/fundamental.h"
#include "raft/fwd.h"
#include "raft/notification.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>

namespace cloud_io {
class remote;
}

namespace cluster {
class partition;
class partition_manager;
}; // namespace cluster

namespace experimental::cloud_topics::recovery {

class snapshot_io;
class managed_partition;

class snapshot_manager {
public:
    snapshot_manager(
      cluster::partition_manager*,
      raft::group_manager*,
      cloud_io::remote*,
      cloud_storage_clients::bucket_name);

    snapshot_manager(const snapshot_manager&) = delete;
    snapshot_manager& operator=(const snapshot_manager&) = delete;
    snapshot_manager(snapshot_manager&&) noexcept = delete;
    snapshot_manager& operator=(snapshot_manager&&) noexcept = delete;
    ~snapshot_manager();

public:
    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> scheduler_loop();
    ss::future<> scheduler_loop_once();

private:
    void attach_partition(ss::lw_shared_ptr<cluster::partition>);
    void detach_partition(const model::ntp&);
    void handle_partition_leader_change(const model::ntp&);

private:
    void schedule_snapshot(ss::lw_shared_ptr<managed_partition>);
    void schedule_recovery(ss::lw_shared_ptr<managed_partition>);

    ss::future<> take_snapshot(ss::lw_shared_ptr<managed_partition>);
    ss::future<> recover_snapshot(ss::lw_shared_ptr<managed_partition>);

private:
    cluster::partition_manager* _partition_manager;
    raft::group_manager* _raft_manager;

    // An indirection to avoid including snapshot_io.h here.
    std::unique_ptr<snapshot_io> _snapshot_io;

    absl::node_hash_map<model::ntp, ss::lw_shared_ptr<managed_partition>>
      _attached_partitions;
    absl::node_hash_map<model::ntp, ss::future<>> _snapshots_in_progress;

    cluster::notification_id_type _manage_notify_handle;
    cluster::notification_id_type _unmanage_notify_handle;
    raft::group_manager_notification_id _raft_manager_notify_handle;

    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace experimental::cloud_topics::recovery
