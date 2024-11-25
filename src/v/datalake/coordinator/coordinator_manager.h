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

#include "cloud_io/remote.h"
#include "cluster/fwd.h"
#include "cluster/notification.h"
#include "datalake/coordinator/coordinator.h"
#include "datalake/fwd.h"
#include "iceberg/catalog.h"
#include "model/fundamental.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "raft/fwd.h"
#include "raft/notification.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace schema {
class registry;
}

namespace datalake::coordinator {
class catalog_factory;

// Manages the lifecycle of datalake coordinators, each of which operate on a
// single partition of the control topic.
class coordinator_manager {
public:
    coordinator_manager(
      model::node_id self,
      ss::sharded<raft::group_manager>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<cluster::topic_table>&,
      ss::sharded<cluster::topics_frontend>&,
      pandaproxy::schema_registry::api*,
      std::unique_ptr<catalog_factory>,
      ss::sharded<cloud_io::remote>&,
      cloud_storage_clients::bucket_name);
    ~coordinator_manager();

    ss::future<> start();
    ss::future<> stop();

    ss::lw_shared_ptr<coordinator> get(const model::ntp&) const;

private:
    void start_managing(cluster::partition&);
    void stop_managing(const model::ntp&);
    void notify_leadership_change(
      raft::group_id group,
      model::term_id term,
      std::optional<model::node_id> leader_id);
    ss::future<checked<std::nullopt_t, coordinator::errc>>
    remove_tombstone(const model::topic&, model::revision_id);

    ss::gate gate_;
    model::node_id self_;
    raft::group_manager& gm_;
    cluster::partition_manager& pm_;
    cluster::topic_table& topics_;
    ss::sharded<cluster::topics_frontend>& topics_fe_;
    std::unique_ptr<schema::registry> schema_registry_;

    // Underlying IO is expected to outlive this class.
    iceberg::manifest_io manifest_io_;
    std::unique_ptr<catalog_factory> catalog_factory_;
    std::unique_ptr<iceberg::catalog> catalog_;
    std::unique_ptr<schema_manager> schema_mgr_;
    std::unique_ptr<type_resolver> type_resolver_;
    std::unique_ptr<table_creator> table_creator_;
    std::unique_ptr<file_committer> file_committer_;

    std::optional<cluster::notification_id_type> manage_notifications_;
    std::optional<cluster::notification_id_type> unmanage_notifications_;
    std::optional<raft::group_manager_notification_id>
      leadership_notifications_;

    absl::btree_map<model::ntp, ss::lw_shared_ptr<coordinator>> coordinators_;
};

} // namespace datalake::coordinator
