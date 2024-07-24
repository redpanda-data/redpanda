/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/fwd.h"
#include "cluster/health_monitor_types.h"
#include "cluster/metadata_dissemination_types.h"
#include "config/tls_config.h"
#include "features/fwd.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/fwd.h"
#include "raft/notification.h"
#include "rpc/fwd.h"
#include "utils/mutex.h"
#include "utils/retry.h"
#include "utils/unresolved_address.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>

namespace cluster {

/// Implementation of metadata dissemination service.
/// This service handles leadership updates on remote nodes that doesn't have
/// instances of raft group that current node have. Instace of raft group
/// triggers leadership notification and by that mean updates leadership in
/// metadata cache.
/// The service caches all leadership updates and sends them as
/// batch per node, every configurable period of time. This service is also
/// responsible for querying one of the cluster nodes for current leadership
/// metadata when node has started.
///
/// Used acronymes:
/// RG<num> - raft group with <num> id
///
/// Exemplary dissemination scenario:
///
///
/// - RG1 has replication factor of 3 is handled at nodes [1,2,3]
/// - Cluster contain five nodes [1,2,3,4,5]
/// - New leader for RG1 was elected, node 2 is new leader
/// - Information about leadership is available on each node that have RG1
///   instance
/// - Nodes without RG1 instance (non overlapping nodes) [4,5]
/// - Dissemination service will distribute metadata information to nodes 4 & 5
///
///                    Dissemination requests <RG1 leader = 2>
///                    +--------------------------------------+
///                    |                                      |
///                    +-------------------------+            |
///                    |                         v            v
/// +1--------+   +2--------+  +3--------+  +4---+----+  +5---+----+
/// | +-----+ |   | +-----+ |  | +-----+ |  |         |  |         |
/// | |     | |   | |     | |  | |     | |  |  No RG1 |  |  No RG1 |
/// | | RG1 | |   | | RG1 | |  | | RG1 | |  |         |  |         |
/// | |     | |   | |     | |  | |     | |  |         |  |         |
/// | +-----+ |   | +-----+ |  | +-----+ |  |         |  |         |
/// +---------+   +---------+  +---------+  +---------+  +---------+
///                New leader

class metadata_dissemination_service final
  : public ss::peering_sharded_service<metadata_dissemination_service> {
public:
    metadata_dissemination_service(
      ss::sharded<raft::group_manager>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<members_table>&,
      ss::sharded<topic_table>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<health_monitor_frontend>&,
      ss::sharded<features::feature_table>&);

    void disseminate_leadership(
      model::ntp,
      model::revision_id,
      model::term_id,
      std::optional<model::node_id>);

    void initialize_leadership_metadata();

    ss::future<> start();
    ss::future<> stop();

private:
    // Used to store pending updates
    // When update was delivered successfully the finished flag is set to true
    // and object is removed from pending updates map
    struct update_retry_meta {
        ss::chunked_fifo<ntp_leader_revision> updates;
        bool finished = false;
    };
    // Used to track the process of requesting update when redpanda starts
    // when update using a node from ids will fail we will try the next one
    struct request_retry_meta {
        using container_t = std::vector<net::unresolved_address>;
        using const_iterator = container_t::const_iterator;
        container_t addresses;
        bool success = false;
        const_iterator next;
        exp_backoff_policy backoff_policy;
    };

    using broker_updates_t
      = absl::flat_hash_map<model::node_id, update_retry_meta>;

    void handle_leadership_notification(
      model::ntp,
      model::revision_id,
      model::term_id,
      std::optional<model::node_id>);
    ss::future<> apply_leadership_notification(
      model::ntp,
      model::revision_id,
      model::term_id,
      std::optional<model::node_id>);

    void collect_pending_updates();
    void cleanup_finished_updates();
    ss::future<> dispatch_disseminate_leadership();
    ss::future<> dispatch_one_update(model::node_id, update_retry_meta&);
    ss::future<result<get_leadership_reply>>
      dispatch_get_metadata_update(net::unresolved_address);
    ss::future<> do_request_metadata_update(request_retry_meta&);
    ss::future<>
    process_get_update_reply(result<get_leadership_reply>, request_retry_meta&);

    ss::future<>
      update_metadata_with_retries(std::vector<net::unresolved_address>);

    ss::future<> update_leaders_with_health_report(cluster_health_report);

    ss::sharded<raft::group_manager>& _raft_manager;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<members_table>& _members_table;
    ss::sharded<topic_table>& _topics;
    ss::sharded<rpc::connection_cache>& _clients;
    ss::sharded<health_monitor_frontend>& _health_monitor;
    ss::sharded<features::feature_table>& _feature_table;
    model::broker _self;
    std::chrono::milliseconds _dissemination_interval;
    config::tls_config _rpc_tls_config;
    ss::chunked_fifo<ntp_leader_revision> _requests;
    std::vector<net::unresolved_address> _seed_servers;
    broker_updates_t _pending_updates;
    mutex _lock{"metadata_dissemination_service"};
    ss::timer<> _dispatch_timer;
    ss::abort_source _as;
    ss::gate _bg;
    raft::group_manager_notification_id _notification_handle;
};

} // namespace cluster
