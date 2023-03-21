
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
#include "cluster/node/local_monitor.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "raft/consensus.h"
#include "rpc/fwd.h"
#include "ssx/semaphore.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>

#include <chrono>
#include <memory>
#include <vector>
namespace cluster {

using health_node_cb_t = ss::noncopyable_function<void(
  node_health_report const&,
  std::optional<std::reference_wrapper<const node_health_report>>)>;

/**
 * Health monitor backend is responsible for collecting cluster health status
 * and caching cluster health information.
 *
 * Health monitor status collection is active only on the node which is a
 * controller partition leader. When any other node is requesting a cluster
 * report it either uses locally cached state or asks controller leader for
 * new report.
 */
class health_monitor_backend {
public:
    static constexpr ss::shard_id shard{0};

    health_monitor_backend(
      ss::lw_shared_ptr<raft::consensus>,
      ss::sharded<members_table>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_manager>&,
      ss::sharded<raft::group_manager>&,
      ss::sharded<ss::abort_source>&,
      ss::sharded<node::local_monitor>&,
      ss::sharded<drain_manager>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<topic_table>&);

    ss::future<> stop();

    ss::future<result<cluster_health_report>> get_cluster_health(
      cluster_report_filter, force_refresh, model::timeout_clock::time_point);

    ss::future<storage::disk_space_alert> get_cluster_disk_health(
      force_refresh refresh, model::timeout_clock::time_point deadline);

    ss::future<result<node_health_report>>
      collect_current_node_health(node_report_filter);

    cluster::notification_id_type register_node_callback(health_node_cb_t cb);
    void unregister_node_callback(cluster::notification_id_type id);

    ss::future<result<std::optional<cluster::drain_manager::drain_status>>>
      get_node_drain_status(model::node_id, model::timeout_clock::time_point);

    ss::future<cluster_health_overview>
      get_cluster_health_overview(model::timeout_clock::time_point);

    bool does_raft0_have_leader();

    ss::future<> maybe_refresh_cloud_health_stats();

private:
    /**
     * Struct used to track pending refresh request, it gives ability
     */
    struct abortable_refresh_request
      : ss::enable_lw_shared_from_this<abortable_refresh_request> {
        abortable_refresh_request(
          model::node_id, ss::gate::holder, ssx::semaphore_units);

        ss::future<std::error_code>
          abortable_await(ss::future<std::error_code>);
        void abort();

        bool finished = false;
        model::node_id leader_id;
        ss::gate::holder holder;
        ssx::semaphore_units units;
        ss::promise<std::error_code> done;
    };

    struct reply_status {
        ss::lowres_clock::time_point last_reply_timestamp
          = ss::lowres_clock::time_point::min();
        alive is_alive = alive::no;
    };

    using status_cache_t = absl::node_hash_map<model::node_id, node_state>;
    using report_cache_t
      = absl::node_hash_map<model::node_id, node_health_report>;

    using last_reply_cache_t
      = absl::node_hash_map<model::node_id, reply_status>;

    void tick();
    ss::future<std::error_code> collect_cluster_health();
    ss::future<result<node_health_report>>
      collect_remote_node_health(model::node_id);
    ss::future<std::error_code> maybe_refresh_cluster_health(
      force_refresh, model::timeout_clock::time_point);
    ss::future<std::error_code> refresh_cluster_health_cache(force_refresh);
    ss::future<std::error_code>
      dispatch_refresh_cluster_health_request(model::node_id);

    cluster_health_report build_cluster_report(const cluster_report_filter&);

    std::optional<node_health_report>
    build_node_report(model::node_id, const node_report_filter&);

    ss::future<std::vector<topic_status>>
      collect_topic_status(partitions_filter);

    void refresh_nodes_status();

    result<node_health_report>
      process_node_reply(model::node_id, result<get_node_health_reply>);

    std::chrono::milliseconds max_metadata_age();
    void abort_current_refresh();

    void on_leadership_changed(
      raft::group_id, model::term_id, std::optional<model::node_id>);

    ss::lw_shared_ptr<raft::consensus> _raft0;
    ss::sharded<members_table>& _members;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_manager>& _partition_manager;
    ss::sharded<raft::group_manager>& _raft_manager;
    ss::sharded<ss::abort_source>& _as;
    ss::sharded<drain_manager>& _drain_manager;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<partition_leaders_table>& _partition_leaders_table;
    ss::sharded<topic_table>& _topic_table;

    ss::lowres_clock::time_point _last_refresh;
    ss::lw_shared_ptr<abortable_refresh_request> _refresh_request;
    cluster::notification_id_type _leadership_notification_handle;

    status_cache_t _status;
    report_cache_t _reports;
    storage::disk_space_alert _reports_disk_health
      = storage::disk_space_alert::ok;
    last_reply_cache_t _last_replies;
    std::optional<size_t> _bytes_in_cloud_storage;

    ss::gate _gate;
    mutex _refresh_mutex;
    ss::sharded<node::local_monitor>& _local_monitor;

    std::vector<std::pair<cluster::notification_id_type, health_node_cb_t>>
      _node_callbacks;
    cluster::notification_id_type _next_callback_id{0};
};
} // namespace cluster
