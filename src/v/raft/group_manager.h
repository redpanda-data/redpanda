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
#include "cluster/types.h"
#include "metrics/metrics.h"
#include "model/metadata.h"
#include "raft/consensus_client_protocol.h"
#include "raft/heartbeat_manager.h"
#include "raft/recovery_memory_quota.h"
#include "raft/recovery_scheduler.h"
#include "raft/timeout_jitter.h"
#include "raft/types.h"
#include "rpc/fwd.h"
#include "storage/fwd.h"
#include "utils/notification_list.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/scheduling.hh>

#include <absl/container/flat_hash_map.h>

#include <tuple>

namespace raft {

/*
 * Owns and manages all raft groups.
 */
class group_manager {
public:
    using leader_cb_t = ss::noncopyable_function<void(
      raft::group_id, model::term_id, std::optional<model::node_id>)>;

    struct configuration {
        config::binding<std::chrono::milliseconds> heartbeat_interval;
        config::binding<std::chrono::milliseconds> heartbeat_timeout;
        config::binding<std::chrono::milliseconds> raft_io_timeout_ms;
        config::binding<bool> enable_lw_heartbeat;
        config::binding<size_t> recovery_concurrency_per_shard;
        config::binding<std::chrono::milliseconds> election_timeout_ms;
        config::binding<std::optional<size_t>> replica_max_not_flushed_bytes;
        config::binding<std::chrono::milliseconds> flush_timer_interval_ms;
    };
    using config_provider_fn = ss::noncopyable_function<configuration()>;

    group_manager(
      model::node_id self,
      ss::scheduling_group raft_scheduling_group,
      config_provider_fn,
      recovery_memory_quota::config_provider_fn recovery_mem_cfg,
      ss::sharded<rpc::connection_cache>& clients,
      ss::sharded<storage::api>& storage,
      ss::sharded<coordinated_recovery_throttle>&,
      ss::sharded<features::feature_table>&);

    ss::future<> start();
    ss::future<> stop();
    void set_ready();
    ss::future<> stop_heartbeats();

    ss::future<ss::lw_shared_ptr<raft::consensus>> create_group(
      raft::group_id id,
      std::vector<model::broker> nodes,
      ss::shared_ptr<storage::log> log,
      with_learner_recovery_throttle enable_learner_recovery_throttle,
      keep_snapshotted_log = keep_snapshotted_log::no);

    ss::future<> shutdown(ss::lw_shared_ptr<raft::consensus>);

    ss::future<> remove(ss::lw_shared_ptr<raft::consensus>);

    cluster::notification_id_type
    register_leadership_notification(leader_cb_t cb) {
        for (auto& gr : _groups) {
            cb(gr->group(), gr->term(), gr->get_leader_id());
        }
        auto id = _notifications.register_cb(std::move(cb));
        return id;
    }

    void unregister_leadership_notification(cluster::notification_id_type id) {
        _notifications.unregister_cb(id);
    }

    recovery_status get_recovery_status() {
        return _recovery_scheduler.get_status();
    }

private:
    void trigger_leadership_notification(raft::leadership_status);
    void setup_metrics();

    ss::future<> flush_groups();
    void collect_learner_metrics();

    raft::group_configuration create_initial_configuration(
      std::vector<model::broker>, model::revision_id) const;
    mutex _groups_mutex;
    model::node_id _self;
    ss::scheduling_group _raft_sg;
    raft::consensus_client_protocol _client;
    configuration _configuration;
    raft::heartbeat_manager _heartbeats;
    ss::gate _gate;
    std::vector<ss::lw_shared_ptr<raft::consensus>> _groups;
    notification_list<leader_cb_t, cluster::notification_id_type>
      _notifications;
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
    storage::api& _storage;
    coordinated_recovery_throttle& _recovery_throttle;
    recovery_memory_quota _recovery_mem_quota;
    recovery_scheduler _recovery_scheduler;
    features::feature_table& _feature_table;
    ss::timer<clock_type> _flush_timer;
    timeout_jitter _flush_timer_jitter;
    std::chrono::milliseconds _metric_collection_interval;
    ss::timer<> _metrics_timer;
    size_t _learners_gap_bytes{0};
    bool _is_ready;
};

} // namespace raft
