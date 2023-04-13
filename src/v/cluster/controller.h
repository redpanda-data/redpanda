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

#include "cluster/controller_probe.h"
#include "cluster/controller_stm.h"
#include "cluster/fwd.h"
#include "cluster/scheduling/leader_balancer.h"
#include "model/fundamental.h"
#include "raft/fwd.h"
#include "rpc/fwd.h"
#include "security/fwd.h"
#include "storage/api.h"
#include "storage/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <vector>

namespace cluster {

class cluster_discovery;

class controller {
public:
    controller(
      cluster::config_manager::preload_result&& config_preload,
      ss::sharded<rpc::connection_cache>& ccache,
      ss::sharded<partition_manager>& pm,
      ss::sharded<shard_table>& st,
      ss::sharded<storage::api>& storage,
      ss::sharded<node::local_monitor>& local_monitor,
      ss::sharded<raft::group_manager>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<cloud_storage::remote>&);

    model::node_id self() { return _raft0->self().id(); }
    ss::sharded<topics_frontend>& get_topics_frontend() { return _tp_frontend; }
    ss::sharded<members_manager>& get_members_manager() {
        return _members_manager;
    }

    ss::sharded<config_frontend>& get_config_frontend() {
        return _config_frontend;
    }
    ss::sharded<config_manager>& get_config_manager() {
        return _config_manager;
    }

    ss::sharded<members_table>& get_members_table() { return _members_table; }
    ss::sharded<topic_table>& get_topics_state() { return _tp_state; }
    ss::sharded<partition_leaders_table>& get_partition_leaders() {
        return _partition_leaders;
    }

    ss::sharded<security::credential_store>& get_credential_store() {
        return _credentials;
    }

    ss::sharded<security::ephemeral_credential_store>&
    get_ephemeral_credential_store() {
        return _ephemeral_credentials;
    }

    ss::sharded<ephemeral_credential_frontend>&
    get_ephemeral_credential_frontend() {
        return _ephemeral_credential_frontend;
    }

    ss::sharded<security_frontend>& get_security_frontend() {
        return _security_frontend;
    }

    ss::sharded<security::authorizer>& get_authorizer() { return _authorizer; }

    ss::sharded<controller_api>& get_api() { return _api; }

    ss::sharded<members_frontend>& get_members_frontend() {
        return _members_frontend;
    }

    ss::sharded<health_monitor_frontend>& get_health_monitor() {
        return _hm_frontend;
    }

    ss::sharded<feature_manager>& get_feature_manager() {
        return _feature_manager;
    }
    ss::sharded<features::feature_table>& get_feature_table() {
        return _feature_table;
    }

    ss::sharded<drain_manager>& get_drain_manager() { return _drain_manager; }

    ss::sharded<partition_manager>& get_partition_manager() {
        return _partition_manager;
    }

    ss::sharded<shard_table>& get_shard_table() { return _shard_table; }

    ss::sharded<partition_balancer_backend>& get_partition_balancer() {
        return _partition_balancer;
    }

    ss::sharded<ss::abort_source>& get_abort_source() { return _as; }

    ss::sharded<storage::api>& get_storage() { return _storage; }
    ss::sharded<members_backend>& get_members_backend() {
        return _members_backend;
    }

    bool is_raft0_leader() const {
        vassert(
          ss::this_shard_id() == ss::shard_id(0),
          "Raft 0 API can only be called from shard 0");
        return _raft0->is_elected_leader();
    }

    int16_t internal_topic_replication() const;

    ss::future<> wire_up();

    /**
     * Create raft0, and start the services that the \c controller owns.
     * \param initial_raft0_brokers Brokers to start raft0 with. Empty for
     *      non-seeds.
     * \param shard0_as an abort source only usable on shard0, and only for
     *        use within this start function -- for the rest of the lifetime
     *        of controller, it has its own member abort source.
     */
    ss::future<> start(cluster_discovery&, ss::abort_source&);

    // prevents controller from accepting new requests
    ss::future<> shutdown_input();
    ss::future<> stop();

    /**
     * Called when the node is ready to become a leader
     */
    ss::future<> set_ready();

    ss::future<model::offset> get_last_applied_offset() {
        return _stm.invoke_on(controller_stm_shard, [](auto& stm) {
            return stm.get_last_applied_offset();
        });
    }

    model::offset get_start_offset() const { return _raft0->start_offset(); }

    model::offset get_commited_index() const {
        return _raft0->committed_offset();
    }

private:
    friend controller_probe;

    /**
     * Create a \c bootstrap_cluster_cmd, replicate-and-wait it to the current
     * quorum, retry infinitely if replicate-and-wait fails.
     */
    ss::future<> create_cluster(bootstrap_cluster_cmd_data cmd_data);

    ss::future<> cluster_creation_hook(cluster_discovery& discovery);

    // Checks configuration invariants stored in kvstore
    ss::future<> validate_configuration_invariants();

    config_manager::preload_result _config_preload;

    ss::sharded<ss::abort_source> _as;                     // instance per core
    ss::sharded<partition_allocator> _partition_allocator; // single instance
    ss::sharded<topic_table> _tp_state;                    // instance per core
    ss::sharded<members_table> _members_table;             // instance per core
    ss::sharded<partition_balancer_state>
      _partition_balancer_state; // single instance
    ss::sharded<partition_leaders_table>
      _partition_leaders;                            // instance per core
    ss::sharded<drain_manager> _drain_manager;       // instance per core
    ss::sharded<members_manager> _members_manager;   // single instance
    ss::sharded<topics_frontend> _tp_frontend;       // instance per core
    ss::sharded<controller_backend> _backend;        // instance per core
    ss::sharded<controller_stm> _stm;                // single instance
    ss::sharded<controller_api> _api;                // instance per core
    ss::sharded<members_frontend> _members_frontend; // instance per core
    ss::sharded<members_backend> _members_backend;   // single instance
    ss::sharded<config_frontend> _config_frontend;   // instance per core
    ss::sharded<config_manager> _config_manager;     // single instance
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_manager>& _partition_manager;
    ss::sharded<shard_table>& _shard_table;
    ss::sharded<storage::api>& _storage;
    ss::sharded<node::local_monitor>& _local_monitor; // single instance
    topic_updates_dispatcher _tp_updates_dispatcher;
    ss::sharded<security::credential_store> _credentials;
    ss::sharded<security::ephemeral_credential_store> _ephemeral_credentials;
    security_manager _security_manager;
    ss::sharded<security_frontend> _security_frontend;
    ss::sharded<ephemeral_credential_frontend> _ephemeral_credential_frontend;
    ss::sharded<security::authorizer> _authorizer;
    ss::sharded<raft::group_manager>& _raft_manager;
    ss::sharded<health_monitor_frontend> _hm_frontend; // instance per core
    ss::sharded<health_monitor_backend> _hm_backend;   // single instance
    ss::sharded<health_manager> _health_manager;
    ss::sharded<metrics_reporter> _metrics_reporter;
    ss::sharded<feature_manager> _feature_manager;        // single instance
    ss::sharded<feature_backend> _feature_backend;        // instance per core
    ss::sharded<features::feature_table>& _feature_table; // instance per core
    std::unique_ptr<leader_balancer> _leader_balancer;
    ss::sharded<partition_balancer_backend> _partition_balancer;
    ss::gate _gate;
    consensus_ptr _raft0;
    ss::sharded<cloud_storage::remote>& _cloud_storage_api;
    controller_probe _probe;
    ss::sharded<bootstrap_backend> _bootstrap_backend; // single instance
    bool _is_ready = false;
};

} // namespace cluster
