/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "config/seed_server.h"
#include "config/tls_config.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/types.h"
#include "random/simple_time_jitter.h"
#include "rpc/connection_cache.h"
#include "storage/fwd.h"

namespace cluster {

// Members manager class is responsible for updating information about
// cluster members, joining the cluster and creating inter cluster
// connections. This class receives updates from controller STM. It reacts
// only on raft configuration batch types. All the updates are propagated to
// core local cluster::members instances. There is only one instance of
// members manager running on core-0. The member manager is also responsible
// for validation of node configuration invariants.
class members_manager {
public:
    static constexpr auto accepted_commands = make_commands_list<
      decommission_node_cmd,
      recommission_node_cmd,
      finish_reallocations_cmd,
      maintenance_mode_cmd>{};
    static constexpr ss::shard_id shard = 0;
    static constexpr size_t max_updates_queue_size = 100;
    enum class node_update_type : int8_t {
        added,
        decommissioned,
        recommissioned,
        reallocation_finished,
    };

    struct node_update {
        model::node_id id;
        node_update_type type;
        model::offset offset;
        friend std::ostream& operator<<(std::ostream&, const node_update&);
    };

    members_manager(
      consensus_ptr,
      ss::sharded<members_table>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<storage::api>&,
      ss::sharded<drain_manager>&,
      ss::sharded<ss::abort_source>&);

    ss::future<> start();
    ss::future<> stop();
    ss::future<> validate_configuration_invariants();
    ss::future<result<join_node_reply>>
    handle_join_request(join_node_request const r);
    ss::future<std::error_code> apply_update(model::record_batch);

    ss::future<result<configuration_update_reply>>
      handle_configuration_update_request(configuration_update_request);

    bool is_batch_applicable(const model::record_batch& b) {
        return b.header().type == model::record_batch_type::node_management_cmd
               || b.header().type
                    == model::record_batch_type::raft_configuration;
    }
    /**
     * This API is backed by the seastar::queue. It can not be called
     * concurrently from multiple fibers.
     */
    ss::future<std::vector<node_update>> get_node_updates();

    ss::future<> join_cluster();

private:
    using seed_iterator = std::vector<config::seed_server>::const_iterator;
    // Cluster join
    void join_raft0();
    bool is_already_member() const;

    ss::future<result<join_node_reply>> dispatch_join_to_seed_server(
      seed_iterator it, join_node_request const& req);
    ss::future<result<join_node_reply>> dispatch_join_to_remote(
      const config::seed_server&, join_node_request&& req);

    ss::future<join_node_reply> dispatch_join_request();
    template<typename Func>
    auto dispatch_rpc_to_leader(rpc::clock_type::duration, Func&& f);

    // Raft 0 config updates
    ss::future<>
      handle_raft0_cfg_update(raft::group_configuration, model::offset);
    ss::future<> update_connections(patch<broker_ptr>);

    ss::future<> maybe_update_current_node_configuration();
    ss::future<> dispatch_configuration_update(model::broker);
    ss::future<result<configuration_update_reply>>
      do_dispatch_configuration_update(model::broker, model::broker);

    template<typename Cmd>
    ss::future<std::error_code> dispatch_updates_to_cores(model::offset, Cmd);

    ss::future<std::error_code>
      apply_raft_configuration_batch(model::record_batch);

    model::offset _last_seen_configuration_offset;
    std::vector<config::seed_server> _seed_servers;
    model::broker _self;
    simple_time_jitter<model::timeout_clock> _join_retry_jitter;
    std::chrono::milliseconds _join_timeout;
    consensus_ptr _raft0;
    ss::sharded<members_table>& _members_table;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<partition_allocator>& _allocator;
    ss::sharded<storage::api>& _storage;
    ss::sharded<drain_manager>& _drain_manager;
    ss::sharded<ss::abort_source>& _as;
    config::tls_config _rpc_tls_config;
    ss::gate _gate;
    ss::queue<node_update> _update_queue;
    ss::abort_source::subscription _queue_abort_subscription;
};

std::ostream&
operator<<(std::ostream&, const members_manager::node_update_type&);
} // namespace cluster
