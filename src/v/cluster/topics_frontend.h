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

#include "cloud_storage/fwd.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/remote_topic_configuration_source.h"
#include "cluster/scheduling/types.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/property.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "partition_balancer_types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>

#include <system_error>

namespace cluster {

// on every core
class topics_frontend {
public:
    struct capacity_info {
        absl::flat_hash_map<model::node_id, node_disk_space> node_disk_reports;
        absl::flat_hash_map<model::partition_id, int64_t> ntp_sizes;
    };
    topics_frontend(
      model::node_id,
      ss::sharded<controller_stm>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<topic_table>&,
      ss::sharded<health_monitor_frontend>&,
      ss::sharded<ss::abort_source>&,
      ss::sharded<cloud_storage::remote>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<cluster::members_table>&,
      ss::sharded<partition_manager>&,
      ss::sharded<shard_table>&,
      plugin_table&,
      metadata_cache&,
      config::binding<unsigned>);

    ss::future<std::vector<topic_result>> create_topics(
      std::vector<custom_assignable_topic_configuration>,
      model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> delete_topics(
      std::vector<model::topic_namespace>, model::timeout_clock::time_point);

    // May be called on any node
    ss::future<topic_result>
      purged_topic(nt_revision, model::timeout_clock::duration);

    // May only be called on leader
    ss::future<topic_result>
      do_purged_topic(nt_revision, model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> autocreate_topics(
      std::vector<topic_configuration>, model::timeout_clock::duration);

    ss::future<std::error_code> move_partition_replicas(
      model::ntp,
      std::vector<model::broker_shard>,
      reconfiguration_policy,
      model::timeout_clock::time_point,
      std::optional<model::term_id> = std::nullopt);

    ss::future<std::error_code> force_update_partition_replicas(
      model::ntp,
      std::vector<model::broker_shard>,
      model::timeout_clock::time_point,
      std::optional<model::term_id> = std::nullopt);

    /**
     * Given a list of defunct nodes, generates a list of ntps that lost
     * majority due to unavailability of the nodes.
     */
    ss::future<result<fragmented_vector<ntp_with_majority_loss>>>
    partitions_with_lost_majority(std::vector<model::node_id> defunct_nodes);

    ss::future<std::error_code> force_recover_partitions_from_nodes(
      std::vector<model::node_id> nodes,
      fragmented_vector<ntp_with_majority_loss>
        user_approved_force_recovery_partitions,
      model::timeout_clock::time_point timeout);

    /**
     * This overload of move_partition_replicas will use the partition
     * allocator to generate a new replica set (i.e., a
     * vector<broker_shard>) based on the given ntp and list of node ids.
     */
    ss::future<std::error_code> move_partition_replicas(
      model::ntp,
      std::vector<model::node_id>,
      reconfiguration_policy,
      model::timeout_clock::time_point,
      std::optional<model::term_id> = std::nullopt);

    ss::future<std::error_code> finish_moving_partition_replicas(
      model::ntp,
      std::vector<model::broker_shard>,
      model::timeout_clock::time_point);

    ss::future<std::error_code> revert_cancel_partition_move(
      model::ntp, model::timeout_clock::time_point);

    /**
     * Cancelling partition replicas move will use graceful path i.e. will never
     * allow data loss but it may require to be able to contact majority nodes
     * in the target(new) quorum.
     */
    ss::future<std::error_code> cancel_moving_partition_replicas(
      model::ntp,
      model::timeout_clock::time_point,
      std::optional<model::term_id> = std::nullopt);

    /**
     * Aborting partition replicas move is an operation that allow force reset
     * of Raft group configuration to previous state. In some cases it may lead
     * to data loss but it does not require target(new) quorum to be available.
     */
    ss::future<std::error_code> abort_moving_partition_replicas(
      model::ntp,
      model::timeout_clock::time_point,
      std::optional<model::term_id> = std::nullopt);

    ss::future<std::vector<topic_result>> update_topic_properties(
      std::vector<topic_properties_update>, model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> create_partitions(
      std::vector<create_partitions_configuration>,
      model::timeout_clock::time_point);

    /// if partition_id is nullopt, the disabled flag is applied to all
    /// partitions of the topic.
    ///
    /// The method is idempotent, i.e. if the disabled flag already has the
    /// desired value, the call is ignored.
    ss::future<std::error_code> set_topic_partitions_disabled(
      model::topic_namespace_view,
      std::optional<model::partition_id>,
      bool disabled,
      model::timeout_clock::time_point);

    ss::future<bool> validate_shard(model::node_id node, uint32_t shard) const;

    ss::future<result<std::vector<move_cancellation_result>>>
      cancel_moving_partition_replicas_node(
        model::node_id,
        partition_move_direction,
        model::timeout_clock::time_point);

    ss::future<result<std::vector<move_cancellation_result>>>
      cancel_moving_all_partition_replicas(model::timeout_clock::time_point);

    // High level code should use update_topic_properties (which will RPC
    // to the controller leader as needed).  This lower level method is only
    // used by upgrade migration code to update topic properties directly
    // from the controller leader.
    ss::future<topic_result> do_update_topic_properties(
      topic_properties_update, model::timeout_clock::time_point);

    ss::future<result<std::vector<partition_state>>>
      get_partition_state(model::ntp);

private:
    using ntp_leader = std::pair<model::ntp, model::node_id>;

    ss::future<topic_result> do_create_topic(
      custom_assignable_topic_configuration, model::timeout_clock::time_point);

    ss::future<topic_result> replicate_create_topic(
      topic_configuration,
      allocation_units::pointer,
      model::timeout_clock::time_point);

    ss::future<topic_result>
      do_delete_topic(model::topic_namespace, model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> dispatch_create_to_leader(
      model::node_id,
      std::vector<topic_configuration>,
      model::timeout_clock::duration);

    ss::future<topic_result> dispatch_purged_topic_to_leader(
      model::node_id, nt_revision, model::timeout_clock::duration);

    ss::future<std::error_code> do_update_replication_factor(
      topic_properties_update&, model::timeout_clock::time_point);

    ss::future<std::error_code> change_replication_factor(
      model::topic_namespace,
      cluster::replication_factor,
      model::timeout_clock::time_point);

    using new_replicas_assigment = std::vector<move_topic_replicas_data>;
    ss::future<std::error_code> increase_replication_factor(
      model::topic_namespace,
      cluster::replication_factor,
      model::timeout_clock::time_point);
    ss::future<std::error_code> decrease_replication_factor(
      model::topic_namespace,
      cluster::replication_factor,
      model::timeout_clock::time_point);

    ss::future<result<model::offset>>
      stm_linearizable_barrier(model::timeout_clock::time_point);

    // returns true if the topic name is valid
    static bool validate_topic_name(const model::topic_namespace&);

    errc
    validate_topic_configuration(const custom_assignable_topic_configuration&);

    ss::future<topic_result> do_create_partition(
      create_partitions_configuration, model::timeout_clock::time_point);

    ss::future<std::vector<move_cancellation_result>>
      do_cancel_moving_partition_replicas(
        std::vector<model::ntp>, model::timeout_clock::time_point);

    ss::future<capacity_info> get_health_info(
      model::topic_namespace topic, int32_t partition_count) const;

    // Generates a new partition assignment for a single partition
    ss::future<result<ss::chunked_fifo<partition_assignment>>>
    generate_reassignments(
      model::ntp, std::vector<model::node_id> new_replicas);

    ss::future<result<partition_state_reply>>
      do_get_partition_state(model::node_id, model::ntp);

    model::node_id _self;
    ss::sharded<controller_stm>& _stm;
    ss::sharded<partition_allocator>& _allocator;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<topic_table>& _topics;
    ss::sharded<health_monitor_frontend>& _hm_frontend;
    ss::sharded<ss::abort_source>& _as;
    ss::sharded<cloud_storage::remote>& _cloud_storage_api;
    ss::sharded<features::feature_table>& _features;
    plugin_table& _plugin_table;
    metadata_cache& _metadata_cache;

    ss::sharded<cluster::members_table>& _members_table;
    ss::sharded<partition_manager>& _pm;
    ss::sharded<shard_table>& _shard_table;

    config::binding<unsigned> _hard_max_disk_usage_ratio;

    static constexpr std::chrono::seconds _get_health_report_timeout = 10s;
};

} // namespace cluster
