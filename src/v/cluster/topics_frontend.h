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

#include "cloud_storage/remote.h"
#include "cluster/controller_stm.h"
#include "cluster/data_policy_frontend.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/read_replica_manager.h"
#include "cluster/scheduling/types.h"
#include "cluster/topic_table.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timeout_clock.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>

#include <system_error>

namespace cluster {

// on every core
class topics_frontend {
public:
    topics_frontend(
      model::node_id,
      ss::sharded<controller_stm>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<topic_table>&,
      ss::sharded<data_policy_frontend>&,
      ss::sharded<ss::abort_source>&,
      ss::sharded<cloud_storage::remote>&,
      ss::sharded<feature_table>&);

    ss::future<std::vector<topic_result>> create_topics(
      std::vector<custom_assignable_topic_configuration>,
      model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> delete_topics(
      std::vector<model::topic_namespace>, model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> autocreate_topics(
      std::vector<topic_configuration>, model::timeout_clock::duration);

    ss::future<std::error_code> move_partition_replicas(
      model::ntp,
      std::vector<model::broker_shard>,
      model::timeout_clock::time_point);

    ss::future<std::error_code> finish_moving_partition_replicas(
      model::ntp,
      std::vector<model::broker_shard>,
      model::timeout_clock::time_point);

    /**
     * Cancelling partition replicas move will use graceful path i.e. will never
     * allow data loss but it may require to be able to contact majority nodes
     * in the target(new) quorum.
     */
    ss::future<std::error_code> cancel_moving_partition_replicas(
      model::ntp, model::timeout_clock::time_point);
    /**
     * Aborting partition replicas move is an operation that allow force reset
     * of Raft group configuration to previous state. In some cases it may lead
     * to data loss but it does not require target(new) quorum to be available.
     */
    ss::future<std::error_code> abort_moving_partition_replicas(
      model::ntp, model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> update_topic_properties(
      std::vector<topic_properties_update>, model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> create_partitions(
      std::vector<create_partitions_configuration>,
      model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> create_non_replicable_topics(
      std::vector<non_replicable_topic> topics,
      model::timeout_clock::time_point timeout);

    ss::future<std::vector<topic_result>> autocreate_non_replicable_topics(
      std::vector<non_replicable_topic>, model::timeout_clock::duration);

    ss::future<bool> validate_shard(model::node_id node, uint32_t shard) const;

    void disable_partition_movement() { _partition_movement_disabled = true; }
    void enable_partition_movement() { _partition_movement_disabled = false; }

private:
    using ntp_leader = std::pair<model::ntp, model::node_id>;

    ss::future<topic_result> do_create_topic(
      custom_assignable_topic_configuration, model::timeout_clock::time_point);

    ss::future<topic_result> replicate_create_topic(
      topic_configuration, allocation_units, model::timeout_clock::time_point);

    ss::future<topic_result>
      do_delete_topic(model::topic_namespace, model::timeout_clock::time_point);

    ss::future<topic_result> do_create_non_replicable_topic(
      non_replicable_topic, model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> dispatch_create_to_leader(
      model::node_id,
      std::vector<topic_configuration>,
      model::timeout_clock::duration);

    ss::future<std::vector<topic_result>>
    dispatch_create_non_replicable_to_leader(
      model::node_id leader,
      std::vector<non_replicable_topic> topics,
      model::timeout_clock::duration timeout);

    ss::future<std::error_code> do_update_data_policy(
      topic_properties_update&, model::timeout_clock::time_point);
    ss::future<topic_result> do_update_topic_properties(
      topic_properties_update, model::timeout_clock::time_point);

    ss::future<result<model::offset>>
      stm_linearizable_barrier(model::timeout_clock::time_point);

    // returns true if the topic name is valid
    static bool validate_topic_name(const model::topic_namespace&);

    errc
    validate_topic_configuration(const custom_assignable_topic_configuration&);

    ss::future<topic_result> do_create_partition(
      create_partitions_configuration, model::timeout_clock::time_point);

    model::node_id _self;
    ss::sharded<controller_stm>& _stm;
    ss::sharded<partition_allocator>& _allocator;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<topic_table>& _topics;
    ss::sharded<data_policy_frontend>& _dp_frontend;
    ss::sharded<ss::abort_source>& _as;
    ss::sharded<cloud_storage::remote>& _cloud_storage_api;
    ss::sharded<feature_table>& _features;
    bool _partition_movement_disabled = false;
};

} // namespace cluster
