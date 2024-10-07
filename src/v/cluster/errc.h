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
#include <system_error>

namespace cluster {

enum class errc : int16_t {
    success = 0, // must be 0
    notification_wait_timeout,
    topic_invalid_partitions,
    topic_invalid_replication_factor,
    topic_invalid_config,
    not_leader_controller,
    topic_already_exists,
    replication_error,
    shutting_down,
    no_leader_controller,
    join_request_dispatch_error,
    seed_servers_exhausted,
    auto_create_topics_exception,
    timeout,
    topic_not_exists,
    invalid_topic_name,
    partition_not_exists,
    not_leader,
    partition_already_exists,
    waiting_for_recovery,
    waiting_for_reconfiguration_finish,
    update_in_progress,
    user_exists,
    user_does_not_exist,
    invalid_producer_epoch,
    sequence_out_of_order,
    generic_tx_error,
    node_does_not_exists,
    invalid_node_operation,
    invalid_configuration_update,
    topic_operation_error,
    no_eligible_allocation_nodes,
    allocation_error,
    partition_configuration_revision_not_updated,
    partition_configuration_in_joint_mode,
    partition_configuration_leader_config_not_committed,
    partition_configuration_differs,
    data_policy_already_exists,
    data_policy_not_exists,
    source_topic_not_exists,
    source_topic_still_in_use,
    waiting_for_partition_shutdown,
    error_collecting_health_report,
    leadership_changed,
    feature_disabled,
    invalid_request,
    no_update_in_progress,
    unknown_update_interruption_error,
    throttling_quota_exceeded,
    cluster_already_exists,
    no_partition_assignments,
    failed_to_create_partition,
    partition_operation_failed,
    transform_does_not_exist,
    transform_invalid_update,
    transform_invalid_create,
    transform_invalid_source,
    transform_invalid_environment,
    trackable_keys_limit_exceeded,
    topic_disabled,
    partition_disabled,
    invalid_partition_operation,
    concurrent_modification_error,
    transform_count_limit_exceeded,
    role_exists,
    role_does_not_exist,
    inconsistent_stm_update,
    waiting_for_shard_placement_update,
    topic_invalid_partitions_core_limit,
    topic_invalid_partitions_memory_limit,
    topic_invalid_partitions_fd_limit,
    topic_invalid_partitions_decreased,
    producer_ids_vcluster_limit_exceeded,
    validation_of_recovery_topic_failed,
    replica_does_not_exist,
    invalid_data_migration_state,
    data_migration_not_exists,
    data_migration_already_exists,
    data_migration_invalid_resources,
    resource_is_being_migrated,
    invalid_target_node_id,
};

std::ostream& operator<<(std::ostream& o, errc err);

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "cluster::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "Success";
        case errc::notification_wait_timeout:
            return "Timeout waiting for append entries notification comming "
                   "from raft 0";
        case errc::topic_invalid_partitions:
            return "Unable to assign topic partitions to current cluster "
                   "resources";
        case errc::topic_invalid_replication_factor:
            return "Unable to allocate topic with given replication factor";
        case errc::topic_invalid_config:
            return "Configuration is invalid";
        case errc::not_leader_controller:
            return "This node is not raft-0 leader. i.e is not leader "
                   "controller";
        case errc::topic_already_exists:
            return "The topic has already been created";
        case errc::replication_error:
            return "Unable to replicate given state across cluster nodes";
        case errc::shutting_down:
            return "Application is shutting down";
        case errc::no_leader_controller:
            return "Currently there is no leader controller elected in the "
                   "cluster";
        case errc::join_request_dispatch_error:
            return "Error occurred when controller tried to join the cluster";
        case errc::seed_servers_exhausted:
            return "There are no seed servers left to try in this round, will "
                   "retry after delay ";
        case errc::auto_create_topics_exception:
            return "An exception was thrown while auto creating topics";
        case errc::timeout:
            return "Timeout occurred while processing request";
        case errc::topic_not_exists:
            return "Topic does not exists";
        case errc::invalid_topic_name:
            return "Invalid topic name";
        case errc::partition_not_exists:
            return "Requested partition does not exists";
        case errc::not_leader:
            return "Current node is not a leader for partition";
        case errc::partition_already_exists:
            return "Requested partition already exists";
        case errc::waiting_for_recovery:
            return "Waiting for partition to recover";
        case errc::waiting_for_reconfiguration_finish:
            return "Waiting for partition reconfiguration to be finished";
        case errc::update_in_progress:
            return "Partition configuration update in progress";
        case errc::user_exists:
            return "User already exists";
        case errc::user_does_not_exist:
            return "User does not exist";
        case errc::invalid_producer_epoch:
            return "Invalid idempotent producer epoch";
        case errc::sequence_out_of_order:
            return "Producer sequence ID out of order";
        case errc::generic_tx_error:
            return "Generic error when processing transactional requests";
        case errc::node_does_not_exists:
            return "Requested node does not exists";
        case errc::invalid_node_operation:
            return "Requested node opeartion is invalid";
        case errc::invalid_configuration_update:
            return "Requested configuration update is invalid";
        case errc::topic_operation_error:
            return "Unable to perform requested topic operation ";
        case errc::no_eligible_allocation_nodes:
            return "No nodes are available to perform allocation after hard "
                   "constraints were solved";
        case errc::allocation_error:
            return "Exception was thrown when allocating partitions ";
        case errc::partition_configuration_revision_not_updated:
            return "Partition configuration revision wasn't yet updated with "
                   "operation revision";
        case errc::partition_configuration_in_joint_mode:
            return "Partition configuration still in joint consensus mode";
        case errc::partition_configuration_leader_config_not_committed:
            return "Partition configuration wasn't committed on the leader";
        case errc::partition_configuration_differs:
            return "Current and requested partition configuration differs";
        case errc::data_policy_already_exists:
            return "Data-policy already exists";
        case errc::data_policy_not_exists:
            return "Data-policy does not exist";
        case errc::source_topic_not_exists:
            return "Attempted to create a non_replicable log for a source "
                   "topic "
                   "that does not exist";
        case errc::source_topic_still_in_use:
            return "Cannot delete source topic for which there still are "
                   "materialized topics for";
        case errc::waiting_for_partition_shutdown:
            return "Partition update on current core can not be finished since "
                   "backend is waiting for the partition to be shutdown on its "
                   "originating core";
        case errc::error_collecting_health_report:
            return "Error requesting health monitor update";
        case errc::leadership_changed:
            return "Raft group leadership has changed while waiting for action "
                   "to finish";
        case errc::feature_disabled:
            return "Requested feature is disabled";
        case errc::invalid_request:
            return "Invalid request";
        case errc::no_update_in_progress:
            return "Partition configuration is not being updated";
        case errc::unknown_update_interruption_error:
            return "Error while cancelling partition reconfiguration";
        case errc::throttling_quota_exceeded:
            return "Request declined due to exceeded requests quotas";
        case errc::cluster_already_exists:
            return "Node is a part of a cluster already, new cluster is not "
                   "created";
        case errc::no_partition_assignments:
            return "No replica assignments for the requested partition";
        case errc::failed_to_create_partition:
            return "Failed to create partition replica instance";
        case errc::partition_operation_failed:
            return "Generic failure occurred during partition operation "
                   "execution";
        case errc::transform_does_not_exist:
            return "Transform does not exist";
        case errc::transform_invalid_update:
            return "Invalid update to transform, topic configuration "
                   "cannot change";
        case errc::transform_invalid_source:
            return "Invalid transform source";
        case errc::transform_invalid_create:
            return "Invalid create transform configuration";
        case errc::transform_invalid_environment:
            return "Invalid transform environment";
        case errc::trackable_keys_limit_exceeded:
            return "Too many keys are currently tracked, no space for more";
        case errc::topic_disabled:
            return "Topic disabled by user";
        case errc::partition_disabled:
            return "Partition disabled by user";
        case errc::invalid_partition_operation:
            return "Invalid partition operation";
        case errc::concurrent_modification_error:
            return "Concurrent modification error";
        case errc::transform_count_limit_exceeded:
            return "Too many transforms deployed";
        case errc::role_exists:
            return "Role already exists";
        case errc::role_does_not_exist:
            return "Role does not exist";
        case errc::inconsistent_stm_update:
            return "STM command can't be applied";
        case errc::waiting_for_shard_placement_update:
            return "Waiting for shard placement table update to finish";
        case errc::topic_invalid_partitions_core_limit:
            return "Can not increase partition count due to core limit";
        case errc::topic_invalid_partitions_memory_limit:
            return "Can not increase partition count due to memory limit";
        case errc::topic_invalid_partitions_fd_limit:
            return "Can not increase partition count due to FD limit";
        case errc::topic_invalid_partitions_decreased:
            return "Can not decrease the number of partitions";
        case errc::producer_ids_vcluster_limit_exceeded:
            return "To many vclusters registered in producer state cache";
        case errc::validation_of_recovery_topic_failed:
            return "Validation of recovery topic failed";
        case errc::replica_does_not_exist:
            return "Partition replica does not exist";
        case errc::invalid_data_migration_state:
            return "Invalid data migration state transition requested";
        case errc::data_migration_not_exists:
            return "Requested data migration does not exist";
        case errc::data_migration_already_exists:
            return "Data migration with requested id already exists";
        case errc::data_migration_invalid_resources:
            return "Data migration contains resources that does not exists or "
                   "are already being migrated";
        case errc::resource_is_being_migrated:
            return "Requested operation can not be executed as the resource is "
                   "undergoing data migration";
        case errc::invalid_target_node_id:
            return "Request was intended for the node with different node id";
        }
        return "cluster::errc::unknown";
    }
};
inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}
} // namespace cluster
namespace std {
template<>
struct is_error_code_enum<cluster::errc> : true_type {};
} // namespace std
