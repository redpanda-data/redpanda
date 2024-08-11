/**
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/errc.h"

#include <iostream>

namespace cluster {
std::ostream& operator<<(std::ostream& o, cluster::errc err) {
    switch (err) {
    case errc::success:
        return o << "cluster::errc::success";
    case errc::notification_wait_timeout:
        return o << "cluster::errc::notification_wait_timeout";
    case errc::topic_invalid_partitions:
        return o << "cluster::errc::topic_invalid_partitions";
    case errc::topic_invalid_replication_factor:
        return o << "cluster::errc::topic_invalid_replication_factor";
    case errc::topic_invalid_config:
        return o << "cluster::errc::topic_invalid_config";
    case errc::not_leader_controller:
        return o << "cluster::errc::not_leader_controller";
    case errc::topic_already_exists:
        return o << "cluster::errc::topic_already_exists";
    case errc::replication_error:
        return o << "cluster::errc::replication_error";
    case errc::shutting_down:
        return o << "cluster::errc::shutting_down";
    case errc::no_leader_controller:
        return o << "cluster::errc::no_leader_controller";
    case errc::join_request_dispatch_error:
        return o << "cluster::errc::join_request_dispatch_error";
    case errc::seed_servers_exhausted:
        return o << "cluster::errc::seed_servers_exhausted";
    case errc::auto_create_topics_exception:
        return o << "cluster::errc::auto_create_topics_exception";
    case errc::timeout:
        return o << "cluster::errc::timeout";
    case errc::topic_not_exists:
        return o << "cluster::errc::topic_not_exists";
    case errc::invalid_topic_name:
        return o << "cluster::errc::invalid_topic_name";
    case errc::partition_not_exists:
        return o << "cluster::errc::partition_not_exists";
    case errc::not_leader:
        return o << "cluster::errc::not_leader";
    case errc::partition_already_exists:
        return o << "cluster::errc::partition_already_exists";
    case errc::waiting_for_recovery:
        return o << "cluster::errc::waiting_for_recovery";
    case errc::waiting_for_reconfiguration_finish:
        return o << "cluster::errc::waiting_for_reconfiguration_finish";
    case errc::update_in_progress:
        return o << "cluster::errc::update_in_progress";
    case errc::user_exists:
        return o << "cluster::errc::user_exists";
    case errc::user_does_not_exist:
        return o << "cluster::errc::user_does_not_exist";
    case errc::invalid_producer_epoch:
        return o << "cluster::errc::invalid_producer_epoch";
    case errc::sequence_out_of_order:
        return o << "cluster::errc::sequence_out_of_order";
    case errc::generic_tx_error:
        return o << "cluster::errc::generic_tx_error";
    case errc::node_does_not_exists:
        return o << "cluster::errc::node_does_not_exists";
    case errc::invalid_node_operation:
        return o << "cluster::errc::invalid_node_operation";
    case errc::invalid_configuration_update:
        return o << "cluster::errc::invalid_configuration_update";
    case errc::topic_operation_error:
        return o << "cluster::errc::topic_operation_error";
    case errc::no_eligible_allocation_nodes:
        return o << "cluster::errc::no_eligible_allocation_nodes";
    case errc::allocation_error:
        return o << "cluster::errc::allocation_error";
    case errc::partition_configuration_revision_not_updated:
        return o
               << "cluster::errc::partition_configuration_revision_not_updated";
    case errc::partition_configuration_in_joint_mode:
        return o << "cluster::errc::partition_configuration_in_joint_mode";
    case errc::partition_configuration_leader_config_not_committed:
        return o << "cluster::errc::partition_configuration_leader_config_not_"
                    "committed";
    case errc::partition_configuration_differs:
        return o << "cluster::errc::partition_configuration_differs";
    case errc::data_policy_already_exists:
        return o << "cluster::errc::data_policy_already_exists";
    case errc::data_policy_not_exists:
        return o << "cluster::errc::data_policy_not_exists";
    case errc::source_topic_not_exists:
        return o << "cluster::errc::source_topic_not_exists";
    case errc::source_topic_still_in_use:
        return o << "cluster::errc::source_topic_still_in_use";
    case errc::waiting_for_partition_shutdown:
        return o << "cluster::errc::waiting_for_partition_shutdown";
    case errc::error_collecting_health_report:
        return o << "cluster::errc::error_collecting_health_report";
    case errc::leadership_changed:
        return o << "cluster::errc::leadership_changed";
    case errc::feature_disabled:
        return o << "cluster::errc::feature_disabled";
    case errc::invalid_request:
        return o << "cluster::errc::invalid_request";
    case errc::no_update_in_progress:
        return o << "cluster::errc::no_update_in_progress";
    case errc::unknown_update_interruption_error:
        return o << "cluster::errc::unknown_update_interruption_error";
    case errc::throttling_quota_exceeded:
        return o << "cluster::errc::throttling_quota_exceeded";
    case errc::cluster_already_exists:
        return o << "cluster::errc::cluster_already_exists";
    case errc::no_partition_assignments:
        return o << "cluster::errc::no_partition_assignments";
    case errc::failed_to_create_partition:
        return o << "cluster::errc::failed_to_create_partition";
    case errc::partition_operation_failed:
        return o << "cluster::errc::partition_operation_failed";
    case errc::transform_does_not_exist:
        return o << "cluster::errc::transform_does_not_exist";
    case errc::transform_invalid_update:
        return o << "cluster::errc::transform_invalid_update";
    case errc::transform_invalid_create:
        return o << "cluster::errc::transform_invalid_create";
    case errc::transform_invalid_source:
        return o << "cluster::errc::transform_invalid_source";
    case errc::transform_invalid_environment:
        return o << "cluster::errc::transform_invalid_environment";
    case errc::trackable_keys_limit_exceeded:
        return o << "cluster::errc::trackable_keys_limit_exceeded";
    case errc::topic_disabled:
        return o << "cluster::errc::topic_disabled";
    case errc::partition_disabled:
        return o << "cluster::errc::partition_disabled";
    case errc::invalid_partition_operation:
        return o << "cluster::errc::invalid_partition_operation";
    case errc::concurrent_modification_error:
        return o << "cluster::errc::concurrent_modification_error";
    case errc::transform_count_limit_exceeded:
        return o << "cluster::errc::transform_count_limit_exceeded";
    case errc::role_exists:
        return o << "cluster::errc::role_exists";
    case errc::role_does_not_exist:
        return o << "cluster::errc::role_does_not_exist";
    case errc::inconsistent_stm_update:
        return o << "cluster::errc::inconsistent_stm_update";
    case errc::waiting_for_shard_placement_update:
        return o << "cluster::errc::waiting_for_shard_placement_update";
    case errc::topic_invalid_partitions_core_limit:
        return o << "cluster::errc::topic_invalid_partitions_core_limit";
    case errc::topic_invalid_partitions_memory_limit:
        return o << "cluster::errc::topic_invalid_partitions_memory_limit";
    case errc::topic_invalid_partitions_fd_limit:
        return o << "cluster::errc::topic_invalid_partitions_fd_limit";
    case errc::topic_invalid_partitions_decreased:
        return o << "cluster::errc::topic_invalid_partitions_decreased";
    case errc::producer_ids_vcluster_limit_exceeded:
        return o << "cluster::errc::producer_ids_vcluster_limit_exceeded";
    case errc::validation_of_recovery_topic_failed:
        return o << "cluster::errc::validation_of_recovery_topic_failed";
    case errc::replica_does_not_exist:
        return o << "cluster::errc::replica_does_not_exist";
    case errc::invalid_data_migration_state:
        return o << "cluster::errc::invalid_data_migration_state";
    case errc::data_migration_not_exists:
        return o << "cluster::errc::data_migration_not_exists";
    case errc::data_migration_already_exists:
        return o << "cluster::errc::data_migration_already_exists";
    case errc::data_migration_invalid_resources:
        return o << "cluster::errc::data_migration_invalid_resources";
    case errc::resource_is_being_migrated:
        return o << "cluster::errc::resource_is_being_migrated";
    case errc::invalid_target_node_id:
        return o << "cluster::errc::invalid_target_node_id";
    }
}
} // namespace cluster
