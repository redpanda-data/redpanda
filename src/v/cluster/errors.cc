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

#include "base/compiler_utils.h"
#include "cluster/errc.h"

namespace cluster {
fmt::iterator format_to(errc err, fmt::iterator out) {
    switch (err) {
    case errc::success:
        return fmt::format_to(out, "cluster::errc::success");
    case errc::notification_wait_timeout:
        return fmt::format_to(out, "cluster::errc::notification_wait_timeout");
    case errc::topic_invalid_partitions:
        return fmt::format_to(out, "cluster::errc::topic_invalid_partitions");
    case errc::topic_invalid_replication_factor:
        return fmt::format_to(
          out, "cluster::errc::topic_invalid_replication_factor");
    case errc::topic_invalid_config:
        return fmt::format_to(out, "cluster::errc::topic_invalid_config");
    case errc::not_leader_controller:
        return fmt::format_to(out, "cluster::errc::not_leader_controller");
    case errc::topic_already_exists:
        return fmt::format_to(out, "cluster::errc::topic_already_exists");
    case errc::replication_error:
        return fmt::format_to(out, "cluster::errc::replication_error");
    case errc::shutting_down:
        return fmt::format_to(out, "cluster::errc::shutting_down");
    case errc::no_leader_controller:
        return fmt::format_to(out, "cluster::errc::no_leader_controller");
    case errc::join_request_dispatch_error:
        return fmt::format_to(
          out, "cluster::errc::join_request_dispatch_error");
    case errc::seed_servers_exhausted:
        return fmt::format_to(out, "cluster::errc::seed_servers_exhausted");
    case errc::auto_create_topics_exception:
        return fmt::format_to(
          out, "cluster::errc::auto_create_topics_exception");
    case errc::timeout:
        return fmt::format_to(out, "cluster::errc::timeout");
    case errc::topic_not_exists:
        return fmt::format_to(out, "cluster::errc::topic_not_exists");
    case errc::invalid_topic_name:
        return fmt::format_to(out, "cluster::errc::invalid_topic_name");
    case errc::partition_not_exists:
        return fmt::format_to(out, "cluster::errc::partition_not_exists");
    case errc::not_leader:
        return fmt::format_to(out, "cluster::errc::not_leader");
    case errc::partition_already_exists:
        return fmt::format_to(out, "cluster::errc::partition_already_exists");
    case errc::waiting_for_recovery:
        return fmt::format_to(out, "cluster::errc::waiting_for_recovery");
    case errc::waiting_for_reconfiguration_finish:
        return fmt::format_to(
          out, "cluster::errc::waiting_for_reconfiguration_finish");
    case errc::update_in_progress:
        return fmt::format_to(out, "cluster::errc::update_in_progress");
    case errc::user_exists:
        return fmt::format_to(out, "cluster::errc::user_exists");
    case errc::user_does_not_exist:
        return fmt::format_to(out, "cluster::errc::user_does_not_exist");
    case errc::invalid_producer_epoch:
        return fmt::format_to(out, "cluster::errc::invalid_producer_epoch");
    case errc::sequence_out_of_order:
        return fmt::format_to(out, "cluster::errc::sequence_out_of_order");
    case errc::generic_tx_error:
        return fmt::format_to(out, "cluster::errc::generic_tx_error");
    case errc::node_does_not_exists:
        return fmt::format_to(out, "cluster::errc::node_does_not_exists");
    case errc::invalid_node_operation:
        return fmt::format_to(out, "cluster::errc::invalid_node_operation");
    case errc::invalid_configuration_update:
        return fmt::format_to(
          out, "cluster::errc::invalid_configuration_update");
    case errc::topic_operation_error:
        return fmt::format_to(out, "cluster::errc::topic_operation_error");
    case errc::no_eligible_allocation_nodes:
        return fmt::format_to(
          out, "cluster::errc::no_eligible_allocation_nodes");
    case errc::allocation_error:
        return fmt::format_to(out, "cluster::errc::allocation_error");
    case errc::partition_configuration_revision_not_updated:
        return fmt::format_to(
          out, "cluster::errc::partition_configuration_revision_not_updated");
    case errc::partition_configuration_in_joint_mode:
        return fmt::format_to(
          out, "cluster::errc::partition_configuration_in_joint_mode");
    case errc::partition_configuration_leader_config_not_committed:
        return fmt::format_to(
          out,
          "cluster::errc::partition_configuration_leader_config_not_committed");
    case errc::partition_configuration_differs:
        return fmt::format_to(
          out, "cluster::errc::partition_configuration_differs");
    case errc::data_policy_already_exists:
        return fmt::format_to(out, "cluster::errc::data_policy_already_exists");
    case errc::data_policy_not_exists:
        return fmt::format_to(out, "cluster::errc::data_policy_not_exists");
    case errc::source_topic_not_exists:
        return fmt::format_to(out, "cluster::errc::source_topic_not_exists");
    case errc::source_topic_still_in_use:
        return fmt::format_to(out, "cluster::errc::source_topic_still_in_use");
    case errc::waiting_for_partition_shutdown:
        return fmt::format_to(
          out, "cluster::errc::waiting_for_partition_shutdown");
    case errc::error_collecting_health_report:
        return fmt::format_to(
          out, "cluster::errc::error_collecting_health_report");
    case errc::leadership_changed:
        return fmt::format_to(out, "cluster::errc::leadership_changed");
    case errc::feature_disabled:
        return fmt::format_to(out, "cluster::errc::feature_disabled");
    case errc::invalid_request:
        return fmt::format_to(out, "cluster::errc::invalid_request");
    case errc::no_update_in_progress:
        return fmt::format_to(out, "cluster::errc::no_update_in_progress");
    case errc::unknown_update_interruption_error:
        return fmt::format_to(
          out, "cluster::errc::unknown_update_interruption_error");
    case errc::throttling_quota_exceeded:
        return fmt::format_to(out, "cluster::errc::throttling_quota_exceeded");
    case errc::cluster_already_exists:
        return fmt::format_to(out, "cluster::errc::cluster_already_exists");
    case errc::no_partition_assignments:
        return fmt::format_to(out, "cluster::errc::no_partition_assignments");
    case errc::failed_to_create_partition:
        return fmt::format_to(out, "cluster::errc::failed_to_create_partition");
    case errc::partition_operation_failed:
        return fmt::format_to(out, "cluster::errc::partition_operation_failed");
    case errc::transform_does_not_exist:
        return fmt::format_to(out, "cluster::errc::transform_does_not_exist");
    case errc::transform_invalid_update:
        return fmt::format_to(out, "cluster::errc::transform_invalid_update");
    case errc::transform_invalid_create:
        return fmt::format_to(out, "cluster::errc::transform_invalid_create");
    case errc::transform_invalid_source:
        return fmt::format_to(out, "cluster::errc::transform_invalid_source");
    case errc::transform_invalid_environment:
        return fmt::format_to(
          out, "cluster::errc::transform_invalid_environment");
    case errc::trackable_keys_limit_exceeded:
        return fmt::format_to(
          out, "cluster::errc::trackable_keys_limit_exceeded");
    case errc::topic_disabled:
        return fmt::format_to(out, "cluster::errc::topic_disabled");
    case errc::partition_disabled:
        return fmt::format_to(out, "cluster::errc::partition_disabled");
    case errc::invalid_partition_operation:
        return fmt::format_to(
          out, "cluster::errc::invalid_partition_operation");
    case errc::concurrent_modification_error:
        return fmt::format_to(
          out, "cluster::errc::concurrent_modification_error");
    case errc::transform_count_limit_exceeded:
        return fmt::format_to(
          out, "cluster::errc::transform_count_limit_exceeded");
    case errc::role_exists:
        return fmt::format_to(out, "cluster::errc::role_exists");
    case errc::role_does_not_exist:
        return fmt::format_to(out, "cluster::errc::role_does_not_exist");
    case errc::waiting_for_shard_placement_update:
        return fmt::format_to(
          out, "cluster::errc::waiting_for_shard_placement_update");
    case errc::topic_invalid_partitions_core_limit:
        return fmt::format_to(
          out, "cluster::errc::topic_invalid_partitions_core_limit");
    case errc::topic_invalid_partitions_memory_limit:
        return fmt::format_to(
          out, "cluster::errc::topic_invalid_partitions_memory_limit");
    case errc::topic_invalid_partitions_fd_limit:
        return fmt::format_to(
          out, "cluster::errc::topic_invalid_partitions_fd_limit");
    case errc::topic_invalid_partitions_decreased:
        return fmt::format_to(
          out, "cluster::errc::topic_invalid_partitions_decreased");
    case errc::producer_ids_vcluster_limit_exceeded:
        return fmt::format_to(
          out, "cluster::errc::producer_ids_vcluster_limit_exceeded");
    case errc::validation_of_recovery_topic_failed:
        return fmt::format_to(
          out, "cluster::errc::validation_of_recovery_topic_failed");
    case errc::replica_does_not_exist:
        return fmt::format_to(out, "cluster::errc::replica_does_not_exist");
    case errc::invalid_data_migration_state:
        return fmt::format_to(
          out, "cluster::errc::invalid_data_migration_state");
    case errc::data_migration_not_exists:
        return fmt::format_to(out, "cluster::errc::data_migration_not_exists");
    case errc::data_migration_already_exists:
        return fmt::format_to(
          out, "cluster::errc::data_migration_already_exists");
    case errc::data_migration_invalid_resources:
        return fmt::format_to(
          out, "cluster::errc::data_migration_invalid_resources");
    case errc::data_migration_invalid_definition:
        return fmt::format_to(
          out, "cluster::errc::data_migration_invalid_definition");
    case errc::data_migrations_disabled:
        return fmt::format_to(out, "cluster::errc::data_migrations_disabled");
    case errc::resource_is_being_migrated:
        return fmt::format_to(out, "cluster::errc::resource_is_being_migrated");
    case errc::invalid_target_node_id:
        return fmt::format_to(out, "cluster::errc::invalid_target_node_id");
    case errc::topic_id_already_exists:
        return fmt::format_to(out, "cluster::errc::topic_id_already_exists");
    case errc::feature_sanctioned:
        return fmt::format_to(out, "cluster::errc::feature_sanctioned");
    case errc::offset_out_of_range:
        return fmt::format_to(out, "cluster::errc::offset_out_of_range");
        REDPANDA_BEGIN_IGNORE_DEPRECATIONS
    case errc::inconsistent_stm_update:
        return fmt::format_to(
          out, "cluster::errc::inconsistent_stm_update (deprecated)");
        REDPANDA_END_IGNORE_DEPRECATIONS
    }

    return fmt::format_to(
      out, "cluster::errc::unknown({})", static_cast<int>(err));
}
} // namespace cluster
