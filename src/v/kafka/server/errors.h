/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "cluster/errc.h"
#include "kafka/protocol/errors.h"

namespace kafka {

constexpr error_code map_topic_error_code(cluster::errc code) {
    switch (code) {
    case cluster::errc::success:
        return error_code::none;
    case cluster::errc::topic_invalid_config:
        return error_code::invalid_config;
    case cluster::errc::topic_invalid_partitions:
        return error_code::invalid_partitions;
    case cluster::errc::topic_invalid_replication_factor:
        return error_code::invalid_replication_factor;
    case cluster::errc::notification_wait_timeout:
        return error_code::request_timed_out;
    case cluster::errc::not_leader_controller:
    case cluster::errc::no_leader_controller:
        return error_code::not_controller;
    case cluster::errc::topic_already_exists:
        return error_code::topic_already_exists;
    case cluster::errc::topic_not_exists:
        return error_code::unknown_topic_or_partition;
    case cluster::errc::source_topic_still_in_use:
        return error_code::cluster_authorization_failed;
    case cluster::errc::timeout:
        return error_code::request_timed_out;
    case cluster::errc::invalid_topic_name:
        return error_code::invalid_topic_exception;
    case cluster::errc::no_eligible_allocation_nodes:
        return error_code::broker_not_available;
    case cluster::errc::not_leader:
        return error_code::not_coordinator;
    case cluster::errc::invalid_request:
        return error_code::invalid_request;
    case cluster::errc::throttling_quota_exceeded:
        return error_code::throttling_quota_exceeded;
    case cluster::errc::no_update_in_progress:
        return error_code::no_reassignment_in_progress;
    case cluster::errc::replication_error:
    case cluster::errc::shutting_down:
    case cluster::errc::join_request_dispatch_error:
    case cluster::errc::source_topic_not_exists:
    case cluster::errc::seed_servers_exhausted:
    case cluster::errc::auto_create_topics_exception:
    case cluster::errc::partition_not_exists:
    case cluster::errc::partition_already_exists:
    case cluster::errc::waiting_for_recovery:
    case cluster::errc::waiting_for_reconfiguration_finish:
    case cluster::errc::update_in_progress:
    case cluster::errc::user_exists:
    case cluster::errc::user_does_not_exist:
    case cluster::errc::invalid_producer_epoch:
    case cluster::errc::sequence_out_of_order:
    case cluster::errc::generic_tx_error:
    case cluster::errc::node_does_not_exists:
    case cluster::errc::invalid_node_operation:
    case cluster::errc::invalid_configuration_update:
    case cluster::errc::topic_operation_error:
    case cluster::errc::allocation_error:
    case cluster::errc::partition_configuration_revision_not_updated:
    case cluster::errc::partition_configuration_in_joint_mode:
    case cluster::errc::partition_configuration_leader_config_not_committed:
    case cluster::errc::partition_configuration_differs:
    case cluster::errc::data_policy_already_exists:
    case cluster::errc::data_policy_not_exists:
    case cluster::errc::waiting_for_partition_shutdown:
    case cluster::errc::error_collecting_health_report:
    case cluster::errc::leadership_changed:
    case cluster::errc::feature_disabled:
    case cluster::errc::unknown_update_interruption_error:
    case cluster::errc::cluster_already_exists:
    case cluster::errc::no_partition_assignments:
    case cluster::errc::failed_to_create_partition:
        break;
    }
    return error_code::unknown_server_error;
}

} // namespace kafka
