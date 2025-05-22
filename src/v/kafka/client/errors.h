/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/protocol/errors.h"
namespace kafka::client {
/**
 * List of retriable errors that can be returned by the broker. This list is
 * slightly different from the list of retriable errors defined in the protocol.
 * (based on the franz-go retriable errors list)
 */
inline bool is_retriable_error(kafka::error_code ec) {
    switch (ec) {
    case error_code::none:
        return false;
    case error_code::offset_out_of_range:
    case error_code::unknown_topic_or_partition:
    case error_code::corrupt_message:
    case error_code::leader_not_available:
    case error_code::not_leader_for_partition:
    case error_code::network_exception:
    case error_code::coordinator_load_in_progress:
    case error_code::coordinator_not_available:
    case error_code::not_enough_replicas:
    case error_code::not_enough_replicas_after_append:
    case error_code::not_controller:
    case error_code::fetch_session_id_not_found:
    case error_code::invalid_fetch_session_epoch:
    case error_code::fenced_leader_epoch:
    case error_code::unknown_leader_epoch:
    case error_code::listener_not_found:
    case error_code::kafka_storage_error:
    case error_code::offset_not_available:
    case error_code::preferred_leader_not_available:
    case error_code::unstable_offset_commit:
    case error_code::throttling_quota_exceeded:
    case error_code::unknown_topic_id:
        return true;
    case error_code::unknown_server_error:
    case error_code::invalid_fetch_size:
    case error_code::request_timed_out:
    case error_code::broker_not_available:
    case error_code::replica_not_available:
    case error_code::message_too_large:
    case error_code::stale_controller_epoch:
    case error_code::offset_metadata_too_large:
    case error_code::not_coordinator:
    case error_code::invalid_topic_exception:
    case error_code::record_list_too_large:
    case error_code::invalid_required_acks:
    case error_code::illegal_generation:
    case error_code::inconsistent_group_protocol:
    case error_code::invalid_group_id:
    case error_code::unknown_member_id:
    case error_code::invalid_session_timeout:
    case error_code::rebalance_in_progress:
    case error_code::invalid_commit_offset_size:
    case error_code::topic_authorization_failed:
    case error_code::group_authorization_failed:
    case error_code::cluster_authorization_failed:
    case error_code::invalid_timestamp:
    case error_code::unsupported_sasl_mechanism:
    case error_code::illegal_sasl_state:
    case error_code::unsupported_version:
    case error_code::topic_already_exists:
    case error_code::invalid_partitions:
    case error_code::invalid_replication_factor:
    case error_code::invalid_replica_assignment:
    case error_code::invalid_config:
    case error_code::invalid_request:
    case error_code::unsupported_for_message_format:
    case error_code::policy_violation:
    case error_code::out_of_order_sequence_number:
    case error_code::duplicate_sequence_number:
    case error_code::invalid_producer_epoch:
    case error_code::invalid_txn_state:
    case error_code::invalid_producer_id_mapping:
    case error_code::invalid_transaction_timeout:
    case error_code::concurrent_transactions:
    case error_code::transaction_coordinator_fenced:
    case error_code::transactional_id_authorization_failed:
    case error_code::security_disabled:
    case error_code::operation_not_attempted:
    case error_code::log_dir_not_found:
    case error_code::sasl_authentication_failed:
    case error_code::unknown_producer_id:
    case error_code::reassignment_in_progress:
    case error_code::delegation_token_auth_disabled:
    case error_code::delegation_token_not_found:
    case error_code::delegation_token_owner_mismatch:
    case error_code::delegation_token_request_not_allowed:
    case error_code::delegation_token_authorization_failed:
    case error_code::delegation_token_expired:
    case error_code::invalid_principal_type:
    case error_code::non_empty_group:
    case error_code::group_id_not_found:
    case error_code::topic_deletion_disabled:
    case error_code::unsupported_compression_type:
    case error_code::stale_broker_epoch:
    case error_code::member_id_required:
    case error_code::group_max_size_reached:
    case error_code::no_reassignment_in_progress:
    case error_code::fenced_instance_id:
    case error_code::group_subscribed_to_topic:
    case error_code::invalid_record:
    case error_code::resource_not_found:
    case error_code::duplicate_resource:
    case error_code::unacceptable_credential:
    case error_code::transactional_id_not_found:
        return false;
    }
}

} // namespace kafka::client
