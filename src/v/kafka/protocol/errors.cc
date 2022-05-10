// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/errors.h"

#include "pandaproxy/error.h"

#include <iostream>

namespace pandaproxy {

namespace {
struct kafka_error_category final : std::error_category {
    const char* name() const noexcept override { return "kafka"; }
    std::string message(int ec) const override {
        return std::string(
          kafka::error_code_to_str(static_cast<kafka::error_code>(ec)));
    }
    std::error_condition
    default_error_condition(int ec) const noexcept override {
        using rec = reply_error_code;
        using kec = kafka::error_code;

        switch (static_cast<kec>(ec)) {
        case kec::none:
            return {};
        case kec::offset_out_of_range:
        case kec::corrupt_message:
        case kec::invalid_fetch_size:
        case kec::not_leader_for_partition:
        case kec::message_too_large:
        case kec::stale_controller_epoch:
        case kec::offset_metadata_too_large:
        case kec::invalid_topic_exception:
        case kec::record_list_too_large:
        case kec::illegal_generation:
        case kec::inconsistent_group_protocol:
        case kec::invalid_group_id:
        case kec::invalid_session_timeout:
        case kec::invalid_commit_offset_size:
        case kec::invalid_timestamp:
        case kec::unsupported_sasl_mechanism:
        case kec::illegal_sasl_state:
        case kec::unsupported_version:
        case kec::topic_already_exists:
        case kec::invalid_partitions:
        case kec::invalid_replication_factor:
        case kec::invalid_replica_assignment:
        case kec::invalid_config:
        case kec::not_controller:
        case kec::invalid_request:
        case kec::unsupported_for_message_format:
        case kec::policy_violation:
        case kec::invalid_required_acks:
        case kec::invalid_producer_epoch:
        case kec::invalid_txn_state:
        case kec::invalid_producer_id_mapping:
        case kec::invalid_transaction_timeout:
        case kec::concurrent_transactions:
        case kec::transaction_coordinator_fenced:
        case kec::security_disabled:
        case kec::log_dir_not_found:
        case kec::unknown_producer_id:
        case kec::delegation_token_not_found:
        case kec::delegation_token_owner_mismatch:
        case kec::delegation_token_request_not_allowed:
        case kec::delegation_token_expired:
        case kec::invalid_principal_type:
        case kec::non_empty_group:
        case kec::group_id_not_found:
        case kec::fetch_session_id_not_found:
        case kec::invalid_fetch_session_epoch:
        case kec::listener_not_found:
        case kec::topic_deletion_disabled:
        case kec::fenced_leader_epoch:
        case kec::unknown_leader_epoch:
        case kec::unsupported_compression_type:
        case kec::stale_broker_epoch:
        case kec::offset_not_available:
        case kec::member_id_required:
        case kec::group_max_size_reached:
        case kec::fenced_instance_id:
        case kec::invalid_record:
            return rec::kafka_bad_request;
        case kec::not_enough_replicas:
        case kec::coordinator_not_available:
        case kec::not_coordinator:
        case kec::not_enough_replicas_after_append:
        case kec::out_of_order_sequence_number:
        case kec::duplicate_sequence_number:
        case kec::operation_not_attempted:
        case kec::kafka_storage_error:
        case kec::unknown_server_error:
            return rec::kafka_error;
        case kec::network_exception:
        case kec::coordinator_load_in_progress:
        case kec::request_timed_out:
        case kec::rebalance_in_progress:
        case kec::reassignment_in_progress:
            return rec::kafka_retriable_error;
        case kec::unknown_topic_or_partition:
            return rec::partition_not_found;
        case kec::unknown_member_id:
            return rec::consumer_instance_not_found;
        case kec::leader_not_available:
        case kec::broker_not_available:
        case kec::replica_not_available:
        case kec::preferred_leader_not_available:
            return rec::broker_not_available;
        case kec::topic_authorization_failed:
        case kec::group_authorization_failed:
        case kec::transactional_id_authorization_failed:
        case kec::delegation_token_authorization_failed:
        case kec::cluster_authorization_failed:
            return rec::kafka_authorization_error;
        case kec::sasl_authentication_failed:
        case kec::delegation_token_auth_disabled: // authn / authz? bad requ?
            return rec::kafka_authentication_error;
        }
        return {}; // keep gcc happy
    }
};

const kafka_error_category kafka_error_category{};

} // namespace
} // namespace pandaproxy

namespace kafka {

std::string_view error_code_to_str(error_code error) {
    switch (error) {
    case error_code::none:
        return "none";
    case error_code::offset_out_of_range:
        return "offset_out_of_range";
    case error_code::corrupt_message:
        return "corrupt_message";
    case error_code::unknown_topic_or_partition:
        return "unknown_topic_or_partition";
    case error_code::invalid_fetch_size:
        return "invalid_fetch_size";
    case error_code::leader_not_available:
        return "leader_not_available";
    case error_code::not_leader_for_partition:
        return "not_leader_for_partition";
    case error_code::request_timed_out:
        return "request_timed_out";
    case error_code::broker_not_available:
        return "broker_not_available";
    case error_code::replica_not_available:
        return "replica_not_available";
    case error_code::message_too_large:
        return "message_too_large";
    case error_code::stale_controller_epoch:
        return "stale_controller_epoch";
    case error_code::offset_metadata_too_large:
        return "offset_metadata_too_large";
    case error_code::network_exception:
        return "network_exception";
    case error_code::coordinator_load_in_progress:
        return "coordinator_load_in_progress";
    case error_code::coordinator_not_available:
        return "coordinator_not_available";
    case error_code::not_coordinator:
        return "not_coordinator";
    case error_code::invalid_topic_exception:
        return "invalid_topic_exception";
    case error_code::record_list_too_large:
        return "record_list_too_large";
    case error_code::not_enough_replicas:
        return "not_enough_replicas";
    case error_code::not_enough_replicas_after_append:
        return "not_enough_replicas_after_append";
    case error_code::invalid_required_acks:
        return "invalid_required_acks";
    case error_code::illegal_generation:
        return "illegal_generation";
    case error_code::inconsistent_group_protocol:
        return "inconsistent_group_protocol";
    case error_code::invalid_group_id:
        return "invalid_group_id";
    case error_code::unknown_member_id:
        return "unknown_member_id";
    case error_code::invalid_session_timeout:
        return "invalid_session_timeout";
    case error_code::rebalance_in_progress:
        return "rebalance_in_progress";
    case error_code::invalid_commit_offset_size:
        return "invalid_commit_offset_size";
    case error_code::topic_authorization_failed:
        return "topic_authorization_failed";
    case error_code::group_authorization_failed:
        return "group_authorization_failed";
    case error_code::cluster_authorization_failed:
        return "cluster_authorization_failed";
    case error_code::invalid_timestamp:
        return "invalid_timestamp";
    case error_code::unsupported_sasl_mechanism:
        return "unsupported_sasl_mechanism";
    case error_code::illegal_sasl_state:
        return "illegal_sasl_state";
    case error_code::unsupported_version:
        return "unsupported_version";
    case error_code::topic_already_exists:
        return "topic_already_exists";
    case error_code::invalid_partitions:
        return "invalid_partitions";
    case error_code::invalid_replication_factor:
        return "invalid_replication_factor";
    case error_code::invalid_replica_assignment:
        return "invalid_replica_assignment";
    case error_code::invalid_config:
        return "invalid_config";
    case error_code::not_controller:
        return "not_controller";
    case error_code::invalid_request:
        return "invalid_request";
    case error_code::unsupported_for_message_format:
        return "unsupported_for_message_format";
    case error_code::policy_violation:
        return "policy_violation";
    case error_code::out_of_order_sequence_number:
        return "out_of_order_sequence_number";
    case error_code::duplicate_sequence_number:
        return "duplicate_sequence_number";
    case error_code::invalid_producer_epoch:
        return "invalid_producer_epoch";
    case error_code::invalid_txn_state:
        return "invalid_txn_state";
    case error_code::invalid_producer_id_mapping:
        return "invalid_producer_id_mapping";
    case error_code::invalid_transaction_timeout:
        return "invalid_transaction_timeout";
    case error_code::concurrent_transactions:
        return "concurrent_transactions";
    case error_code::transaction_coordinator_fenced:
        return "transaction_coordinator_fenced";
    case error_code::transactional_id_authorization_failed:
        return "transactional_id_authorization_failed";
    case error_code::security_disabled:
        return "security_disabled";
    case error_code::operation_not_attempted:
        return "operation_not_attempted";
    case error_code::kafka_storage_error:
        return "kafka_storage_error";
    case error_code::log_dir_not_found:
        return "log_dir_not_found";
    case error_code::sasl_authentication_failed:
        return "sasl_authentication_failed";
    case error_code::unknown_producer_id:
        return "unknown_producer_id";
    case error_code::reassignment_in_progress:
        return "reassignment_in_progress";
    case error_code::delegation_token_auth_disabled:
        return "delegation_token_auth_disabled";
    case error_code::delegation_token_not_found:
        return "delegation_token_not_found";
    case error_code::delegation_token_owner_mismatch:
        return "delegation_token_owner_mismatch";
    case error_code::delegation_token_request_not_allowed:
        return "delegation_token_request_not_allowed";
    case error_code::delegation_token_authorization_failed:
        return "delegation_token_authorization_failed";
    case error_code::delegation_token_expired:
        return "delegation_token_expired";
    case error_code::invalid_principal_type:
        return "invalid_principal_type";
    case error_code::non_empty_group:
        return "non_empty_group";
    case error_code::group_id_not_found:
        return "group_id_not_found";
    case error_code::fetch_session_id_not_found:
        return "fetch_session_id_not_found";
    case error_code::invalid_fetch_session_epoch:
        return "invalid_fetch_session_epoch";
    case error_code::listener_not_found:
        return "listener_not_found";
    case error_code::topic_deletion_disabled:
        return "topic_deletion_disabled";
    case error_code::fenced_leader_epoch:
        return "fenced_leader_epoch";
    case error_code::unknown_leader_epoch:
        return "unknown_leader_epoch";
    case error_code::unsupported_compression_type:
        return "unsupported_compression_type";
    case error_code::stale_broker_epoch:
        return "stale_broker_epoch";
    case error_code::offset_not_available:
        return "offset_not_available";
    case error_code::member_id_required:
        return "member_id_required";
    case error_code::preferred_leader_not_available:
        return "preferred_leader_not_available";
    case error_code::group_max_size_reached:
        return "group_max_size_reached";
    case error_code::fenced_instance_id:
        return "fenced_instance_id";
    case error_code::unknown_server_error:
        return "unknown_server_error";
    case error_code::invalid_record:
        return "invalid_record";
    default:
        std::terminate(); // make gcc happy
    }
}

std::ostream& operator<<(std::ostream& o, error_code code) {
    o << "{ error_code: ";
    // special case as unknown_server_error = -1
    if (code == error_code::unknown_server_error) {
        o << "unknown_server_error";
    } else {
        o << error_code_to_str(code);
    }
    o << " [" << (int16_t)code << "] }";
    return o;
}

std::error_code make_error_code(kafka::error_code ec) {
    return {static_cast<int>(ec), pandaproxy::kafka_error_category};
}

} // namespace kafka
