/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "error.h"

#include "kafka/protocol/errors.h"
#include "pandaproxy/parsing/error.h"

namespace pandaproxy {

namespace {

struct reply_error_category final : std::error_category {
    const char* name() const noexcept override { return "pandaproxy"; }
    std::string message(int ev) const override {
        switch (static_cast<reply_error_code>(ev)) {
        case reply_error_code::not_acceptable:
            return "HTTP 406 Not Acceptable";
        case reply_error_code::conflict:
            return "HTTP 409 Conflict";
        case reply_error_code::unsupported_media_type:
            return "HTTP 415 Unsupported Media Type";
        case reply_error_code::unprocessable_entity:
            return "HTTP 422 Unprocesable Entity";
        case reply_error_code::internal_server_error:
            return "HTTP 500 Internal Server Error";
        case reply_error_code::kafka_bad_request:
            return "kafka_bad_request";
        case reply_error_code::kafka_authentication_error:
            return "kafka_authentication_error";
        case reply_error_code::kafka_authorization_error:
            return "kafka_authorization_error";
        case reply_error_code::topic_not_found:
            return "topic_not_found";
        case reply_error_code::partition_not_found:
            return "partition_not_found";
        case reply_error_code::consumer_instance_not_found:
            return "consumer_instance_not_found";
        case reply_error_code::subject_soft_deleted:
            return "subject_soft_deleted";
        case reply_error_code::subject_not_deleted:
            return "subject_not_deleted";
        case reply_error_code::subject_version_soft_deleted:
            return "subject_version_soft_deleted";
        case reply_error_code::subject_version_not_deleted:
            return "subject_version_not_deleted";
        case reply_error_code::consumer_already_exists:
            return "Consumer with specified consumer ID already exists in the "
                   "specified consumer group.";
        case reply_error_code::schema_empty:
            return "Empty schema";
        case reply_error_code::schema_version_invalid:
            return "Invalid schema version";
        case reply_error_code::compatibility_level_invalid:
            return "Invalid compatibility level";
        case reply_error_code::subject_version_has_references:
            return "One or more references exist to the schema";
        case reply_error_code::write_collision:
            return "write_collision";
        case reply_error_code::zookeeper_error:
            return "zookeeper_error";
        case reply_error_code::kafka_error:
            return "kafka_error";
        case reply_error_code::kafka_retriable_error:
            return "kafka_retriable_error";
        case reply_error_code::ssl_unavailable:
            return "ssl_unavailable";
        case reply_error_code::broker_not_available:
            return "broker_not_available";
        }
        return "(unrecognized error)";
    }
};

const reply_error_category reply_error_category{};

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

}; // namespace

std::error_condition make_error_condition(reply_error_code ec) {
    return {static_cast<int>(ec), reply_error_category};
}

const std::error_category& reply_category() noexcept {
    return reply_error_category;
}

} // namespace pandaproxy

namespace kafka {
std::error_code make_error_code(kafka::error_code ec) {
    return {static_cast<int>(ec), pandaproxy::kafka_error_category};
}

} // namespace kafka