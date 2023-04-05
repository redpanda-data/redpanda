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

#include "error.h"

#include "kafka/protocol/errors.h"
#include "pandaproxy/json/error.h"
#include "pandaproxy/parsing/error.h"

#include <seastar/http/reply.hh>

namespace pandaproxy {

namespace {

struct reply_error_category final : std::error_category {
    const char* name() const noexcept override { return "pandaproxy"; }
    std::string message(int ev) const override {
        switch (static_cast<reply_error_code>(ev)) {
        case reply_error_code::continue_:
            return "HTTP 100 Continue";
        case reply_error_code::switching_protocols:
            return "HTTP 101 Switching Protocols";
        case reply_error_code::ok:
            return "HTTP 200 OK";
        case reply_error_code::created:
            return "HTTP 201 Created";
        case reply_error_code::accepted:
            return "HTTP 202 Accepted";
        case reply_error_code::nonauthoritative_information:
            return "HTTP 203 Non-Authoritative Information";
        case reply_error_code::no_content:
            return "HTTP 204 No Content";
        case reply_error_code::reset_content:
            return "HTTP 205 Reset Content";
        case reply_error_code::partial_content:
            return "HTTP 206 Partial Content";
        case reply_error_code::multiple_choices:
            return "HTTP 300 Multiple Choices";
        case reply_error_code::moved_permanently:
            return "HTTP 301 Moved Permanently";
        case reply_error_code::moved_temporarily:
            return "HTTP 302 Moved Temporarily";
        case reply_error_code::see_other:
            return "HTTP 303 See Other";
        case reply_error_code::not_modified:
            return "HTTP 304 Not Modified";
        case reply_error_code::use_proxy:
            return "HTTP 305 Use Proxy";
        case reply_error_code::temporary_redirect:
            return "HTTP 307 Temporary Redirect";
        case reply_error_code::bad_request:
            return "HTTP 400 Bad Request";
        case reply_error_code::unauthorized:
            return "HTTP 401 Unauthorized";
        case reply_error_code::payment_required:
            return "HTTP 402 Payment Required";
        case reply_error_code::forbidden:
            return "HTTP 403 Forbidden";
        case reply_error_code::not_found:
            return "HTTP 404 Not Found";
        case reply_error_code::method_not_allowed:
            return "HTTP 405 Method Not Allowed";
        case reply_error_code::not_acceptable:
            return "HTTP 406 Not Acceptable";
        case reply_error_code::request_timeout:
            return "HTTP 408 Request Timeout";
        case reply_error_code::conflict:
            return "HTTP 409 Conflict";
        case reply_error_code::gone:
            return "HTTP 410 Gone";
        case reply_error_code::length_required:
            return "HTTP 411 Length Required";
        case reply_error_code::payload_too_large:
            return "HTTP 413 Payload Too Large";
        case reply_error_code::uri_too_long:
            return "HTTP 414 URI Too Long";
        case reply_error_code::unsupported_media_type:
            return "HTTP 415 Unsupported Media Type";
        case reply_error_code::expectation_failed:
            return "HTTP 417 Expectation Failed";
        case reply_error_code::unprocessable_entity:
            return "HTTP 422 Unprocesable Entity";
        case reply_error_code::upgrade_required:
            return "HTTP 426 Upgrade Required";
        case reply_error_code::too_many_requests:
            return "HTTP 429 Too Many Requests";
        case reply_error_code::internal_server_error:
            return "HTTP 500 Internal Server Error";
        case reply_error_code::not_implemented:
            return "HTTP 501 Not Implemented";
        case reply_error_code::bad_gateway:
            return "HTTP 502 Bad Gateway";
        case reply_error_code::service_unavailable:
            return "HTTP 503 Service Unavailable";
        case reply_error_code::gateway_timeout:
            return "HTTP 504 Gateway Unavailable";
        case reply_error_code::http_version_not_supported:
            return "HTTP 505 HTTP Version Not Supported";
        case reply_error_code::insufficient_storage:
            return "HTTP 507 Insufficient Storage";
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
        case reply_error_code::serialization_error:
            return "serialization_error";
        case reply_error_code::consumer_already_exists:
            return "Consumer with specified consumer ID already exists in the "
                   "specified consumer group.";
        case reply_error_code::schema_empty:
            return "Empty schema";
        case reply_error_code::schema_version_invalid:
            return "Invalid schema version";
        case reply_error_code::compatibility_level_invalid:
            return "Invalid compatibility level";
        case reply_error_code::subject_version_operaton_not_permitted:
            return "Overwrite new schema is not permitted.";
        case reply_error_code::subject_version_has_references:
            return "One or more references exist to the schema";
        case reply_error_code::subject_version_schema_id_already_exists:
            return "Schema already registered with another id";
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
    bool
    equivalent(const std::error_code& ec, int cond) const noexcept override {
        return make_error_condition(ec).value() == cond;
    }
};

const reply_error_category reply_error_category{};

}; // namespace

std::error_condition make_error_condition(std::error_code ec) {
    using rec = reply_error_code;
    using kec = kafka::error_code;
    using pec = pandaproxy::parse::error_code;
    using jec = pandaproxy::json::error_code;

    if (ec.category() == make_error_code(kec::none).category()) {
        switch (static_cast<kec>(ec.value())) {
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
        case kec::transactional_id_not_found:
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
        case kec::group_subscribed_to_topic:
        case kec::unstable_offset_commit:
        case kec::no_reassignment_in_progress:
            return rec::kafka_error;
        case kec::network_exception:
        case kec::coordinator_load_in_progress:
        case kec::request_timed_out:
        case kec::rebalance_in_progress:
        case kec::throttling_quota_exceeded:
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
    } else if (ec.category() == make_error_code(pec::empty_param).category()) {
        switch (static_cast<pec>(ec.value())) {
        case pec::empty_param:
        case pec::invalid_param:
            return rec::kafka_bad_request;
        case pec::not_acceptable:
            return rec::not_acceptable;
        case pec::unsupported_media_type:
            return rec::unsupported_media_type;
        }
        return {};
    } else if (ec.category() == make_error_code(jec::invalid_json).category()) {
        switch (static_cast<jec>(ec.value())) {
        case jec::invalid_json:
            return rec::unprocessable_entity;
        case jec::unable_to_serialize:
            return rec::serialization_error;
        }
        return {};
    }
    return ec.default_error_condition();
}

std::error_condition make_error_condition(reply_error_code ec) {
    return {static_cast<int>(ec), reply_error_category};
}

std::error_condition make_error_condition(ss::http::reply::status_type st) {
    using rec = reply_error_code;
    using sec = ss::http::reply::status_type;

    switch (st) {
    case sec::continue_:
        return rec::continue_;
    case sec::switching_protocols:
        return rec::switching_protocols;
    case sec::ok:
        return rec{};
    case sec::created:
        return rec::created;
    case sec::accepted:
        return rec::accepted;
    case sec::nonauthoritative_information:
        return rec::nonauthoritative_information;
    case sec::no_content:
        return rec::no_content;
    case sec::reset_content:
        return rec::reset_content;
    case sec::multiple_choices:
        return rec::multiple_choices;
    case sec::moved_permanently:
        return rec::moved_permanently;
    case sec::moved_temporarily:
        return rec::moved_temporarily;
    case sec::see_other:
        return rec::see_other;
    case sec::not_modified:
        return rec::not_modified;
    case sec::use_proxy:
        return rec::use_proxy;
    case sec::temporary_redirect:
        return rec::temporary_redirect;
    case sec::bad_request:
        return rec::kafka_bad_request;
    case sec::unauthorized:
        return rec::kafka_authentication_error;
    case sec::payment_required:
        return rec::payment_required;
    case sec::forbidden:
        return rec::forbidden;
    case sec::not_found:
        return rec::not_found;
    case sec::method_not_allowed:
        return rec::method_not_allowed;
    case sec::not_acceptable:
        return rec::not_acceptable;
    case sec::request_timeout:
        return rec::request_timeout;
    case sec::conflict:
        return rec::conflict;
    case sec::gone:
        return rec::gone;
    case sec::length_required:
        return rec::length_required;
    case sec::payload_too_large:
        return rec::payload_too_large;
    case sec::uri_too_long:
        return rec::uri_too_long;
    case sec::unsupported_media_type:
        return rec::unsupported_media_type;
    case sec::expectation_failed:
        return rec::expectation_failed;
    case sec::unprocessable_entity:
        return rec::unprocessable_entity;
    case sec::upgrade_required:
        return rec::upgrade_required;
    case sec::too_many_requests:
        return rec::too_many_requests;
    case sec::internal_server_error:
        return rec::internal_server_error;
    case sec::not_implemented:
        return rec::not_implemented;
    case sec::bad_gateway:
        return rec::bad_gateway;
    case sec::service_unavailable:
        return rec::service_unavailable;
    case sec::gateway_timeout:
        return rec::gateway_timeout;
    case sec::http_version_not_supported:
        return rec::http_version_not_supported;
    case sec::insufficient_storage:
        return rec::insufficient_storage;
    case sec::partial_content:
        return rec::partial_content;
    }
    return rec::kafka_bad_request;
}

const std::error_category& reply_category() noexcept {
    return reply_error_category;
}

} // namespace pandaproxy
