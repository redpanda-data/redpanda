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

#include "kafka/protocol/errors.h"
#include "seastarx.h"

#include <seastar/http/reply.hh>

#include <cstdint>
#include <system_error>

namespace pandaproxy {

// reply_error_code is a std::error_condition through which errors are returned
// in the reply.
//
// This is the default_error_condition for the proxy; to map other
// error_codes to this, override its error_category::default_error_condition().
enum class reply_error_code : uint16_t {
    continue_ = 100,
    switching_protocols = 101,
    ok = 200,
    created = 201,
    accepted = 202,
    nonauthoritative_information = 203,
    no_content = 204,
    reset_content = 205,
    partial_content = 206,
    multiple_choices = 300,
    moved_permanently = 301,
    moved_temporarily = 302,
    see_other = 303,
    not_modified = 304,
    use_proxy = 305,
    temporary_redirect = 307,
    bad_request = 400,
    unauthorized = 401,
    payment_required = 402,
    forbidden = 403,
    not_found = 404,
    method_not_allowed = 405,
    not_acceptable = 406,
    request_timeout = 408,
    conflict = 409,
    gone = 410,
    length_required = 411,
    payload_too_large = 413,
    uri_too_long = 414,
    unsupported_media_type = 415,
    expectation_failed = 417,
    unprocessable_entity = 422,
    upgrade_required = 426,
    too_many_requests = 429,
    internal_server_error = 500,
    not_implemented = 501,
    bad_gateway = 502,
    service_unavailable = 503,
    gateway_timeout = 504,
    http_version_not_supported = 505,
    insufficient_storage = 507,
    kafka_bad_request = 40002,
    kafka_authentication_error = 40101,
    kafka_authorization_error = 40301,
    topic_not_found = 40401,
    partition_not_found = 40402,
    consumer_instance_not_found = 40403,
    subject_soft_deleted = 40404,
    subject_not_deleted = 40405,
    subject_version_soft_deleted = 40406,
    subject_version_not_deleted = 40407,
    serialization_error = 40801,
    consumer_already_exists = 40902,
    schema_empty = 42201,
    schema_version_invalid = 42202,
    compatibility_level_invalid = 42203,
    subject_version_operaton_not_permitted = 42205,
    subject_version_has_references = 42206,
    subject_version_schema_id_already_exists = 42207,
    write_collision = 50301,
    zookeeper_error = 50001,
    kafka_error = 50002,
    kafka_retriable_error = 50003,
    ssl_unavailable = 50101,
    broker_not_available = 50302,
};

std::error_condition make_error_condition(reply_error_code);
std::error_condition make_error_condition(std::error_code ec);
std::error_condition make_error_condition(ss::http::reply::status_type ec);
const std::error_category& reply_category() noexcept;

} // namespace pandaproxy

namespace std {

template<>
struct is_error_condition_enum<pandaproxy::reply_error_code> : true_type {};

} // namespace std
