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

}; // namespace

std::error_condition make_error_condition(reply_error_code ec) {
    return {static_cast<int>(ec), reply_error_category};
}

const std::error_category& reply_category() noexcept {
    return reply_error_category;
}

} // namespace pandaproxy
