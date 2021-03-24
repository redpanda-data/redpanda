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

namespace pandaproxy {

namespace {

struct reply_error_category final : std::error_category {
    const char* name() const noexcept override { return "pandaproxy"; }
    std::string message(int ev) const override {
        switch (static_cast<reply_error_code>(ev)) {
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
