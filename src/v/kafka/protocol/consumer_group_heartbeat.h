/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/seastarx.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/consumer_group_heartbeat_request.h"
#include "kafka/protocol/schemata/consumer_group_heartbeat_response.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace kafka {

struct consumer_group_heartbeat_request final {
    using api_type = consumer_group_heartbeat_api;

    consumer_group_heartbeat_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    // connection context of the requester, set during request processing and
    // recorded on the member for ConsumerGroupDescribe.
    std::optional<kafka::client_id> client_id;
    kafka::client_host client_host;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", data);
    }
};

struct consumer_group_heartbeat_response final {
    using api_type = consumer_group_heartbeat_api;

    consumer_group_heartbeat_response_data data;

    consumer_group_heartbeat_response() = default;

    explicit consumer_group_heartbeat_response(error_code error)
      : data({
          .error_code = error,
        }) {}

    consumer_group_heartbeat_response(
      error_code error, ss::sstring error_message)
      : data({
          .error_code = error,
          .error_message = std::move(error_message),
        }) {}

    consumer_group_heartbeat_response(
      const consumer_group_heartbeat_request&, error_code error)
      : consumer_group_heartbeat_response(error) {}

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", data);
    }
};

} // namespace kafka
