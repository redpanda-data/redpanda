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
#include "kafka/protocol/schemata/consumer_group_describe_request.h"
#include "kafka/protocol/schemata/consumer_group_describe_response.h"

#include <seastar/core/future.hh>

namespace kafka {

struct consumer_group_describe_request final {
    using api_type = consumer_group_describe_api;

    consumer_group_describe_request_data data;

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

struct consumer_group_describe_response final {
    using api_type = consumer_group_describe_api;

    consumer_group_describe_response_data data;

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

/// Build a per-group error entry for the ConsumerGroupDescribe response.
inline consumer_group_described_group make_described_group_error(
  kafka::group_id group_id, error_code error, ss::sstring error_message) {
    return {
      .error_code = error,
      .error_message = std::move(error_message),
      .group_id = std::move(group_id),
    };
}

} // namespace kafka
