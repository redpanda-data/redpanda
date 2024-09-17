/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "kafka/protocol/schemata/find_coordinator_request.h"
#include "kafka/protocol/schemata/find_coordinator_response.h"

#include <seastar/core/future.hh>

namespace kafka {

struct find_coordinator_request final {
    using api_type = find_coordinator_api;

    find_coordinator_request_data data;

    find_coordinator_request() = default;

    find_coordinator_request(
      ss::sstring key, coordinator_type key_type = coordinator_type::group)
      : data({
          .key = std::move(key),
          .key_type = key_type,
        }) {}

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const find_coordinator_request& r) {
        return os << r.data;
    }
};

struct find_coordinator_response final {
    using api_type = find_coordinator_api;

    find_coordinator_response_data data;

    find_coordinator_response() = default;

    find_coordinator_response(
      error_code error,
      std::optional<ss::sstring> error_message,
      model::node_id node,
      ss::sstring host,
      int32_t port)
      : data({
          .error_code = error,
          .error_message = std::move(error_message),
          .node_id = node,
          .host = std::move(host),
          .port = port,
        }) {}

    find_coordinator_response(
      model::node_id node, ss::sstring host, int32_t port)
      : find_coordinator_response(
          error_code::none, std::nullopt, node, std::move(host), port) {}

    find_coordinator_response(error_code error, ss::sstring error_message)
      : find_coordinator_response(
          error, std::move(error_message), model::node_id(-1), "", -1) {}

    explicit find_coordinator_response(error_code error)
      : find_coordinator_response(
          error, std::nullopt, model::node_id(-1), "", -1) {}

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const find_coordinator_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
