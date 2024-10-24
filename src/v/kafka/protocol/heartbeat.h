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
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/heartbeat_request.h"
#include "kafka/protocol/schemata/heartbeat_response.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace kafka {

struct heartbeat_request final {
    using api_type = heartbeat_api;

    heartbeat_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const heartbeat_request& r) {
        return os << r.data;
    }
};

struct heartbeat_response final {
    using api_type = heartbeat_api;

    heartbeat_response_data data;

    heartbeat_response() = default;

    explicit heartbeat_response(error_code error)
      : data({
          .error_code = error,
        }) {}

    heartbeat_response(const heartbeat_request&, error_code error)
      : heartbeat_response(error) {}

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const heartbeat_response& r) {
        return os << r.data;
    }
};

inline ss::future<heartbeat_response> make_heartbeat_error(error_code error) {
    return ss::make_ready_future<heartbeat_response>(error);
}

} // namespace kafka
