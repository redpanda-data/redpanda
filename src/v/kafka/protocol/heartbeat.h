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
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/heartbeat_request.h"
#include "kafka/protocol/schemata/heartbeat_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct heartbeat_response;

struct heartbeat_api final {
    using response_type = heartbeat_response;

    static constexpr const char* name = "heartbeat";
    static constexpr api_key key = api_key(12);
};

struct heartbeat_request final {
    using api_type = heartbeat_api;

    heartbeat_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

inline std::ostream& operator<<(std::ostream& os, const heartbeat_request& r) {
    return os << r.data;
}

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

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

inline ss::future<heartbeat_response> make_heartbeat_error(error_code error) {
    return ss::make_ready_future<heartbeat_response>(error);
}

inline std::ostream& operator<<(std::ostream& os, const heartbeat_response& r) {
    return os << r.data;
}

} // namespace kafka
