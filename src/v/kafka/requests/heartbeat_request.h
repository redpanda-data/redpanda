/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "kafka/errors.h"
#include "kafka/requests/response.h"
#include "kafka/requests/schemata/heartbeat_request.h"
#include "kafka/requests/schemata/heartbeat_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct heartbeat_api final {
    static constexpr const char* name = "heartbeat";
    static constexpr api_key key = api_key(12);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(3);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct heartbeat_request final {
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

    explicit heartbeat_response(error_code error)
      : data({
        .throttle_time_ms = std::chrono::milliseconds(0),
        .error_code = error,
      }) {}

    heartbeat_response(const heartbeat_request&, error_code error)
      : heartbeat_response(error) {}

    void encode(const request_context&, response&);
};

inline ss::future<heartbeat_response> make_heartbeat_error(error_code error) {
    return ss::make_ready_future<heartbeat_response>(error);
}

inline std::ostream& operator<<(std::ostream& os, const heartbeat_response& r) {
    return os << r.data;
}

} // namespace kafka
