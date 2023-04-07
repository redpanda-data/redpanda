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
#include "kafka/protocol/schemata/leave_group_request.h"
#include "kafka/protocol/schemata/leave_group_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct leave_group_request final {
    using api_type = leave_group_api;

    leave_group_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;
    // additional context, set in decode
    api_version version;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const leave_group_request& r) {
        return os << r.data;
    }
};

struct leave_group_response final {
    using api_type = leave_group_api;

    leave_group_response_data data;

    leave_group_response() = default;

    explicit leave_group_response(error_code error)
      : data({
        .error_code = error,
      }) {}

    leave_group_response(const leave_group_request&, error_code error)
      : leave_group_response(error) {}

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const leave_group_response& r) {
        return os << r.data;
    }
};

inline ss::future<leave_group_response> make_leave_error(error_code error) {
    return ss::make_ready_future<leave_group_response>(error);
}

} // namespace kafka
