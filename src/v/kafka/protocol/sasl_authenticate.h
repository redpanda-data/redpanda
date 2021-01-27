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

#include "kafka/protocol/schemata/sasl_authenticate_request.h"
#include "kafka/protocol/schemata/sasl_authenticate_response.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "kafka/types.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct sasl_authenticate_response;

class sasl_authenticate_api final {
public:
    using response_type = sasl_authenticate_response;

    static constexpr const char* name = "sasl authenticate";
    static constexpr api_key key = api_key(36);
};

struct sasl_authenticate_request final {
    using api_type = sasl_authenticate_api;

    sasl_authenticate_request_data data;

    sasl_authenticate_request() = default;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const sasl_authenticate_request& r) {
    return os << r.data;
}

struct sasl_authenticate_response final {
    using api_type = sasl_authenticate_api;

    sasl_authenticate_response_data data;

    explicit sasl_authenticate_response(
      error_code error, ss::sstring error_msg) {
        data.error_code = error;
        data.error_message = std::move(error_msg);
    }

    void encode(const request_context& ctx, response& resp) {
        data.encode(resp.writer(), ctx.header().version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const sasl_authenticate_response& r) {
    return os << r.data;
}

} // namespace kafka
