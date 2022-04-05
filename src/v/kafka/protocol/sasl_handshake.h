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

#include "kafka/protocol/schemata/sasl_handshake_request.h"
#include "kafka/protocol/schemata/sasl_handshake_response.h"
#include "kafka/types.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct sasl_handshake_response;

class sasl_handshake_api final {
public:
    using response_type = sasl_handshake_response;

    static constexpr const char* name = "sasl handshake";
    static constexpr api_key key = api_key(17);
};

struct sasl_handshake_request final {
    using api_type = sasl_handshake_api;

    sasl_handshake_request_data data;

    sasl_handshake_request() = default;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const sasl_handshake_request& r) {
    return os << r.data;
}

struct sasl_handshake_response final {
    using api_type = sasl_handshake_api;

    sasl_handshake_response_data data;

    sasl_handshake_response() = default;

    sasl_handshake_response(
      error_code error, std::vector<ss::sstring> mechanisms) {
        data.error_code = error;
        data.mechanisms = std::move(mechanisms);
    }

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const sasl_handshake_response& r) {
    return os << r.data;
}

} // namespace kafka
