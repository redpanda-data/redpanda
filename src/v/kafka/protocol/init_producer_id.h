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

#include "bytes/iobuf.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/init_producer_id_request.h"
#include "kafka/protocol/schemata/init_producer_id_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct init_producer_id_response;

struct init_producer_id_api final {
    using response_type = init_producer_id_response;

    static constexpr const char* name = "init producer_id";
    static constexpr api_key key = api_key(22);
};

struct init_producer_id_request final {
    using api_type = init_producer_id_api;

    init_producer_id_request_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const init_producer_id_request& r) {
    return os << r.data;
}

struct init_producer_id_response final {
    using api_type = init_producer_id_api;

    init_producer_id_response_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const init_producer_id_response& r) {
    return os << r.data;
}

} // namespace kafka
