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

#include "bytes/iobuf.h"
#include "kafka/errors.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "kafka/requests/schemata/init_producer_id_request.h"
#include "kafka/requests/schemata/init_producer_id_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#define SUPPRESS_IDEMPOTENT_PRODUCER

namespace kafka {

struct init_producer_id_response;

struct init_producer_id_api final {
    using response_type = init_producer_id_response;

    static constexpr const char* name = "init producer_id";
    static constexpr api_key key = api_key(22);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(1);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
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

static inline std::ostream&
operator<<(std::ostream& os, const init_producer_id_request& r) {
    return os << r.data;
}

struct init_producer_id_response final {
    using api_type = init_producer_id_api;

    init_producer_id_response_data data;

    void encode(const request_context& ctx, response& resp) {
        data.encode(resp.writer(), ctx.header().version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

static inline std::ostream&
operator<<(std::ostream& os, const init_producer_id_response& r) {
    return os << r.data;
}

} // namespace kafka
