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
#include "kafka/requests/response.h"
#include "kafka/requests/schemata/list_groups_request.h"
#include "kafka/requests/schemata/list_groups_response.h"
#include "kafka/types.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct list_groups_response;

class list_groups_api final {
public:
    using response_type = list_groups_response;

    static constexpr const char* name = "list groups";
    static constexpr api_key key = api_key(16);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(2);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct list_groups_request final {
    using api_type = list_groups_api;

    list_groups_request_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const list_groups_request& r) {
    return os << r.data;
}

struct list_groups_response final {
    using api_type = list_groups_api;

    list_groups_response_data data;

    void encode(const request_context& ctx, response& resp);

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const list_groups_response& r) {
    return os << r.data;
}

} // namespace kafka
