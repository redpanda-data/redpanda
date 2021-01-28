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

#include "kafka/protocol/schemata/api_versions_request.h"
#include "kafka/protocol/schemata/api_versions_response.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct api_versions_response;

struct api_versions_api final {
    using response_type = api_versions_response;

    static constexpr const char* name = "api-versions";
    static constexpr api_key key = api_key(18);
};

struct api_versions_request final {
    using api_type = api_versions_api;

    api_versions_request_data data;

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }
};

struct api_versions_response final {
    using api_type = api_versions_api;

    api_versions_response_data data;

    void encode(const request_context& ctx, response& resp) {
        data.encode(resp.writer(), ctx.header().version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

std::ostream& operator<<(std::ostream&, const api_versions_response&);

inline bool operator==(
  const api_versions_response_key& a, const api_versions_response_key& b) {
    return a.api_key == b.api_key && a.min_version == b.min_version
           && a.max_version == b.max_version;
}

std::vector<api_versions_response_key> get_supported_apis();

} // namespace kafka
