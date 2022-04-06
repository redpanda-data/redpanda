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

#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/protocol/schemata/create_topics_response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#include <unordered_map>

namespace kafka {

struct create_topics_response;

class create_topics_api final {
public:
    using response_type = create_topics_response;

    static constexpr const char* name = "create topics";
    static constexpr api_key key = api_key(19);
};

struct create_topics_request final {
    using api_type = create_topics_api;

    create_topics_request_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const create_topics_request& r) {
    return os << r.data;
}

struct create_topics_response final {
    using api_type = create_topics_api;

    create_topics_response_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const create_topics_response& r) {
    return os << r.data;
}

} // namespace kafka
