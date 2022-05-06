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
#include "kafka/protocol/schemata/delete_topics_request.h"
#include "kafka/protocol/schemata/delete_topics_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct delete_topics_response;

struct delete_topics_api final {
    using response_type = delete_topics_response;

    static constexpr const char* name = "delete topics";
    static constexpr api_key key = api_key(20);
};

struct delete_topics_request final {
    using api_type = delete_topics_api;

    delete_topics_request_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const delete_topics_request& r) {
        return os << r.data;
    }
};

struct delete_topics_response final {
    using api_type = delete_topics_api;

    delete_topics_response_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const delete_topics_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
