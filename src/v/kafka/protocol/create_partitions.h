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

#include "kafka/protocol/schemata/create_partitions_request.h"
#include "kafka/protocol/schemata/create_partitions_response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct create_partitions_request final {
    using api_type = create_partitions_api;

    create_partitions_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const create_partitions_request& r) {
        return os << r.data;
    }
};

struct create_partitions_response final {
    using api_type = create_partitions_api;

    create_partitions_response_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const create_partitions_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
