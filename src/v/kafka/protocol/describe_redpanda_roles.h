/*
 * Copyright 2026 Redpanda Data, Inc.
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
#include "kafka/protocol/schemata/describe_redpanda_roles_request.h"
#include "kafka/protocol/schemata/describe_redpanda_roles_response.h"

#include <seastar/core/future.hh>

namespace kafka {

struct describe_redpanda_roles_request final {
    using api_type = describe_redpanda_roles_api;

    describe_redpanda_roles_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", data);
    }
};

struct describe_redpanda_roles_response final {
    using api_type = describe_redpanda_roles_api;

    describe_redpanda_roles_response_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", data);
    }
};

} // namespace kafka
