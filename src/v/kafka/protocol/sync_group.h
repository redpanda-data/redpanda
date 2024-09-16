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
#include "base/seastarx.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/sync_group_request.h"
#include "kafka/protocol/schemata/sync_group_response.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

#include <utility>

namespace kafka {

struct sync_group_request final {
    using api_type = sync_group_api;

    sync_group_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const sync_group_request& r) {
        return os << r.data;
    }
};

struct sync_group_response final {
    using api_type = sync_group_api;

    sync_group_response_data data;

    sync_group_response() = default;

    sync_group_response(error_code error, bytes assignment)
      : data({
          .error_code = error,
          .assignment = std::move(assignment),
        }) {}

    explicit sync_group_response(error_code error)
      : sync_group_response(error, bytes()) {}

    sync_group_response(const sync_group_request&, error_code error)
      : sync_group_response(error) {}

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const sync_group_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
