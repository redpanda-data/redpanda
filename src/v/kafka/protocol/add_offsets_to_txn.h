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
#include "bytes/iobuf.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/add_offsets_to_txn_request.h"
#include "kafka/protocol/schemata/add_offsets_to_txn_response.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace kafka {

struct add_offsets_to_txn_request final {
    using api_type = add_offsets_to_txn_api;

    add_offsets_to_txn_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const add_offsets_to_txn_request& r) {
    return os << r.data;
}

struct add_offsets_to_txn_response final {
    using api_type = add_offsets_to_txn_api;

    add_offsets_to_txn_response_data data;
    add_offsets_to_txn_response() = default;
    explicit add_offsets_to_txn_response(error_code error) {
        data.error_code = error;
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const add_offsets_to_txn_response& r) {
    return os << r.data;
}

} // namespace kafka
