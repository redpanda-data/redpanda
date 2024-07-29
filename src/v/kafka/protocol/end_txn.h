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
#include "kafka/protocol/schemata/end_txn_request.h"
#include "kafka/protocol/schemata/end_txn_response.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace kafka {

struct end_txn_request final {
    using api_type = end_txn_api;

    end_txn_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const end_txn_request& r) {
        return os << r.data;
    }
};

struct end_txn_response final {
    using api_type = end_txn_api;

    end_txn_response_data data;

    end_txn_response() = default;
    explicit end_txn_response(error_code error) { data.error_code = error; }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const end_txn_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
