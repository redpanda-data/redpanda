/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "kafka/protocol/schemata/write_txn_markers_request.h"
#include "kafka/protocol/schemata/write_txn_markers_response.h"

namespace kafka {

struct write_txn_markers_request final {
    using api_type = write_txn_markers_api;

    write_txn_markers_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const write_txn_markers_request& r) {
        return os << r.data;
    }
};

struct write_txn_markers_response final {
    using api_type = write_txn_markers_api;

    write_txn_markers_response_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const write_txn_markers_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
