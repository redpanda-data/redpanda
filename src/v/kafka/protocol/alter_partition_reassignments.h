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

#include "kafka/protocol/schemata/alter_partition_reassignments_request.h"
#include "kafka/protocol/schemata/alter_partition_reassignments_response.h"

namespace kafka {

struct alter_partition_reassignments_request final {
    using api_type = alter_partition_reassignments_api;

    alter_partition_reassignments_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream& operator<<(
      std::ostream& os, const alter_partition_reassignments_request& r) {
        return os << r.data;
    }
};

struct alter_partition_reassignments_response final {
    using api_type = alter_partition_reassignments_api;

    alter_partition_reassignments_response_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream& operator<<(
      std::ostream& os, const alter_partition_reassignments_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
