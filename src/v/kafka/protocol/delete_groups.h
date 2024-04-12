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
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/delete_groups_request.h"
#include "kafka/protocol/schemata/delete_groups_response.h"
#include "kafka/types.h"

namespace kafka {

struct delete_groups_request final {
    using api_type = delete_groups_api;

    delete_groups_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const delete_groups_request& r) {
        return os << r.data;
    }
};

struct delete_groups_response final {
    using api_type = delete_groups_api;

    delete_groups_response_data data;

    explicit delete_groups_response(
      std::vector<deletable_group_result> results) {
        data.results = std::move(results);
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const delete_groups_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
