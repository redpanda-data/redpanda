/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "kafka/protocol/types.h"
#include "model/record.h"

#include <optional>

namespace kafka::client {

struct record_essence {
    std::optional<model::partition_id> partition_id;
    std::optional<iobuf> key;
    std::optional<iobuf> value;
    std::vector<model::record_header> headers;
};

inline constexpr model::node_id consumer_replica_id{-1};

/// \brief during connection, the node_id isn't known.
inline constexpr model::node_id unknown_node_id{-1};

/**
 * Represents a range of supported API versions for a given API key.
 */
struct supported_api_versions {
    api_version min;
    api_version max;

    bool is_supported(api_version v) const { return v >= min && v <= max; }
};
} // namespace kafka::client
