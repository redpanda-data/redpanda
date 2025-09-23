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

#include "base/format_to.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/metadata.h"
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

/// \brief Structure used to report metadata updates
/// Both describe cluster and metadata use very similar fields but are typed
/// differently
struct metadata_update {
    struct broker {
        model::node_id node_id{};
        ss::sstring host{};
        int32_t port{};
        std::optional<ss::sstring> rack{};

        fmt::iterator format_to(fmt::iterator it) const;
    };

    chunked_vector<broker> brokers;
    std::optional<ss::sstring> cluster_id;
    model::node_id controller_id{-1};
    std::optional<chunked_vector<metadata_response_topic>> topics;
    kafka::cluster_authorized_operations cluster_authorized_operations{
      kafka::cluster_authorized_operations_not_set};

    fmt::iterator format_to(fmt::iterator it) const;
};
} // namespace kafka::client
