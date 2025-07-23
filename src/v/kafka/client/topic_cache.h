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
#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace kafka::client {

class topic_cache {
    struct partition_data {
        model::node_id leader;
    };

    struct topic_data {
        chunked_hash_map<model::partition_id, partition_data> partitions;
    };

    using topics_t = chunked_hash_map<model::topic, topic_data>;

public:
    topic_cache() = default;
    topic_cache(const topic_cache&) = delete;
    topic_cache(topic_cache&&) = default;
    topic_cache& operator=(const topic_cache&) = delete;
    topic_cache& operator=(topic_cache&&) = delete;
    ~topic_cache() noexcept = default;

    /// \brief Apply the given metadata response.
    void apply(const small_fragment_vector<metadata_response::topic>& topics);

    /// \brief Obtain the leader for the given topic-partition
    std::optional<model::node_id> leader(const model::topic_partition&) const;

private:
    /// \brief Cache of topic information.
    topics_t _topics;
};

} // namespace kafka::client
