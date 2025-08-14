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
#include "container/chunked_vector.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace kafka::client {

class topic_cache {
    struct partition_data {
        model::node_id leader;
        kafka::leader_epoch leader_epoch{invalid_leader_epoch};
    };

    struct topic_data {
        chunked_hash_map<model::partition_id, partition_data> partitions;
        kafka::topic_authorized_operations authorized_operations
          = kafka::topic_authorized_operations_not_set;
        int16_t replication_factor;
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
    void apply(const chunked_vector<metadata_response::topic>& topics);

    /// \brief Obtain the leader for the given topic-partition
    std::optional<model::node_id> leader(model::topic_partition_view) const;
    std::optional<kafka::leader_epoch>
      leader_epoch(model::topic_partition_view) const;

    auto topics() const { return std::views::keys(_topics); }

    std::optional<kafka::topic_authorized_operations>
    authorized_operations_for_topic(model::topic_view tp) const;

    std::optional<int32_t> partition_count(model::topic_view tp) const;

    std::optional<int16_t> replication_factor(model::topic_view tp) const;

    const topics_t& cache() const noexcept;

private:
    /// \brief Cache of topic information.
    topics_t _topics;
};

} // namespace kafka::client
