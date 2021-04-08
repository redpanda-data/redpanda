// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/topic_cache.h"

#include "kafka/client/exceptions.h"
#include "kafka/protocol/metadata.h"

#include <seastar/core/future.hh>

namespace kafka::client {

ss::future<>
topic_cache::apply(std::vector<metadata_response::topic>&& topics) {
    leaders_t leaders;
    for (const auto& t : topics) {
        for (auto const& p : t.partitions) {
            leaders.emplace(model::topic_partition(t.name, p.index), p.leader);
        }
    }
    std::swap(leaders, _leaders);
    return ss::now();
}

ss::future<model::node_id>
topic_cache::leader(model::topic_partition tp) const {
    if (auto topic_it = _leaders.find(tp); topic_it != _leaders.end()) {
        return ss::make_ready_future<model::node_id>(topic_it->second);
    }
    return ss::make_exception_future<model::node_id>(
      partition_error(std::move(tp), error_code::unknown_topic_or_partition));
}

} // namespace kafka::client
