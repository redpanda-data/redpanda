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
#include "container/fragmented_vector.h"
#include "kafka/client/broker.h"
#include "kafka/client/configuration.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace kafka::client {

/// \brief during connection, the node_id isn't known.
inline const model::node_id unknown_node_id{-1};

class brokers {
    using brokers_t
      = absl::flat_hash_set<shared_broker_t, broker_hash, broker_eq>;

public:
    explicit brokers(const configuration& config)
      : _config(config) {};
    brokers(const brokers&) = delete;
    brokers(brokers&&) = default;
    brokers& operator=(const brokers&) = delete;
    brokers& operator=(brokers&&) = delete;
    ~brokers() noexcept = default;

    /// \brief stop and wait for all outstanding activity to finish.
    ss::future<> stop();

    /// \brief Retrieve any broker.
    ///
    /// The broker returned is fetched using a round-robin strategy.
    ss::future<shared_broker_t> any();

    /// \brief Retrieve the broker for the given node_id.
    ss::future<shared_broker_t> find(model::node_id id);

    /// \brief Remove a broker.
    ss::future<> erase(model::node_id id);

    /// \brief Apply the given metadata response.
    ss::future<> apply(chunked_vector<metadata_response::broker>&& brokers);

    /// \brief Returns true if there are no connected brokers
    ss::future<bool> empty() const;

private:
    const configuration& _config;
    /// \brief Brokers map a model::node_id to a client.
    brokers_t _brokers;
    /// \brief Next broker to select with round-robin
    size_t _next_broker{0};
};

} // namespace kafka::client
