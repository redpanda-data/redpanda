#pragma once

#include "model/fundamental.h"
#include "pandaproxy/client/broker.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace pandaproxy::client {

/// \brief during connection, the node_id isn't known.
const model::node_id unknown_node_id{-1};

class brokers {
    using brokers_t
      = absl::flat_hash_set<shared_broker_t, broker_hash, broker_eq>;
    using leaders_t
      = absl::flat_hash_map<model::topic_partition, model::node_id>;

public:
    /// \brief stop and wait for all outstanding activity to finish.
    ss::future<> stop();

    /// \brief Retrieve any broker.
    ///
    /// The broker returned is fetched using a round-robin strategy.
    ss::future<shared_broker_t> any();

    /// \brief Retrieve the broker for the given topic_partition.
    ss::future<shared_broker_t> find(model::topic_partition tp);

    /// \brief Remove a broker.
    ss::future<> erase(model::node_id id);

    /// \brief Apply the given metadata response.
    ss::future<> apply(kafka::metadata_response&& res);

private:
    /// \brief Brokers map a model::node_id to a kafka::client.
    brokers_t _brokers;
    /// \brief Next broker to select with round-robin
    size_t _next_broker;
    /// \brief Leaders map a partition to a model::node_id.
    leaders_t _leaders;
};

} // namespace pandaproxy::client
