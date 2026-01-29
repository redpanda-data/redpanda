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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "base/seastarx.h"
#include "container/chunked_vector.h"
#include "kafka/client/broker.h"
#include "kafka/client/configuration.h"
#include "kafka/client/types.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace kafka::client {

class brokers {
    using brokers_t = absl::flat_hash_map<model::node_id, shared_broker_t>;

public:
    explicit brokers(
      const connection_configuration& config, prefix_logger& logger)
      : brokers(
          logger, std::make_unique<remote_broker_factory>(config, logger)) {};

    brokers(prefix_logger& logger, std::unique_ptr<broker_factory> factory)
      : _logger(&logger)
      , _factory(std::move(factory)) {};

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
    shared_broker_t any();

    /// \brief Retrieve the broker for the given node_id.
    shared_broker_t find(model::node_id id);

    /// \brief Remove a broker.
    ss::future<> erase(model::node_id id);

    /// \brief Remove all brokers.
    ss::future<> clear();

    /**
     * Applies the metadata response to the brokers. This method will throw if
     * any of the brokers can not be connected to.
     */
    ss::future<> apply(const chunked_vector<metadata_update::broker>& brokers);

    /// \brief Returns true if there are no connected brokers
    bool empty() const;

    ss::future<shared_broker_t>
    create_broker(model::node_id node_id, net::unresolved_address addr);

    size_t size() const { return _brokers.size(); }
    /**
     * Returns the range of versions that is supported by all the brokers in the
     * cluster. It connects to the brokers if necessary.
     */
    ss::future<std::optional<api_version_range>> supported_api_versions(
      api_key key, std::optional<std::reference_wrapper<ss::abort_source>>);

    /**
     * Returns the range of versions that is supported the requested broker.
     * Connection to the broker is established if necessary.
     */
    ss::future<std::optional<api_version_range>> supported_api_versions(
      model::node_id id,
      api_key key,
      std::optional<std::reference_wrapper<ss::abort_source>>);

    // Returns a view of a all broker ids
    auto get_broker_ids() const { return std::views::keys(_brokers); }

private:
    ss::future<> do_erase(model::node_id id);
    ss::future<> do_clear();
    /// \brief Brokers map a model::node_id to a client.
    brokers_t _brokers;
    /// \brief Next broker to select with round-robin
    size_t _next_broker{0};
    prefix_logger* _logger;
    std::unique_ptr<broker_factory> _factory;
    ssx::mutex _state_mutex{"brokers::mutex"};
};

} // namespace kafka::client
